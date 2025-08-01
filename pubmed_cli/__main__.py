# pubmed_cli/__main__.py

import argparse
import requests
import xml.etree.ElementTree as ET
import csv
import re
import sys
from typing import List, Dict, Optional, Tuple

# Heuristic keyword sets for affiliation classification
ACADEMIC_KEYWORDS = [
    "university", "college", "institute", "school of", "department of", "faculty of",
    "research center", "centre for", "hospital", "medical center", "clinic"
]
COMPANY_KEYWORDS = [
    "inc", "ltd", "llc", "gmbh", "corporation", "corp", "pharma", "biotech",
    "biotechnology", "technologies", "systems", "bio", "laboratories", "laboratory",
    "sas", "sa", "nv", "ab", "co.", "ag"
]
EMAIL_REGEX = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")

def debug_print(message: str, enabled: bool) -> None:
    if enabled:
        print(f"[DEBUG] {message}", file=sys.stderr)

def esearch(term: str, retmax: int, api_key: Optional[str], debug: bool) -> Tuple[List[str], int]:
    params = {
        "db": "pubmed",
        "term": term,
        "retmode": "json",
        "retmax": retmax,
    }
    if api_key:
        params["api_key"] = api_key
    debug_print(f"Calling esearch with params: {params}", debug)
    resp = requests.get("https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi", params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json().get("esearchresult", {})
    idlist = data.get("idlist", [])
    count = int(data.get("count", "0"))
    debug_print(f"esearch result count={count}, ids={idlist}", debug)
    return idlist, count

def efetch(pmids: List[str], api_key: Optional[str], debug: bool) -> str:
    if not pmids:
        return ""
    params = {
        "db": "pubmed",
        "id": ",".join(pmids),
        "retmode": "xml",
    }
    if api_key:
        params["api_key"] = api_key
    debug_print(f"Calling efetch with params: {params}", debug)
    resp = requests.get("https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi", params=params, timeout=60)
    resp.raise_for_status()
    return resp.text

def contains_keyword(text: str, keywords: List[str]) -> bool:
    lower = text.lower()
    return any(k.lower() in lower for k in keywords)

def extract_papers(xml_text: str, debug: bool) -> List[Dict[str, str]]:
    root = ET.fromstring(xml_text)
    results: List[Dict[str, str]] = []

    for article in root.findall(".//PubmedArticle"):
        pmid = article.findtext(".//PMID") or ""
        title = article.findtext(".//ArticleTitle") or ""

        # Publication date heuristics
        pub_date = ""
        pubdate_node = article.find(".//PubDate")
        if pubdate_node is not None:
            year = pubdate_node.findtext("Year")
            medline = pubdate_node.findtext("MedlineDate")
            month = pubdate_node.findtext("Month") or "01"
            day = pubdate_node.findtext("Day") or "01"
            if year:
                pub_date = f"{year}-{month}-{day}"
            elif medline:
                pub_date = medline

        # Authors and affiliations
        authors_info: List[Dict[str, List[str]]] = []
        for author in article.findall(".//Author"):
            lastname = author.findtext("LastName") or ""
            forename = author.findtext("ForeName") or ""
            name = f"{forename} {lastname}".strip()
            affs = []
            for aff in author.findall(".//AffiliationInfo/Affiliation"):
                if aff is not None and aff.text:
                    affs.append(aff.text.strip())
            authors_info.append({"name": name, "affiliations": affs})

        # Non-academic authors: those with no academic keyword in any affiliation
        non_academic_authors: List[str] = []
        company_affiliations: List[str] = []
        for a in authors_info:
            is_academic = any(contains_keyword(aff, ACADEMIC_KEYWORDS) for aff in a["affiliations"])
            if not is_academic and a["name"]:
                non_academic_authors.append(a["name"])
            for aff in a["affiliations"]:
                if contains_keyword(aff, COMPANY_KEYWORDS):
                    # extract relevant fragment
                    company_affiliations.append(aff)

        # Deduplicate company names heuristically (simple)
        company_affiliations_unique = sorted(set(company_affiliations))

        # Corresponding email: first found in affiliations
        email = ""
        for a in authors_info:
            for aff in a["affiliations"]:
                match = EMAIL_REGEX.search(aff)
                if match:
                    email = match.group(0)
                    break
            if email:
                break

        record = {
            "PubmedID": pmid,
            "Title": title,
            "Publication Date": pub_date,
            "Non-academicAuthor(s)": "; ".join(sorted(set(non_academic_authors))) if non_academic_authors else "",
            "CompanyAffiliation(s)": "; ".join(company_affiliations_unique),
            "Corresponding Author Email": email
        }
        debug_print(f"Extracted record for PMID {pmid}: {record}", debug)
        results.append(record)

    return results

def output_csv(papers: List[Dict[str, str]], filename: Optional[str]) -> None:
    fieldnames = [
        "PubmedID",
        "Title",
        "Publication Date",
        "Non-academicAuthor(s)",
        "CompanyAffiliation(s)",
        "Corresponding Author Email"
    ]
    if filename:
        with open(filename, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for p in papers:
                writer.writerow(p)
        print(f"Saved {len(papers)} records to {filename}")
    else:
        writer = csv.DictWriter(sys.stdout, fieldnames=fieldnames)
        writer.writeheader()
        for p in papers:
            writer.writerow(p)

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch PubMed papers for a query and output a CSV with enriched metadata."
    )
    parser.add_argument("query", help="PubMed search query (supports full syntax).")
    parser.add_argument("-d", "--debug", action="store_true", help="Print debug information.")
    parser.add_argument("-f", "--file", help="Output CSV file path. If omitted, prints to stdout.")
    parser.add_argument("--retmax", type=int, default=10, help="Number of papers to fetch (default 10).")
    parser.add_argument("--api-key", help="NCBI API key to increase rate limits (optional).")

    args = parser.parse_args()

    try:
        pmids, total = esearch(args.query, retmax=args.retmax, api_key=args.api_key, debug=args.debug)
        if not pmids:
            print("No papers found for the query.", file=sys.stderr)
            return
        if args.debug:
            print(f"Total hits reported: {total}. Fetching PMIDs: {pmids}", file=sys.stderr)
        xml_data = efetch(pmids, api_key=args.api_key, debug=args.debug)
        papers = extract_papers(xml_data, debug=args.debug)
        output_csv(papers, filename=args.file)
    except requests.HTTPError as e:
        print(f"HTTP error during PubMed access: {e}", file=sys.stderr)
    except Exception as e:
        print(f"Unexpected error: {e}", file=sys.stderr)
