import os
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse, parse_qs
import csv
import time
from collections import defaultdict
from datetime import datetime
import sqlite3


# ---- File Paths ----
ALLOWED_CRAWL_PATTERNS_FILE = "allowed_crawl_patterns.txt"
BLOCKED_CRAWL_PATTERNS_FILE = "blocked_crawl_patterns.txt"

STATE_DIR = "state"
DB_PATH = os.path.join(STATE_DIR, "found_documents.db")
BEING_VISITED_FILE = os.path.join(STATE_DIR, "urls_being_visited.txt")
VISITED_FILE = os.path.join(STATE_DIR, "urls_visited.txt")
TO_VISIT_FILE = os.path.join(STATE_DIR, "urls_to_visit.txt")
ERROR_LOG_FILE = os.path.join(STATE_DIR, "errors.log")
UNREACHABLE_DOMAINS_FILE = os.path.join(STATE_DIR, "unreachable_domains.txt")

REQUEST_DELAY = 2  # secondes
MAX_DEPTH = 3
PDF_BATCH_SIZE = 20

last_request_time = defaultdict(float)

# ---- Utilities ----



def ensure_file(path):
    if not os.path.isfile(path):
        print(f"[INFO] Creating missing file: {path}")
        open(path, 'w').close()

def ensure_state_environment():
    os.makedirs(STATE_DIR, exist_ok=True)
    for path in [
        TO_VISIT_FILE,
        VISITED_FILE,
        BEING_VISITED_FILE,
        ERROR_LOG_FILE,
    ]:
        ensure_file(path)

def init_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
      CREATE TABLE IF NOT EXISTS found_documents (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        url TEXT  NOT NULL,
        source_url TEXT NOT NULL,
        source_title TEXT,
        link_extension TEXT,
        link_text TEXT,
        link_title TEXT,
        link_date_added TEXT DEFAULT (datetime('now')),
        link_date_accessed TEXT,
        link_http_code INTEGER,
        link_content_type TEXT,
        link_content_length INTEGER,
        link_last_modified TEXT,
        doc_initial_bytes BLOB,
        doc_date_downloaded TEXT,
        doc_local_path TEXT,
        doc_file_size INTEGER,
        doc_file_name TEXT,
        doc_checksum TEXT,
        doc_date_created TEXT,
        doc_author TEXT,
        doc_title TEXT,
        doc_producer TEXT,
        doc_page_count INTEGER
      )
    """)
    conn.commit()
    return conn

def extract_meta_author(soup):
    author_tag = soup.find("meta", attrs={"name": "author"})
    if author_tag and author_tag.get("content"):
        return author_tag["content"].strip()
    return None

def get_meta_refresh_redirect_url(soup, current_url):
    if soup is None:
        return None

    meta = soup.find("meta", attrs={"http-equiv": lambda x: x and x.lower() == "refresh"})
    if not meta:
        return None

    content = meta.get("content", "")
    if not isinstance(content, str):
        return None

    parts = content.lower().split(";")
    for part in parts:
        part = part.strip()
        if part.startswith("url="):
            raw_url = part[4:].strip().strip('\'"')
            if raw_url:
                return urljoin(current_url, raw_url)

    return None



def normalize_url(url):
    parsed = urlparse(url)
    parsed = parsed._replace(fragment="")  # Remove #section
    return parsed.geturl()

def is_probable_pdf(url):
    #rajouter tests sur le 'text' : contient la chaîne "TD" ou "pdf", ou "download" ou "télécharge" ou ".pdf" ou autre ?
    if not isinstance(url, str):
        return False

    parsed = urlparse(url)
    path = parsed.path.lower()
    query = parse_qs(parsed.query)
    netloc = parsed.netloc.lower()

    if path.endswith(".pdf"):
        return True

    if "download" in path and "id" in query:
        return True
    if "plmbox.math.cnrs.fr/f/" in url and "dl" in query:
        return True
    if "plmbox.math.cnrs.fr/seafhttp/f/" in url:
        return True

    if netloc == "drive.google.com":
        parts = path.split('/')
        if len(parts) >= 5 and parts[1] == 'file' and parts[2] == 'd':
            # plus de validation sur {id} si besoin
            return True

    return False



def get_file_extension(url):
    parsed_url = urlparse(url)
    path = parsed_url.path
    _, ext = os.path.splitext(path)
    return ext[1:].lower() if ext else ""

def is_probable_html(url):
    html_like_extensions = {
        'html', 'htm', 'php', 'asp', 'aspx', 'jsp', 'jspx',
        'cgi', 'pl', 'xhtml', 'shtml', 'cfm', 'rhtml', 'erb',
        'do', 'action', 'axd'
    }
    ext = get_file_extension(url)
    if ext in html_like_extensions:
        return True
    if not get_file_extension(url):
        return True

    return False



def fetch_with_throttle(url):
    domain = get_domain(url)
    elapsed = time.time() - last_request_time[domain]
    if elapsed < REQUEST_DELAY:
        sleep_time = REQUEST_DELAY - elapsed
        print(f"Throttling: waiting {sleep_time:.2f}s before accessing {domain}")
        time.sleep(sleep_time)

    try:
        res = requests.get(
            url,
            timeout=10,
            headers={"User-Agent": "Mozilla/5.0 (compatible; MyCrawler/1.0)"}
        )
        last_request_time[domain] = time.time()
        return res
    except requests.RequestException as e:
        log_error(f"Request failed for {url}: {e}")
        last_request_time[domain] = time.time()
        unreachable_domains.add(domain)
        return None



def log_error(message):
    timestamp = time.strftime("[%Y-%m-%d %H:%M:%S]")
    with open(ERROR_LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"{timestamp} {message}\n")


def add_to_file(filename, url):
    existing = load_set(filename)
    if url not in existing:
        with open(filename, "a") as f:
            f.write(f"{url}\n")

def remove_from_file(filename, url):
    if not os.path.exists(filename):
        return
    with open(filename, "r") as f:
        lines = f.readlines()
    with open(filename, "w") as f:
        for line in lines:
            if line.strip() != url:
                f.write(line)

def load_set(filename):
    if not os.path.exists(filename):
        return set()
    with open(filename, "r") as f:
        return set(line.strip() for line in f if line.strip())

def save_set(s, filename):
    with open(filename, "w") as f:
        for item in sorted(s):
            f.write(f"{item}\n")

def load_to_visit(filename):
    if not os.path.exists(filename):
        return []
    with open(filename, "r") as f:
        return [
            (url.strip(), int(depth.strip()))
            for line in f if line.strip()
            for url, depth in [line.strip().split('|', 1)]
        ]


def save_to_visit(lst, filename):
    tmp_filename = filename + ".tmp"
    with open(tmp_filename, "w", encoding="utf-8") as f:
        for url, depth in lst:
            f.write(f"{url}|{depth}\n")
    os.replace(tmp_filename, filename)

def save_state_to_files():
    save_to_visit(urls_to_visit, TO_VISIT_FILE)
    save_set(urls_already_visited, VISITED_FILE)
    save_set(urls_being_visited, BEING_VISITED_FILE)
    save_set(unreachable_domains,UNREACHABLE_DOMAINS_FILE)

def append_pdf_info_batch(batch, pdf_url, extension, anchor_text, anchor_title, source_url, source_title):
    batch.append((
        pdf_url,
        extension,
        anchor_text,
        anchor_title,
        source_url,
        source_title
    ))

def flush_pdf_info_batch(db_conn, batch):
    if not batch:
        return

    sql = """
      INSERT INTO found_documents (
        url,
        link_extension,
        link_text,
        link_title,
        source_url,
        source_title
      ) VALUES (?, ?, ?, ?, ?, ?)
    """

    cur = db_conn.cursor()
    cur.executemany(sql, batch)
    db_conn.commit()
    print(f"Committed {len(batch)} new entries.")
    batch.clear()



def get_domain(url):
    try:
        return urlparse(url).netloc
    except Exception:
        return ""


def is_url_allowed(url):
    # attention utilise les variables globales.
    # si la liste allowed domaines est vide, on autorise
    # pas ultra sécurisé
    if len(allowed_crawl_patterns) == 0:
        return True
    return any(pattern.strip().lower() in url for pattern in allowed_crawl_patterns)

def is_url_blocked(url):
    # attention utilise les variables globales.
    for pattern in blocked_crawl_patterns:
        pattern = pattern.strip().lower()
        if pattern and pattern in url:
            return True
    return False

def is_eligible_for_crawl(url):
    # attention utilise les variables globales.
    domain = get_domain(url)
    if domain in unreachable_domains:
        print(f"[SKIP] Domain marked as unreachable: {url}")
        return False
    if is_url_blocked(url):
        print(f"[BLOCKED] {url}")
        return False
    if not is_url_allowed(url):
        print(f"[NOT ALLOWED] {url}")
        return False
    if url in urls_already_visited:
        print(f"[SKIP] Already visited: {url}")
        return False
    if url in urls_being_visited:
        print(f"[SKIP] Already being visited: {url}")
        return False
    if url in urls_to_visit_set:
        print(f"[SKIP] Already scheduled : {url}")
        return False

    return True

def get_next_url_to_visit():
    now = time.time()
    for i, (candidate_url, candidate_depth) in enumerate(urls_to_visit):
        sanitized_url = normalize_url(candidate_url)
        domain = get_domain(sanitized_url)
        elapsed = now - last_request_time[domain]

        if elapsed >= REQUEST_DELAY:
            urls_to_visit.pop(i)
            urls_to_visit_set.discard(candidate_url)
            return sanitized_url, int(candidate_depth)

    return None, None




def crawl():

    db_conn = init_db()

    try:
        while urls_to_visit:
            now = time.time()

            current_url, current_depth = get_next_url_to_visit()

            if current_url is None:
                print("[WAITING 0.1s]")
                time.sleep(0.1)
                continue

            if current_depth > MAX_DEPTH:
                print("[SKIP] Max depth")
                continue

            if not is_eligible_for_crawl(current_url):
                continue


            urls_being_visited.add(current_url)
            # save state ?
            print(f"Crawling (depth {current_depth}): {current_url}")
            try:
                res = fetch_with_throttle(current_url)
                if res is None:
                    print(f"[UNREACHEABLE] {current_url}")
                    unreachable_domains.add(domain)
                    continue

                current_url = normalize_url(res.url) # éventuel redirect http :
                if not is_eligible_for_crawl(current_url):
                    continue

                soup = BeautifulSoup(res.text, "html.parser")

                redirect_url = get_meta_refresh_redirect_url(soup, current_url)
                if redirect_url:
                    if is_eligible_for_crawl(redirect_url):
                        print(f"[FOLLOW] {redirect_url}")
                        urls_to_visit.append((normalize_url(redirect_url), str(current_depth)))
                        urls_to_visit_set.add(normalize_url(redirect_url))
                        urls_already_visited.add(current_url)
                        urls_being_visited.discard(current_url)
                    continue  # skip 

                
                source_title = soup.title.string.strip() if soup.title and soup.title.string else None

                for link in soup.find_all("a", href=True):
                    href = link["href"].strip()
                    full_url = urljoin(current_url, href)
                    url = normalize_url(full_url)
                    
                    text = link.text.strip() or "[no text]"
                    title = link.get("title", None)

                    if not is_eligible_for_crawl(url):
                        continue

                    if url in added_documents:
                        print(f"url {url} already in added_documents!")
                        continue

                    if is_probable_pdf(url):
                        append_pdf_info_batch(pdf_batch, url, get_file_extension(url), text, title, current_url, source_title)
                        added_documents.add(url)
                        print(f"[ADDED PDF] {url}. Batch length : {len(pdf_batch)}")
                        if len(pdf_batch) >= PDF_BATCH_SIZE:
                            flush_pdf_info_batch(db_conn, pdf_batch)
                    elif is_probable_html(url):
                        print(f"[ADDED PAGE] {url}")
                        urls_to_visit.append((url, str(current_depth + 1)))
                        urls_to_visit_set.add(url)
      



            except Exception as e:
                log_error(f"Error visiting {current_url}: {e}")
            finally:
                flush_pdf_info_batch(db_conn, pdf_batch) #à la fin de chaque page
                urls_already_visited.add(current_url)
                urls_being_visited.discard(current_url)
                save_state_to_files()

    finally:
        # par exemple en cas d'interruption au clavier
        flush_pdf_info_batch(db_conn, pdf_batch)
        db_conn.close()
        save_state_to_files()


if __name__ == "__main__":
    ensure_state_environment()
    unreachable_domains = set()
    added_documents = set()
    pdf_batch = []
    
    unreachable_domains = load_set(UNREACHABLE_DOMAINS_FILE)
    urls_being_visited = load_set(BEING_VISITED_FILE)
    urls_already_visited = load_set(VISITED_FILE)
    urls_to_visit = load_to_visit(TO_VISIT_FILE)
    urls_to_visit_set = set(url for url, _ in urls_to_visit)

    allowed_crawl_patterns = load_set(ALLOWED_CRAWL_PATTERNS_FILE)
    blocked_crawl_patterns = load_set(BLOCKED_CRAWL_PATTERNS_FILE)

    if not urls_to_visit:
        seed = input("Enter seed URL to start crawling: ").strip()
        urls_to_visit = [(seed, 0)]

    try:
        crawl()
    except KeyboardInterrupt:
        print("Interrupted by user")