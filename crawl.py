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
ALLOWED_DOMAINS_FILE = "domains_allowed.txt"
BLOCKED_DOMAINS_FILE = "domains_blocked.txt"

STATE_DIR = "state"
DB_PATH = os.path.join(STATE_DIR, "found_documents.db")
BEING_VISITED_FILE = os.path.join(STATE_DIR, "urls_being_visited.txt")
VISITED_FILE = os.path.join(STATE_DIR, "urls_visited.txt")
TO_VISIT_FILE = os.path.join(STATE_DIR, "urls_to_visit.txt")
ERROR_LOG_FILE = os.path.join(STATE_DIR, "errors.log")


REQUEST_DELAY = 2  
MAX_DEPTH = 3

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
        return [line.strip().split('|') for line in f if line.strip()]


def save_to_visit(lst, filename):
    tmp_filename = filename + ".tmp"
    with open(tmp_filename, "w", encoding="utf-8") as f:
        for url, depth in lst:
            f.write(f"{url}|{depth}\n")
    os.replace(tmp_filename, filename)

def append_pdf_info_batch(batch, pdf_url, extension, anchor_text, anchor_title, source_url, source_title, source_author):
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


def is_allowed_domain(url, allowed_domains):
    # si la liste allowed domaines est vide, on autorise
    if len(allowed_domains) == 0:
        return True
    domain = get_domain(url)
    return any(
        domain == allowed or domain.endswith('.' + allowed) or domain.endswith(allowed)
        for allowed in allowed_domains
    )

def is_blocked_domain(url, blocked_domains):
    domain = get_domain(url)
    return any(
        domain == blocked or domain.endswith('.' + blocked) or domain.endswith(blocked)
        for blocked in blocked_domains
    )


def crawl():
    added_documents = set()
    pdf_batch = []
    visited = load_set(VISITED_FILE)
    to_visit = load_to_visit(TO_VISIT_FILE)
    allowed_domains = load_set(ALLOWED_DOMAINS_FILE)
    blocked_domains = load_set(BLOCKED_DOMAINS_FILE)

    if not to_visit:
        seed = input("Enter seed URL to start crawling: ").strip()
        to_visit = [(seed, "0")]

    db_conn = init_db()

    try:
        while to_visit:
            now = time.time()
            i = 0
            while i < len(to_visit):
                candidate_url, candidate_depth_str = to_visit[i]
                domain = get_domain(candidate_url)
                elapsed = now - last_request_time[domain]
                if elapsed >= REQUEST_DELAY:
                    break
                i += 1

            if i == len(to_visit):
                time.sleep(0.1)
                continue

            current_url, current_depth_str = to_visit.pop(i)
            current_url = normalize_url(current_url)
            current_depth = int(current_depth_str)

            if current_url in visited or current_depth > MAX_DEPTH:
                save_to_visit(to_visit, TO_VISIT_FILE)
                print("skip: already in visited or max depth")
                continue

            if is_blocked_domain(current_url, blocked_domains):
                print(f"Skipping {current_url} (domain blocked)")
                save_to_visit(to_visit, TO_VISIT_FILE)
                continue

            if not is_allowed_domain(current_url, allowed_domains):
                print(f"Skipping {current_url} (not in allowed domains)")
                save_to_visit(to_visit, TO_VISIT_FILE)
                continue

            add_to_file(BEING_VISITED_FILE, current_url)
            save_to_visit(to_visit, TO_VISIT_FILE)



            print(f"Crawling (depth {current_depth}): {current_url}")
            try:
                res = fetch_with_throttle(current_url)
                if res is None:
                    print(f"Skipping {current_url} due to request failure.")
                    continue

                current_url = normalize_url(res.url)
                if current_url in visited:
                    print(f"url {current_url} already visited")
                    continue

                soup = BeautifulSoup(res.text, "html.parser")

                # Detect meta-refresh redirects
                meta_refresh = soup.find('meta', attrs={'http-equiv': 'refresh'})
                if meta_refresh:
                    content = meta_refresh.get('content', '')
                    if 'url=' in content.lower():
                        redirect_url = content.lower().split('url=')[-1].strip()
                        full_redirect_url = urljoin(current_url, redirect_url)
                        print(f"Meta-refresh redirect found: {full_redirect_url}")
                        to_visit.append((normalize_url(full_redirect_url), str(current_depth)))
                        visited.add(current_url)
                        remove_from_file(BEING_VISITED_FILE, current_url)
                        save_set(visited, VISITED_FILE)
                        save_to_visit(to_visit, TO_VISIT_FILE)
                        continue  # skip further processing of this page

                
                source_title = soup.title.string.strip() if soup.title and soup.title.string else None



                source_author = extract_meta_author(soup)
                for link in soup.find_all("a", href=True):
                    href = link["href"].strip()
                    full_url = urljoin(current_url, href)
                    url = normalize_url(full_url)
                    print(f"New link found : {url}")

                    
                    text = link.text.strip() or "[no text]"
                    title = link.get("title", None)

                    if is_blocked_domain(url, blocked_domains) or not is_allowed_domain(url, allowed_domains):
                        print(f"url {url} has domain blocked or not allowed")
                        continue

                    if  any(existing_url == url for existing_url, _ in to_visit):
                        print(f"url {url} already in 'to_visit'!")
                        continue
                    if url in added_documents:
                        print(f"url {url} already in added_documents!")
                        continue
                    if url in visited:
                        print("page already visited!")
                        continue
                    if url == current_url:
                        print("Page is being visited!")
                        continue



                    if is_probable_pdf(url):
                        append_pdf_info_batch(pdf_batch, url, get_file_extension(url), text, title, current_url, source_title, source_author)
                        added_documents.add(url)
                        print(f"Probable pdf {url} added to batch. Batch length : {len(pdf_batch)}")
                        if len(pdf_batch) >= 20:
                            flush_pdf_info_batch(db_conn, pdf_batch)
                    elif is_probable_html(url):
                        print(f"probable html page: {url}. Appended to 'to_visit'.")
                        to_visit.append((url, str(current_depth + 1)))
      

                flush_pdf_info_batch(db_conn, pdf_batch) #à la fin de chaque page

                visited.add(current_url)
                remove_from_file(BEING_VISITED_FILE, current_url)
                save_set(visited, VISITED_FILE)
                save_to_visit(to_visit, TO_VISIT_FILE)

            except Exception as e:
                log_error(f"Error visiting {current_url}: {e}")
    finally:
        # par exemple en cas d'interruption au clavier
        flush_pdf_info_batch(db_conn, pdf_batch)
        db_conn.close()


if __name__ == "__main__":
    ensure_state_environment()
    try:
        crawl()
    except KeyboardInterrupt:
        print("Interrupted by user")