import os
import sqlite3
import requests
from datetime import datetime
from urllib.parse import urlparse
from hashlib import sha256
from PyPDF2 import PdfReader

# Configuration
DB_PATH = 'state/found_documents.db'
DOWNLOAD_DIR = 'state/downloaded_files/'
CONTENT_TYPE_FILTER = 'application/pdf'
MAX_CONTENT_LENGTH = 500 * 1024  # 500 kB

ALLOWED_DOMAINS = []
BLOCKED_DOMAINS = ["blocked.com"]
ALLOWED_TYPES = ["application/pdf"]
BLOCKED_TYPES = ["text/html"]

conditions = ["doc_date_downloaded IS NULL","link_last_http_code IS NOT NULL"]
params = []

# Allowed content types
if ALLOWED_TYPES:
    placeholders = ','.join(['?'] * len(ALLOWED_TYPES))
    conditions.append(f"LOWER(link_content_type) IN ({placeholders})")
    params.extend([t.lower() for t in ALLOWED_TYPES])

# Blocked content types
if BLOCKED_TYPES:
    for t in BLOCKED_TYPES:
        conditions.append("LOWER(link_content_type) != ?")
        params.append(t.lower())

# Allowed domains
if ALLOWED_DOMAINS:
    domain_conditions = [f"url LIKE ?" for _ in ALLOWED_DOMAINS]
    conditions.append(f"({' OR '.join(domain_conditions)})")
    params.extend([f"%{d}%" for d in ALLOWED_DOMAINS])

# Blocked domains
if BLOCKED_DOMAINS:
    for d in BLOCKED_DOMAINS:
        conditions.append("url NOT LIKE ?")
        params.append(f"%{d}%")


conditions.append("(link_content_length IS NULL OR link_content_length < ?)")
params.append(MAX_CONTENT_LENGTH)








os.makedirs(DOWNLOAD_DIR, exist_ok=True)

def sanitize_filename(url):
    parsed = urlparse(url)
    filename = os.path.basename(parsed.path)
    if not filename:
        filename = "nofilename"
    return filename 

def download_file(url, dest_path):
    try:
        with requests.get(url, stream=True, timeout=20) as r:
            r.raise_for_status()
            with open(dest_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
        return True
    except Exception as e:
        print(f"[ERROR] Failed to download {url}: {e}")
        return False

def extract_pdf_metadata(pdf_path):
    metadata = {
        'encoding': None,
        'date_created': None,
        'author': None,
        'title': None,
        'subject': None,
        'keywords': None,
        'page_count': None,
        'producer': None,
    }

    try:
        reader = PdfReader(pdf_path)
        info = reader.metadata

        # Page count
        try:
            metadata['page_count'] = len(reader.pages)
        except Exception as e:
            print(f"[WARN] Could not determine page count: {e}")

        try:
            if hasattr(reader.stream, 'encoding'):
                metadata['encoding'] = reader.stream.encoding
        except Exception as e:
            print(f"[WARN] Could not extract encoding: {e}")

        for key, pdf_key in {
            'date_created': '/CreationDate',
            'author': '/Author',
            'title': '/Title',
            'subject': '/Subject',
            'keywords': '/Keywords',
            'producer_raw': '/Producer'
        }.items():
            try:
                value = info.get(pdf_key)
                if value:
                    metadata[key.replace('_raw', '')] = str(value).strip()
            except Exception as e:
                print(f"[WARN] Could not extract {key}: {e}")

        producer_raw = metadata.get('producer') or ""
        prod = producer_raw.lower()
        if 'xelatex' in prod:
            metadata['producer'] = 'xelatex'
        elif 'lualatex' in prod:
            metadata['producer'] = 'lualatex'
        elif 'pdftex' in prod:
            metadata['producer'] = 'pdftex'
        elif 'latex' in prod:
            metadata['producer'] = 'Latex'
        elif producer_raw:
            metadata['producer'] = producer_raw  

    except Exception as e:
        print(f"[WARN] Failed to read PDF metadata: {e}")

    return metadata




def main():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    sql = f"""
        SELECT id, url, link_text, link_content_type, link_content_length
        FROM found_documents
        WHERE {' AND '.join(conditions)}
    """
    cursor.execute(sql, params)


    rows = cursor.fetchall()
    print(f"Found {len(rows)} document(s) to download.")

    for row in rows:
        doc_id, url, link_text, content_type, content_length = row
        filename = sanitize_filename(url)
        filename = f"{doc_id}__{link_text}__{filename}"
        local_path = os.path.join(DOWNLOAD_DIR, filename)

        print(f"[INFO] Downloading ID {doc_id}: {url}")
        if download_file(url, local_path):
            file_size = os.path.getsize(local_path)
            checksum = sha256(open(local_path, 'rb').read()).hexdigest()
            now = datetime.utcnow().isoformat()


            metadata = {}
            if content_type == 'application/pdf':
                metadata = extract_pdf_metadata(local_path)

            cursor.execute("""
                UPDATE found_documents
                SET 
                    doc_local_path = ?,
                    doc_file_name = ?,
                    doc_file_size = ?,
                    doc_date_downloaded = ?,
                    doc_checksum = ?,
                    doc_encoding = ?,
                    doc_date_created = ?,
                    doc_author = ?,
                    doc_title = ?,
                    doc_subject = ?,
                    doc_keywords = ?,
                    doc_producer = ?,
                    doc_page_count = ?
                WHERE id = ?
            """, (
                local_path,
                filename,
                file_size,
                now,
                checksum,
                metadata.get('encoding'),
                metadata.get('date_created'),
                metadata.get('author'),
                metadata.get('title'),
                metadata.get('subject'),
                metadata.get('keywords'),
                metadata.get('producer'),
                metadata.get('page_count'),
                doc_id
            ))

            conn.commit()
            print(f"[SUCCESS] Saved to {local_path}")
        else:
            print(f"[SKIPPED] Failed to download ID {doc_id}")

    conn.close()

if __name__ == "__main__":
    main()
