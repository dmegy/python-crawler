import sqlite3
import requests
import time
from datetime import datetime, timezone

DB_PATH = "state/found_documents.db"
USER_AGENT = "Mozilla/5.0 (compatible; MyCrawler/1.0)"


def fetch_head_and_initial_bytes(url):
    headers = {
        "User-Agent": USER_AGENT
    }

    try:
        res = requests.get(url, stream=True, headers=headers, timeout=10)
        initial_bytes = res.raw.read(32)
        if res.status_code in (200, 206):  # 206 = partial content
            headers = res.headers
            return {
                "status_code": res.status_code,
                "content_type": headers.get("Content-Type"),
                "content_length": int(headers.get("Content-Length")) if headers.get("Content-Length", "").isdigit() else None,
                "last_modified": headers.get("Last-Modified"),
                "initial_bytes": initial_bytes
            }
        else:
            return {
                "status_code": res.status_code,
                "content_type": res.headers.get("Content-Type"),
                "content_length": int(res.headers.get("Content-Length")) if res.headers.get("Content-Length", "").isdigit() else None,
                "last_modified": res.headers.get("Last-Modified"),
                "initial_bytes": b''
            }
    except requests.RequestException as e:
        return {
            "status_code": None,
            "error": str(e),
            "initial_bytes": b''
        }

def verify_links():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute("""
        SELECT id, url FROM found_documents
        WHERE link_http_code IS NULL
    """)
    rows = cur.fetchall()

    print(f"Found {len(rows)} unverified links.")

    for row in rows:
        doc_id = row["id"]
        url = row["url"]
        print(f"Verifying: {url}")

        result = fetch_head_and_initial_bytes(url)
        if not result.get("status_code"):
            continue

        now = datetime.now(timezone.utc).isoformat()
        cur.execute("""
            UPDATE found_documents
            SET
                link_date_accessed = ?,
                link_http_code = ?,
                link_content_type = ?,
                link_content_length = ?,
                link_last_modified = ?,
                doc_initial_bytes = ?
            WHERE id = ?
        """, (
            now,
            result.get("status_code"),
            result.get("content_type"),
            result.get("content_length"),
            result.get("last_modified"),
            result.get("initial_bytes"),
            doc_id
        ))
        conn.commit()
        time.sleep(1)  # polite delay

    print("Verification complete.")
    conn.close()

if __name__ == "__main__":
    verify_links()
