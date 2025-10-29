# services/confluence_service.py

from typing import List, Dict, Any
from urllib.parse import urlparse
import os
import shutil
import requests
import json
import html
import tempfile
import re
from dotenv import load_dotenv
from requests.auth import HTTPBasicAuth
from bs4 import BeautifulSoup

from tools.auth import auth
# Load environment variables
load_dotenv()

# Cache folder path inside services directory
CACHE_DIR = os.path.join(os.path.dirname(__file__), "cache_folder")

# ✅ Only Dify credentials come from .env
DIFY_BASE_URL = os.getenv("DIFY_BASE_URL")
DIFY_API_KEY = os.getenv("DIFY_API_KEY")


class ConfluenceService:
    def __init__(self, base_url: str, email: str, api_token: str):
        # Remove trailing /wiki if present
        self.base_url = base_url.rstrip("/")
        if self.base_url.endswith("/wiki"):
            self.base_url = self.base_url[:-5]

        self.email = email
        self.api_token = api_token
        self.domain = self.extract_domain_name()

        credentials = {
            "url": self.base_url,
            "email": self.email,
            "token": self.api_token
        }
        self.client = auth(credentials)
        

    #########################################################
    # DATASET MANAGEMENT
    #########################################################
    def create_dataset_cache_folder(self, dataset_name: str):
        """Creates/reset local cache folder with trackers for a dataset."""
        folder_path = os.path.join(CACHE_DIR, dataset_name)
        if os.path.exists(folder_path):
            shutil.rmtree(folder_path)  # Reset

        os.makedirs(folder_path, exist_ok=True)
        for tracker in ["page_tracker.txt", "attachment_tracker.txt"]:
            with open(os.path.join(folder_path, tracker), "w", encoding="utf-8") as f:
                f.write("")
        print(f"[CACHE] Created folder {folder_path} with trackers")

    def extract_domain_name(self) -> str:
        netloc = urlparse(self.base_url).netloc
        return netloc.split('.')[0]

    def get_or_create_dataset(self, space_key: str) -> str:
        """Checks if dataset exists in Dify, otherwise creates it."""
        domain = self.extract_domain_name()
        dataset_name = f"{domain}_{space_key}"

        headers = {"Authorization": f"Bearer {DIFY_API_KEY}", "Content-Type": "application/json"}

        resp = requests.get(f"{DIFY_BASE_URL}/datasets?page=1&limit=100", headers=headers, verify=False)
        resp.raise_for_status()
        for ds in resp.json().get("data", []):
            if ds.get("name") == dataset_name:
                return ds.get("id")

        # Create dataset
        payload = {
            "name": dataset_name,
            "description": f"Knowledge base for {domain}/{space_key}",
            "indexing_technique": "high_quality"
        }
        create_resp = requests.post(f"{DIFY_BASE_URL}/datasets", headers=headers, json=payload, verify=False)
        create_resp.raise_for_status()

        self.create_dataset_cache_folder(dataset_name)
        return create_resp.json().get("id")

    #########################################################
    # PAGE HANDLING
    #########################################################
    def get_all_pages_from_space(self, space_key: str, limit: int = 1000) -> List[Dict[str, Any]]:
        """Fetch all pages for a given Confluence space."""
        pages, start = [], 0
        while True:
            result = self.client.get_all_pages_from_space(space=space_key, start=start, limit=limit, expand="version")
            if not result:
                break
            for page in result:
                pages.append({
                    "ID": page.get("id"),
                    "Title": page.get("title"),
                    "Version": page.get("version", {}).get("when")
                })
            if len(result) < limit:
                break
            start += limit
        return pages

    def get_page_tracker_file_path(self, dataset_name: str) -> str:
        return os.path.join(CACHE_DIR, dataset_name, "page_tracker.txt")

    def load_previous_page_list(self, dataset_name: str) -> list:
        path = self.get_page_tracker_file_path(dataset_name)
        if not os.path.exists(path):
            return []
        return json.loads(open(path, encoding="utf-8").read() or "[]")

    def save_current_page_list(self, dataset_name: str, pages: list):
        with open(self.get_page_tracker_file_path(dataset_name), "w", encoding="utf-8") as f:
            f.write(json.dumps(pages, indent=2))

    def compare_page_lists(self, previous: list, current: list) -> dict:
        prev, curr = {p["ID"]: p for p in previous}, {p["ID"]: p for p in current}
        return {
            "added": [p for pid, p in curr.items() if pid not in prev],
            "removed": [p for pid, p in prev.items() if pid not in curr],
            "updated": [p for pid, p in curr.items() if pid in prev and p["Version"] != prev[pid]["Version"]],
        }

    def fetch_page_content(self, page_id: str) -> str:
        url = f"{self.base_url}/wiki/rest/api/content/{page_id}?expand=body.export_view"
        resp = requests.get(url, auth=HTTPBasicAuth(self.email, self.api_token))
        resp.raise_for_status()
        return resp.json().get("body", {}).get("export_view", {}).get("value", "")

    def clean_text(self, html_content: str) -> str:
        soup = BeautifulSoup(html_content, "html.parser")
        for tag in soup(["script", "style", "ac:placeholder", "ac:link"]):
            tag.decompose()
        text = soup.get_text(separator=" ")
        return re.sub(r'\s+', ' ', html.unescape(text)).strip()

    def upload_to_dataset(self, dataset_id: str, page_key: str, text: str):
        headers = {"Authorization": f"Bearer {DIFY_API_KEY}", "Content-Type": "application/json"}
        payload = {
            "name": page_key, "text": text, "indexing_technique": "high_quality",
            "process_rule": {
                "mode": "custom",
                "rules": {
                    "pre_processing_rules": [{"id": "remove_extra_spaces", "enabled": True}],
                    "segmentation": {"separator": "\n\n", "max_tokens": 1024, "chunk_overlap": 100}
                }
            }
        }
        res = requests.post(f"{DIFY_BASE_URL}/datasets/{dataset_id}/document/create-by-text",
                            headers=headers, json=payload, verify=False)
        if res.status_code >= 400:
            print("[ERROR UPLOAD PAGE]", res.text)
        res.raise_for_status()
        print(f"[UPLOAD] Page '{page_key}' uploaded")

    def delete_from_dataset(self, dataset_id: str, doc_name: str):
        headers = {"Authorization": f"Bearer {DIFY_API_KEY}"}
        resp = requests.get(f"{DIFY_BASE_URL}/datasets/{dataset_id}/documents", headers=headers, verify=False)
        if resp.status_code != 200:
            return
        for doc in resp.json().get("data", []):
            if doc.get("name") == doc_name:
                del_url = f"{DIFY_BASE_URL}/datasets/{dataset_id}/documents/{doc['id']}"
                d = requests.delete(del_url, headers=headers, verify=False)
                if d.status_code in (200, 204):
                    print(f"[DELETE] {doc_name} deleted")
                return

    #########################################################
    # ATTACHMENT HANDLING
    #########################################################

    def list_attachments_by_page_id(self, page_id: str) -> List[Dict[str, Any]]:
        url = f"{self.base_url}/wiki/rest/api/content/{page_id}/child/attachment?expand=version"
        resp = requests.get(url, auth=HTTPBasicAuth(self.email, self.api_token))
        resp.raise_for_status()
        return [
            {
                "ID": att.get("id"),
                "Title": att.get("title"),
                "Version": att.get("version", {}).get("when"),
                "PageID": page_id
            }
            for att in resp.json().get("results", [])
        ]

    def get_attachment_tracker_file_path(self, dataset_name: str) -> str:
        return os.path.join(CACHE_DIR, dataset_name, "attachment_tracker.txt")

    def load_previous_attachment_list(self, dataset_name: str) -> list:
        path = self.get_attachment_tracker_file_path(dataset_name)
        if not os.path.exists(path):
            return []
        return json.loads(open(path, encoding="utf-8").read() or "[]")

    def save_current_attachment_list(self, dataset_name: str, attachments: list):
        with open(self.get_attachment_tracker_file_path(dataset_name), "w", encoding="utf-8") as f:
            f.write(json.dumps(attachments, indent=2))

    def compare_attachment_lists(self, previous: list, current: list) -> dict:
        prev, curr = {a["ID"]: a for a in previous}, {a["ID"]: a for a in current}
        return {
            "added": [a for aid, a in curr.items() if aid not in prev],
            "removed": [a for aid, a in prev.items() if aid not in curr],
            "updated": [a for aid, a in curr.items() if aid in prev and a["Version"] != prev[aid]["Version"]],
        }

    def download_attachment(self, page_id: str, att: dict) -> str:
        filename = att.get("Title") or att.get("title") or f"att_{att.get('ID')}"
        download_url = f"{self.base_url}/wiki/download/attachments/{page_id}/{filename}"
        resp = requests.get(download_url, auth=HTTPBasicAuth(self.email, self.api_token), stream=True, verify=False)
        resp.raise_for_status()
        tmp_dir = tempfile.mkdtemp(prefix="confluence_att_")
        local_path = os.path.join(tmp_dir, filename)
        with open(local_path, "wb") as f:
            for chunk in resp.iter_content(8192):
                f.write(chunk)
        print(f"[ATTACHMENT] Downloaded {filename} → {local_path}")
        return local_path

    def upload_attachment_into_dataset(self, dataset_id: str, file_path: str, doc_name: str):
        """Uploads an attachment into a Dify dataset."""
        url = f"{DIFY_BASE_URL}/datasets/{dataset_id}/document/create-by-file"
        headers = {"Authorization": f"Bearer {DIFY_API_KEY}"}

        # Build the config as per API docs
        config = {
            "indexing_technique": "high_quality",
            "doc_form": "text_model",
            "process_rule": {
                "mode": "custom",
                "rules": {
                    "pre_processing_rules": [
                        {"id": "remove_extra_spaces", "enabled": True},
                        {"id": "remove_urls_emails", "enabled": True}
                    ],
                    "segmentation": {
                        "separator": "###",
                        "max_tokens": 500
                    }
                }
            }
        }

        with open(file_path, "rb") as f:
            files = {
                "data": (None, json.dumps(config), "text/plain"),
                "file": (doc_name, f, "application/octet-stream"),
            }
            res = requests.post(url, headers=headers, files=files, verify=False)

        if res.status_code == 200:
            print(f"[UPLOAD ATTACHMENT] {doc_name} uploaded successfully")
        else:
            print(f"[ERROR UPLOAD ATTACHMENT] {res.text}")
            res.raise_for_status()

    #########################################################
    # SYNC LOGIC
    #########################################################

    def sync_dataset(self, space_key: str, limit: int = 50):
        """Syncs Confluence space (pages + attachments) into a Dify dataset."""

        # Dataset name format
        domain = self.base_url.split("//")[-1].split(".")[0]
        dataset_name = f"{domain}_{space_key}"

        # Get or create dataset
        dataset_id = self.get_or_create_dataset(space_key)

        # ---------------- PAGES ----------------
        current_pages = self.get_all_pages_from_space(space_key, limit=limit)
        previous_pages = self.load_previous_page_list(dataset_name)
        diff_pages = self.compare_page_lists(previous_pages, current_pages)

        # Handle added/updated pages
        for page in diff_pages["added"] + diff_pages["updated"]:
            page_id = page["ID"]
            title = page["Title"]
            print(f"[SYNC] Uploading page: {title}")
            raw_html = self.fetch_page_content(page_id)
            cleaned_text = self.clean_text(raw_html)
            self.upload_to_dataset(dataset_id, title, cleaned_text)

        # Handle removed pages
        for page in diff_pages["removed"]:
            title = page["Title"]
            print(f"[DELETE] Removing page: {title}")
            self.delete_from_dataset(dataset_id, title)

        # Save new page list
        self.save_current_page_list(dataset_name, current_pages)

        # ---------------- ATTACHMENTS ----------------
        current_attachments = []
        for page in current_pages:
            page_id = page["ID"]
            current_attachments.extend(self.list_attachments_by_page_id(page_id))

        previous_attachments = self.load_previous_attachment_list(dataset_name)
        diff_atts = self.compare_attachment_lists(previous_attachments, current_attachments)

        # Handle added/updated attachments
        for att in diff_atts["added"] + diff_atts["updated"]:
            page_id = att["PageID"]
            att_title = att["Title"]
            print(f"[SYNC] Uploading attachment: {att_title}")
            local_path = self.download_attachment(page_id, att)
            self.upload_attachment_into_dataset(dataset_id, local_path, att_title)

        # Handle removed attachments
        for att in diff_atts["removed"]:
            att_title = att["Title"]
            print(f"[DELETE] Removing attachment: {att_title}")
            self.delete_from_dataset(dataset_id, att_title)

        # Save new attachment list
        self.save_current_attachment_list(dataset_name, current_attachments)

        print(f"[DONE] Sync completed for dataset: {dataset_name}")
