import requests
import os
import re
from bs4 import BeautifulSoup
from requests.auth import HTTPBasicAuth
import time
import json
import base64
from openai import OpenAI
import pandas as pd

from utils.YT import get_transcript_and_upload

#  Local paths for Windows
BASE_DIR = r"C:\Users\abdul.muhmin\Al&ML\confluence+Dify\Documents\ConfluenceData"
PAGE_ID_FOLDER = os.path.join(BASE_DIR, "PAGE_ID")
ATTACHMENT_FOLDER = os.path.join(BASE_DIR, "ATTACHMENT")
DOC_TRACK_FOLDER = os.path.join(BASE_DIR, "DOCUMENT_TRACK")
SAVED_DATA_FOLDER = os.path.join(BASE_DIR, "SAVED_DATA")  
YOUTUBE_TRACK_FOLDER = os.path.join(BASE_DIR, "YOUTUBE_ID_TRACK")
YOUTUBE_TRACK_FILE = os.path.join(YOUTUBE_TRACK_FOLDER, "YOUTUBE_IDS_WITH_PAGES.txt")

 
#  Ensure all directories exist
os.makedirs(PAGE_ID_FOLDER, exist_ok=True)
os.makedirs(ATTACHMENT_FOLDER, exist_ok=True)
os.makedirs(DOC_TRACK_FOLDER, exist_ok=True)
os.makedirs(SAVED_DATA_FOLDER, exist_ok=True)
os.makedirs(YOUTUBE_TRACK_FOLDER, exist_ok=True)
 
class LingoInferenceVLM:
    def __init__(self):
        self.client = OpenAI(api_key='sandlogic', base_url='http://164.52.205.212:4141/v1')
        self.model_name = self.client.models.list().data[0].id
 
    def load_image(self, image_path):
        with open(image_path, "rb") as f:
            image_data = base64.b64encode(f.read()).decode("utf-8")
            image_url = f"data:image/jpeg;base64,{image_data}"
            return image_url
 
    def generate(self, image_path, text_prompt, max_tokens):
        image_data = self.load_image(image_path)
        response = self.client.chat.completions.create(
            model=self.model_name,
            messages=[{
                "role": "user",
                "content": [
                    {"type": "text", "text": text_prompt},
                    {"type": "image_url", "image_url": {"url": image_data}},
                ],
            }],
            max_tokens=max_tokens
        )
        return response.choices[0].message.content
 
vlm_inference = LingoInferenceVLM()
IMAGE_PROMPT = """Extract everything and all the information present in the image and give a detailed description on the image."""
 
def clean_text(text):
    lines = text.splitlines()
    lines = [line.strip() for line in lines if line.strip()]
    return "\n".join(lines)

def extract_youtube_id(link):
    match = re.search(r"(?:v=|youtu\.be/)([\w-]+)", link)
    return match.group(1) if match else None
 
def sync_pages_and_attachments(domain, email, api_key, dataset_id, dify_api_key):
    t1 = time.time()
 
    # Step 1: Fetch pages with version
    all_pages = []
    start = 0
    limit = 100
    while True:
        page_url = f"{domain}/wiki/rest/api/content?type=page&limit={limit}&start={start}&expand=version"
        resp = requests.get(page_url, auth=HTTPBasicAuth(email, api_key))
        if resp.status_code != 200:
            return False, "Failed to fetch page list"
        batch = resp.json().get('results', [])
        all_pages.extend(batch)
        if len(batch) < limit:
            break
        start += limit
 
    page_versions = {page['id']: page.get('version', {}).get('when', '') for page in all_pages}
 
    # Step 2: Fetch attachments with version
    all_attachments = []
    start = 0
    while True:
        attach_url = f"{domain}/wiki/api/v2/attachments?limit={limit}&start={start}"
        resp = requests.get(attach_url, headers={"Accept": "application/json"}, auth=HTTPBasicAuth(email, api_key))
        if resp.status_code != 200:
            print("Failed to fetch attachments")
            break
        batch = resp.json().get('results', [])
        all_attachments.extend(batch)
        if len(batch) < limit:
            break
        start += limit
 
    attachment_versions = {att['title']: att.get('version', {}).get('createdAt', '') for att in all_attachments}
 
    # Step 3: Load cached versions
    page_version_file = os.path.join(PAGE_ID_FOLDER, "PAGE_ID.txt")
    attachment_version_file = os.path.join(ATTACHMENT_FOLDER, "ATTACHMENT_VERSION.txt")
    doc_track_path = os.path.join(DOC_TRACK_FOLDER, "DOCS_KB.json")
 
    saved_page_versions = {}
    if os.path.exists(page_version_file):
        with open(page_version_file, "r") as f:
            saved_page_versions = eval(f.read())
 
    saved_attachment_versions = {}
    if os.path.exists(attachment_version_file):
        with open(attachment_version_file, "r") as f:
            saved_attachment_versions = eval(f.read())
 
    saved_docs = {}
    if os.path.exists(doc_track_path):
        with open(doc_track_path, "r") as f:
            saved_docs = json.load(f)

    saved_youtube_map = {}
    if os.path.exists(YOUTUBE_TRACK_FILE):
        with open(YOUTUBE_TRACK_FILE, "r") as f:
            saved_youtube_map = json.load(f)
 
    headers = {
        "Authorization": f"Bearer {dify_api_key}",
        "Content-Type": "application/json"
    }
 
    # Step 4: Upload updated pages
    current_page_ids = set(page_versions.keys())
    cached_page_ids = set(saved_page_versions.keys())
    
    current_youtube_map = {}
 
    for page_id in page_versions:
        if page_versions[page_id] != saved_page_versions.get(page_id):
            content_url = f"{domain}/wiki/rest/api/content/{page_id}?expand=body.export_view"
            resp = requests.get(content_url, auth=HTTPBasicAuth(email, api_key))
            html = resp.json().get('body', {}).get('export_view', {}).get('value', '')
            text = clean_text(BeautifulSoup(html, "html.parser").get_text("\n"))

            youtube_links = re.findall(r"(https?://(?:www\.)?(?:youtube\.com/watch\?v=[\w-]+|youtu\.be/[\w-]+))", text)
            page_key = f"Page_{page_id}"
            current_ids = set()

            for link in youtube_links:
                video_id = extract_youtube_id(link)
                if not video_id:
                    continue
                current_ids.add(video_id)
                if saved_youtube_map.get(page_key, []) and video_id in saved_youtube_map[page_key]:
                    continue
                success, _ = get_transcript_and_upload(link, dataset_id, dify_api_key)
                if success:
                    saved_youtube_map.setdefault(page_key, []).append(video_id)

            current_youtube_map[page_key] = list(current_ids)

            # Upload page itself
            payload = {
                "name": page_key,
                "text": text,
                "indexing_technique": "high_quality",
                "process_rule": {
                    "mode": "custom",
                    "rules": {
                        "pre_processing_rules": [{"id": "remove_extra_spaces", "enabled": True}],
                        "segmentation": {"separator": "\n\n", "max_tokens": 1024, "chunk_overlap": 100}
                    }
                }
            }
            dify_text_url = f"http://164.52.196.111:6860/v1/datasets/{dataset_id}/document/create-by-text"
            res = requests.post(dify_text_url, headers=headers, json=payload)

    # Step 4: Handle removed YouTube IDs (only for updated pages)
    for page_key in current_youtube_map:
        old_ids = set(saved_youtube_map.get(page_key, []))
        new_ids = set(current_youtube_map.get(page_key, []))
        removed_ids = old_ids - new_ids
        if removed_ids:
            doc_list_url = f"http://164.52.196.111:6860/v1/datasets/{dataset_id}/documents"
            res = requests.get(doc_list_url, headers=headers)
            if res.status_code == 200:
                docs = res.json().get("data", [])
                name_to_id = {doc['name']: doc['id'] for doc in docs}
                for yt_id in removed_ids:
                    doc_name = f"www.youtube.com_watch?v={yt_id}.txt"
                    doc_id = name_to_id.get(doc_name)
                    if doc_id:
                        delete_url = f"http://164.52.196.111:6860/v1/datasets/{dataset_id}/documents/{doc_id}"
                        del_res = requests.delete(delete_url, headers=headers)
                        if del_res.status_code == 200:
                            print(f"Deleted YouTube doc: {doc_name}")
            saved_youtube_map[page_key] = list(new_ids)

 
    # Step 5: Upload attachments
    current_att_titles = set(att['title'] for att in all_attachments)
    cached_att_titles = set(saved_attachment_versions.keys())
 
    for att in all_attachments:
        original_filename = att['title']
        current_timestamp = attachment_versions.get(original_filename)
        saved_timestamp = saved_attachment_versions.get(original_filename)
 
 
        if current_timestamp == saved_timestamp:
            continue
 
        download_url = f"{domain}/wiki{att['_links']['download']}"
 
        file_ext = os.path.splitext(original_filename)[1]
 
        IMAGE_EXTENSIONS = {'.jpg', '.jpeg', '.png', '.jfif', '.bmp', '.webp', '.tiff'}
        if file_ext.lower() in IMAGE_EXTENSIONS:
            # IMAGE
            resp = requests.get(download_url, auth=HTTPBasicAuth(email, api_key))
            if resp.status_code != 200:
                continue
            tmp_image_path = os.path.join(SAVED_DATA_FOLDER, f"tmp_{original_filename}")
            with open(tmp_image_path, 'wb') as f:
                f.write(resp.content)
 
            response = vlm_inference.generate(tmp_image_path, IMAGE_PROMPT, 1024)
            base_name = os.path.splitext(original_filename)[0]
            text_file_name = f"{base_name}.txt"
            txt_path = os.path.join(SAVED_DATA_FOLDER, text_file_name)
            with open(txt_path, 'w') as ff:
                ff.write(f"Image Name: {original_filename}\nImage Description:\n{response}")
 
            with open(txt_path, 'rb') as file_data:
                files = {
                    "data": (None, json.dumps({
                        "name": original_filename,
                        "indexing_technique": "high_quality",
                        "process_rule": {
                            "rules": {
                                "pre_processing_rules": [
                                    {"id": "remove_extra_spaces", "enabled": True},
                                    {"id": "remove_urls_emails", "enabled": True}
                                ],
                                "segmentation": {"separator": "###", "max_tokens": 3800, "chunk_overlap": 100}
                            },
                            "mode": "custom"
                        }
                    }), 'text/plain'),
                    "file": (text_file_name, file_data)
                }
                upload_url = f"http://164.52.196.111:6860/v1/datasets/{dataset_id}/document/create-by-file"
                res = requests.post(upload_url, headers={"Authorization": f"Bearer {dify_api_key}"}, files=files)
                if res.status_code == 200:
                    print("uploaded:", original_filename)
 
            saved_attachment_versions[original_filename] = current_timestamp
            os.remove(txt_path)
            os.remove(tmp_image_path)
        else:
            # NON-IMAGE
            resp = requests.get(download_url, auth=HTTPBasicAuth(email, api_key))
            if resp.status_code != 200:
                continue

            file_ext = os.path.splitext(original_filename)[1].lower()
            EXCEL_EXTENSIONS = {'.xlsx', '.xls'}

            if file_ext in EXCEL_EXTENSIONS:
                print("excel found")
                # Convert Excel to text before uploading
                tmp_excel_path = os.path.join(SAVED_DATA_FOLDER, f"tmp_{original_filename}")
                with open(tmp_excel_path, 'wb') as f:
                    f.write(resp.content)

                try:
                    df = pd.read_excel(tmp_excel_path)
                    base_name = os.path.splitext(original_filename)[0]
                    text_file_name = f"{base_name}.txt"
                    txt_path = os.path.join(SAVED_DATA_FOLDER, text_file_name)

                    with open(txt_path, "w", encoding="utf-8") as f:
                        for _, row in df.iterrows():
                            row_text = "; ".join([f"{col}: {row[col]}" for col in df.columns])
                            f.write(row_text + "\n")

                    with open(txt_path, 'rb') as file_data:
                        files = {
                            "data": (None, json.dumps({
                                "name": original_filename,
                                "indexing_technique": "high_quality",
                                "process_rule": {
                                    "rules": {
                                        "pre_processing_rules": [
                                            {"id": "remove_extra_spaces", "enabled": True},
                                            {"id": "remove_urls_emails", "enabled": True}
                                        ],
                                        "segmentation": {"separator": "\n\n", "max_tokens": 2048, "chunk_overlap": 200}
                                    },
                                    "mode": "custom"
                                }
                            }), 'text/plain'),
                            "file": (text_file_name, file_data)
                        }
                        upload_url = f"http://164.52.196.111:6860/v1/datasets/{dataset_id}/document/create-by-file"
                        res = requests.post(upload_url, headers={"Authorization": f"Bearer {dify_api_key}"}, files=files)
                        if res.status_code == 200:
                            print("uploaded:", original_filename)
                            

                    saved_attachment_versions[original_filename] = current_timestamp
                    os.remove(txt_path)
                    os.remove(tmp_excel_path)

                except Exception as e:
                    print(f"Error converting Excel to text: {e}")
                    continue

            else:
                # Other non-image documents (PDF, DOCX, etc.)
                tmp_path = os.path.join(SAVED_DATA_FOLDER, f"tmp_{original_filename}")
                with open(tmp_path, 'wb') as f:
                    f.write(resp.content)

                with open(tmp_path, 'rb') as file_data:
                    files = {
                        "data": (None, json.dumps({
                            "name": original_filename,
                            "indexing_technique": "high_quality",
                            "process_rule": {
                                "rules": {
                                    "pre_processing_rules": [
                                        {"id": "remove_extra_spaces", "enabled": True},
                                        {"id": "remove_urls_emails", "enabled": True}
                                    ],
                                    "segmentation": {"separator": "\n\n", "max_tokens": 2048, "chunk_overlap": 200}
                                },
                                "mode": "custom"
                            }
                        }), 'text/plain'),
                        "file": (original_filename, file_data)
                    }
                    upload_url = f"http://164.52.196.111:6860/v1/datasets/{dataset_id}/document/create-by-file"
                    res = requests.post(upload_url, headers={"Authorization": f"Bearer {dify_api_key}"}, files=files)
                    if res.status_code == 200:
                        print("uploaded:", original_filename)

                saved_attachment_versions[original_filename] = current_timestamp
                os.remove(tmp_path)

 
    # Step 6: Handle deletions
    deleted_page_ids = cached_page_ids - current_page_ids
 
    deleted_att_titles = cached_att_titles - current_att_titles
 
    if deleted_page_ids or deleted_att_titles:
        doc_list_url = f"http://164.52.196.111:6860/v1/datasets/{dataset_id}/documents"
        res = requests.get(doc_list_url, headers=headers)
        if res.status_code == 200:
            docs = res.json().get("data", [])
            name_to_id = {doc['name']: doc['id'] for doc in docs}
 
            for pid in deleted_page_ids:
                doc_key = f"Page_{pid}"
                doc_id = name_to_id.get(doc_key)
                if doc_id:
                    delete_url = f"http://164.52.196.111:6860/v1/datasets/{dataset_id}/documents/{doc_id}"
                    res = requests.delete(delete_url, headers=headers)
                    if res.status_code == 200:
                        print(f"Deleted page: {doc_key}")
                    saved_docs.pop(doc_key, None)
 
            IMAGE_EXTENSIONS = {'.jpg', '.jpeg', '.png', '.jfif', '.bmp', '.webp', '.tiff'}    
            EXCEL_EXTENSIONS = {'.xlsx', '.xls'}    
 
            for title in deleted_att_titles:
                name_to_lookup = title
                ext = os.path.splitext(title)[1].lower()
                if ext in IMAGE_EXTENSIONS or ext in EXCEL_EXTENSIONS:
                    # Convert image name to .txt (what was stored)
                    base_name = os.path.splitext(title)[0]
                    name_to_lookup = f"{base_name}.txt"
 
                doc_id = name_to_id.get(name_to_lookup)
                if doc_id:
                    delete_url = f"http://164.52.196.111:6860/v1/datasets/{dataset_id}/documents/{doc_id}"
                    res = requests.delete(delete_url, headers=headers)
                    if res.status_code == 200:
                        print(f"Deleted attachment: {title}")
                    saved_attachment_versions.pop(title, None)
 
    # Step 7: Save state
    with open(page_version_file, "w") as f:
        f.write(str(page_versions))
    with open(attachment_version_file, "w") as f:
        f.write(str(saved_attachment_versions))
    with open(doc_track_path, "w") as f:
        f.write(json.dumps(saved_docs))
    with open(YOUTUBE_TRACK_FILE, "w") as f:
        json.dump(saved_youtube_map, f, indent=2)
 
    return True, "Sync complete"