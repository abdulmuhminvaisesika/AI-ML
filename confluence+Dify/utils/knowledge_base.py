import requests
import os

BASE_DIR = r"C:\Users\abdul.muhmin\Al&ML\confluence+Dify\Documents\ConfluenceData"
PAGE_ID_FOLDER = os.path.join(BASE_DIR, "PAGE_ID")
ATTACHMENT_FOLDER = os.path.join(BASE_DIR, "ATTACHMENT")
DOC_TRACK_FOLDER = os.path.join(BASE_DIR, "DOCUMENT_TRACK")
SAVED_DATA_FOLDER = os.path.join(BASE_DIR, "SAVED_DATA")

def clear_folder_contents(folder_path):
    if os.path.exists(folder_path):
        for file in os.listdir(folder_path):
            file_path = os.path.join(folder_path, file)
            if os.path.isfile(file_path):
                os.remove(file_path)

def get_or_create_dataset(domain, dify_api_key):
    base_url = "http://164.52.196.111:6860/v1/datasets"
    headers = {"Authorization": f"Bearer {dify_api_key}"}

    # 1. Fetch existing datasets
    resp = requests.get(f"{base_url}?page=1&limit=100", headers=headers)
    if resp.status_code != 200:
        raise Exception("Failed to fetch datasets")

    datasets = resp.json().get('data', [])
    for dataset in datasets:
        if dataset['name'] == domain:
            return dataset['id']  # dataset already exists

    # 2. Create new dataset
    payload = {
        "name": domain,
        "description": f"Knowledge base for domain {domain}",
        "indexing_technique": "high_quality"
    }
    resp = requests.post(base_url, json=payload, headers=headers)
    if resp.status_code == 200:
        #  Clear folder contents ONLY if new dataset is created
        clear_folder_contents(PAGE_ID_FOLDER)
        clear_folder_contents(ATTACHMENT_FOLDER)
        clear_folder_contents(DOC_TRACK_FOLDER)
        clear_folder_contents(SAVED_DATA_FOLDER)
        return resp.json()['id']
    else:
        raise Exception("Failed to create dataset")
