import os
import uuid
import requests
from bs4 import BeautifulSoup
from requests.auth import HTTPBasicAuth

from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain_huggingface import HuggingFaceEmbeddings

from qdrant_client import QdrantClient
from qdrant_client.http.models import Distance, VectorParams
from langchain_qdrant import QdrantVectorStore

from langchain.document_loaders import (
    PyPDFLoader,
    UnstructuredWordDocumentLoader,
    CSVLoader,
    TextLoader,
    UnstructuredExcelLoader
)

def clean_text(text):
    lines = text.splitlines()
    lines = [line.strip() for line in lines if line.strip()]
    return "\n".join(lines)


def fetch_confluence_text_and_files(email, api_token, domain, download_dir="downloads"):
    os.makedirs(download_dir, exist_ok=True)
    all_docs = []
    start = 0
    limit = 25

    while True:
        url = f"{domain}/wiki/api/v2/pages?limit={limit}&start={start}"
        response = requests.get(url, auth=HTTPBasicAuth(email, api_token))
        if response.status_code != 200:
            raise Exception("Failed to fetch pages: " + response.text)
        print("Confluence authorized, pages fetched")

        data = response.json()
        pages = data.get("results", [])

        for page in pages:
            try:
                page_id = page["id"]
                title = page["title"]

                # Extract page text
                content_url = f"{domain}/wiki/rest/api/content/{page_id}?expand=body.export_view"
                content_res = requests.get(content_url, auth=HTTPBasicAuth(email, api_token))
                if content_res.status_code == 200:
                    html = content_res.json().get("body", {}).get("export_view", {}).get("value", "")
                    soup = BeautifulSoup(html, "html.parser")
                    text = soup.get_text(separator="\n").strip()
                    text = clean_text(text)
                    if text:
                        all_docs.append(text)
                        print(f"Page '{title}' cleaned and added")

                # Fetch attachments
                attachments_url = f"{domain}/wiki/rest/api/content/{page_id}/child/attachment"
                attach_res = requests.get(attachments_url, auth=HTTPBasicAuth(email, api_token))
                if attach_res.status_code == 200:
                    attachments = attach_res.json().get("results", [])
                    for file in attachments:
                        file_name = file["title"]
                        download_link = file["_links"]["download"]
                        full_url = domain + download_link
                        file_path = os.path.join(download_dir, file_name)

                        file_data = requests.get(full_url, auth=HTTPBasicAuth(email, api_token))
                        with open(file_path, "wb") as f:
                            f.write(file_data.content)
                        print(f"Downloaded file: {file_name}")

                        # Load file content using LangChain loader
                        try:
                            if file_name.endswith(".pdf"):
                                loader = PyPDFLoader(file_path)
                            elif file_name.endswith(".docx"):
                                loader = UnstructuredWordDocumentLoader(file_path)
                            elif file_name.endswith(".csv"):
                                loader = CSVLoader(file_path)
                            elif file_name.endswith(".txt"):
                                loader = TextLoader(file_path)
                            elif file_name.endswith(".xlsx"):
                                loader = UnstructuredExcelLoader(file_path)
                            else:
                                print(f"Unsupported file type: {file_name}")
                                continue

                            docs = loader.load()
                            for doc in docs:
                                all_docs.append(doc.page_content)

                        except Exception as e:
                            print(f"Failed to load {file_name}: {e}")

            except Exception as e:
                print(f"Error processing page {page_id}: {str(e)}")

        if not data.get("_links", {}).get("next"):
            break
        start += limit

    return all_docs

def fetch_and_store_confluence_data(email, api_token, domain, qdrant_url, qdrant_api_key):
    documents = fetch_confluence_text_and_files(email, api_token, domain)
    splitter = RecursiveCharacterTextSplitter(chunk_size=750, chunk_overlap=100)
    chunks = splitter.create_documents(documents)
    print(f"Total chunks created: {len(chunks)}")

    embeddings = HuggingFaceEmbeddings(model_name="sentence-transformers/all-MiniLM-L6-v2")
    collection_name = f"confluence-{uuid.uuid4().hex[:6]}"
    qdrant_client = QdrantClient(url=qdrant_url, api_key=qdrant_api_key)

    qdrant_client.recreate_collection(
        collection_name=collection_name,
        vectors_config=VectorParams(size=384, distance=Distance.COSINE)
    )

    qdrant = QdrantVectorStore(
        client=qdrant_client,
        collection_name=collection_name,
        embedding=embeddings,
    )
    qdrant.add_documents(chunks)
    print("Documents embedded and stored in Qdrant")

    return collection_name, len(chunks), len(documents)
