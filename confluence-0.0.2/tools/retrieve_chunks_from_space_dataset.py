from collections.abc import Generator
from typing import Any, List, Dict
import os
import requests
from dotenv import load_dotenv
from urllib.parse import urlparse

from dify_plugin import Tool
from dify_plugin.entities.tool import ToolInvokeMessage

# Load environment variables
load_dotenv()
DIFY_API_KEY = os.getenv("DIFY_API_KEY")
DIFY_BASE_URL = os.getenv("DIFY_BASE_URL")


class RetrieveChunksFromSpaceDataset(Tool):
    def _invoke(self, tool_parameters: dict[str, Any]) -> Generator[ToolInvokeMessage]:
        """
        Retrieve top 10 chunks from a space-level dataset in Dify.
        """
        # Input parameters
        space_key = tool_parameters.get("space_key")
        query = tool_parameters.get("query")
        if not space_key or not query:
            yield self.create_text_message("Missing required parameters: space_key or query")
            return

        # Compute domain from plugin runtime credentials URL
        base_url = self.runtime.credentials.get("url", "")
        domain = urlparse(base_url).netloc.split('.')[0] if base_url else "default"
        dataset_name = f"{domain}_{space_key}"

        # Get or create dataset
        dataset_id = self.get_or_create_dataset(dataset_name)
        if not dataset_id:
            yield self.create_text_message(f"Dataset '{dataset_name}' not found or could not be created")
            return

        # Prepare retrieve API request
        url = f"{DIFY_BASE_URL}/datasets/{dataset_id}/retrieve"
        payload = {
            "query": query,
            "retrieval_model": {
                "search_method": "hybrid_search",
                "reranking_enable": True,
                "reranking_mode": "dense",
                "reranking_model": {
                    "reranking_provider_name": "openai",
                    "reranking_model_name": "text-embedding-3-small"
                },
                "weights": {"bm25": 0.4, "embedding": 0.6},
                "top_k": 7,
                "score_threshold_enabled": True,
                "score_threshold": 0.65
            }
        }
        headers = {"Authorization": f"Bearer {DIFY_API_KEY}", "Content-Type": "application/json"}

        try:
            response = requests.post(url, headers=headers, json=payload, verify=False)
            response.raise_for_status()
            records = response.json().get("records", [])
            if not records:
                yield self.create_text_message("No chunks found for this query")
                return

            # Simplify chunk output
            chunks: List[Dict[str, str]] = []
            for record in records:
                segment = record.get("segment", {})
                document = segment.get("document", {})
                chunks.append({
                    "content": segment.get("content", ""),
                    "document_name": document.get("name", "Unknown")
                })

            for chunk in chunks:
                yield self.create_text_message(f"Document: {chunk['document_name']}\nContent: {chunk['content']}\n")

        except Exception as e:
            yield self.create_text_message(f"Failed to retrieve chunks: {str(e)}")

    def get_or_create_dataset(self, dataset_name: str) -> str:
        """
        Get existing dataset by name, or create a new one in Dify.
        """
        headers = {"Authorization": f"Bearer {DIFY_API_KEY}", "Content-Type": "application/json"}

        # List datasets
        resp = requests.get(f"{DIFY_BASE_URL}/datasets?page=1&limit=100", headers=headers, verify=False)
        if resp.status_code != 200:
            return ""

        for ds in resp.json().get("data", []):
            if ds.get("name") == dataset_name:
                return ds.get("id")

        # Create dataset
        payload = {
            "name": dataset_name,
            "description": f"Space-level dataset for {dataset_name}",
            "indexing_technique": "high_quality"
        }
        create_resp = requests.post(f"{DIFY_BASE_URL}/datasets", headers=headers, json=payload, verify=False)
        if create_resp.status_code == 200:
            return create_resp.json().get("id", "")
        return ""
