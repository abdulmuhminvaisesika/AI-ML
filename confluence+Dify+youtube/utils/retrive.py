import requests

def retrieve_chunks(dataset_id, dify_api_key, query):
    url = f"http://164.52.196.111:6860/v1/datasets/{dataset_id}/retrieve"

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {dify_api_key}"
    }

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
            "weights": {
                "bm25": 0.4,
                "embedding": 0.6
            },
            "top_k": 7,
            "score_threshold_enabled": True,
            "score_threshold": 0.65
        }
    }

    response = requests.post(url, headers=headers, json=payload)

    if response.status_code == 200:
        records = response.json().get("records", [])
        simplified_chunks = []
        for record in records:
            segment = record.get("segment", {})
            document = segment.get("document", {})

            simplified_chunks.append({
                "content": segment.get("content", ""),
                "document_name": document.get("name", "Unknown")
            })
        return simplified_chunks
    else:
        return {"error": "Failed to retrieve chunks", "details": response.text}
