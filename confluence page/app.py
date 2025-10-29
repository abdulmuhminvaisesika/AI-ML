from flask import Flask, request, jsonify
from utils.confluence_handler import fetch_and_store_confluence_data

app = Flask(__name__)

@app.route('/fetch_confluence', methods=['POST'])
def fetch_confluence():
    body = request.json
    required_fields = ['email', 'api_token', 'domain', 'Qdrant_url', 'qdrant_api_key']

    if not all(field in body for field in required_fields):
        return jsonify({"error": "Missing required fields"}), 400

    try:
        collection, chunk_count, page_count = fetch_and_store_confluence_data(
            email=body['email'],
            api_token=body['api_token'],
            domain=body['domain'],
            qdrant_url=body['Qdrant_url'],
            qdrant_api_key=body['qdrant_api_key']
        )

        return jsonify({
            "message": "Successfully embedded and stored in Qdrant",
            "collection": collection,
            "total_chunks": chunk_count,
            "total_pages_processed": page_count
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == '__main__':
    app.run(debug=True, port=5001)
