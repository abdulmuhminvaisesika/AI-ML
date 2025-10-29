
# app.py
from flask import Flask, request, jsonify
from utils.verify import verify_page
from utils.knowledge_base import get_or_create_dataset
from utils.confluence_sync import sync_pages_and_attachments
from utils.retrive import retrieve_chunks

app = Flask(__name__)

@app.route('/verify', methods=['POST'])
def verify_credentials():
    data = request.get_json()
    email = data.get('email')
    api_key = data.get('confluence_api_key')
    user_input_domain = data['domain'] 
    domain = f"https://{user_input_domain}.atlassian.net"

    if not all([email, api_key, domain]):
        return jsonify({"error": "Missing required parameters"}), 400
    if verify_page(email, api_key, domain):
        return jsonify({"message": "Credentials are valid."}), 200
    return jsonify({"message": "Invalid credentials."}), 401

@app.route('/sync-confluence', methods=['POST'])
def sync():
    data = request.get_json()
    email = data['email']
    user_input_domain = data['domain']
    domain = f"https://{user_input_domain}.atlassian.net"
    confluence_api_key = data['confluence_api_key']
    dify_api_key = "dataset-OofEcsOysamwBLT44tlgqtRU"

    try:
        dataset_id = get_or_create_dataset(user_input_domain, dify_api_key)
        success, msg = sync_pages_and_attachments(domain, email, confluence_api_key, dataset_id, dify_api_key)
        if success:
            return jsonify({"message": msg}), 200
        return jsonify({"error": msg}), 500
    except Exception as e:
        return jsonify({"error": str(e)}), 500




@app.route('/retrieve', methods=['POST'])
def retrieve_chunks_route():
    data = request.get_json()
    
    query = data.get('query')
    domain = data.get('domain')
    confluence_api_key = data.get('confluence_api_key')
    dify_api_key = "dataset-OofEcsOysamwBLT44tlgqtRU"

    if not all([query, domain, confluence_api_key,]):
        return jsonify({"error": "Missing required parameters"}), 400

    dataset_id = get_or_create_dataset(domain, dify_api_key)
    chunks = retrieve_chunks(dataset_id, dify_api_key, query)

    return jsonify(chunks)
    




if __name__ == '__main__':
    app.run(debug=True)

