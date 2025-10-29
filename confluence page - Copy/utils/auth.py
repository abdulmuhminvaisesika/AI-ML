import base64

def get_auth_header(email, api_token):
    token = f"{email}:{api_token}"
    encoded = base64.b64encode(token.encode()).decode()
    return {
        "Authorization": f"Basic {encoded}",
        "Content-Type": "application/json"
    }
