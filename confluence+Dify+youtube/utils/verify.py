import requests
from requests.auth import HTTPBasicAuth

def verify_page(email, api_key, domain):
    url = f"{domain}/wiki/rest/api/user/current"
    response = requests.get(url, auth=HTTPBasicAuth(email, api_key))
    return response.status_code == 200
