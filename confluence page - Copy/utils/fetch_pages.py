import os
import requests
from requests.auth import HTTPBasicAuth
from bs4 import BeautifulSoup


def fetch_confluence_pages(email, api_token, domain):
    all_pages = []
    start = 0
    limit = 25

    while True:
        url = f"{domain}/wiki/api/v2/pages?limit={limit}&start={start}"
        response = requests.get(url, auth=HTTPBasicAuth(email, api_token))

        if response.status_code != 200:
            return {"error": "Failed to fetch pages", "details": response.text}, response.status_code

        data = response.json()
        pages = data.get("results", [])

        for page in pages:
            page_id = page["id"]
            title = page["title"]

            content_url = f"{domain}/wiki/rest/api/content/{page_id}?expand=body.export_view"
            content_res = requests.get(content_url, auth=HTTPBasicAuth(email, api_token))

            content = "Not available"
            if content_res.status_code == 200:
                content_json = content_res.json()
                content = content_json.get("body", {}).get("export_view", {}).get("value", "")

            all_pages.append({
                "id": page_id,
                "title": title,
                "content": content
            })

        if not data.get("_links", {}).get("next"):
            break

        start += limit

    return {"total_pages": len(all_pages), "pages": all_pages}


def extract_from_confluence_pages(pages, email, api_token, domain):
    text_data = []
    video_urls = []
    youtube_urls = []
    image_urls = []
    file_links = []
    downloaded_file_paths = []
    table_data = []
    code_blocks = []
    all_urls = []
    metadata_list = []

    video_extensions = ('.mp4', '.mov', '.webm', '.avi')
    image_extensions = ('.jpg', '.jpeg', '.png', '.svg', '.gif', '.bmp')
    file_extensions = ('.pdf', '.docx', '.pptx', '.xlsx', '.csv', '.zip')

    for page in pages:
        content_html = page.get("content", "")
        title = page.get("title", "")
        soup = BeautifulSoup(content_html, "html.parser")

        # Extract plain text
        text = soup.get_text(separator=" ", strip=True)
        if text:
            text_data.append({"title": title, "text": text})

        # Extract all URLs
        for link in soup.find_all("a", href=True):
            href = link["href"]
            all_urls.append(href)

            # YouTube URLs
            if "youtube.com" in href or "youtu.be" in href:
                youtube_urls.append(href)
            elif href.endswith(video_extensions):
                video_urls.append(href)
            elif href.endswith(image_extensions):
                image_urls.append(href)
            elif href.endswith(file_extensions):
                file_links.append(href)

                # Attempt to download if it's a Confluence attachment
                if href.startswith("/download/attachments/"):
                    full_url = f"{domain}{href}"
                    file_name = href.split("/")[-1]

                    try:
                        response = requests.get(full_url, auth=HTTPBasicAuth(email, api_token))
                        if response.status_code == 200:
                            os.makedirs("downloads", exist_ok=True)
                            save_path = os.path.join("downloads", file_name)
                            with open(save_path, "wb") as f:
                                f.write(response.content)
                            downloaded_file_paths.append(save_path)
                    except Exception as e:
                        print(f"Failed to download {full_url}: {e}")

        # Extract tables
        for table in soup.find_all("table"):
            rows = []
            for tr in table.find_all("tr"):
                cells = [td.get_text(strip=True) for td in tr.find_all(["td", "th"])]
                if cells:
                    rows.append(cells)
            if rows:
                table_data.append({"title": title, "table": rows})

        # Extract code blocks
        for code in soup.find_all(["pre", "code"]):
            code_text = code.get_text(strip=True)
            if code_text:
                code_blocks.append({"title": title, "code": code_text})

        metadata_list.append({
            "title": title,
            "length": len(text),
            "word_count": len(text.split())
        })

    return {
        "text_data": text_data,
        "video_urls": list(set(video_urls)),
        "youtube_urls": list(set(youtube_urls)),
        "image_urls": list(set(image_urls)),
        "file_links": list(set(file_links)),
        "downloaded_file_paths": downloaded_file_paths,
        "table_data": table_data,
        "code_blocks": code_blocks,
        "all_urls": list(set(all_urls)),
        "metadata": metadata_list
    }
