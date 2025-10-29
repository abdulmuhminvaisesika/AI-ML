def get_transcript_and_upload(video_url: str, dataset_id: str, dify_api_key: str):
    import os
    import re
    import requests
    from datetime import timedelta
    from youtube_transcript_api import YouTubeTranscriptApi, NoTranscriptFound, TranscriptsDisabled, VideoUnavailable
    import yt_dlp
    import base64

    def format_timestamp(seconds: float) -> str:
        minutes, sec = divmod(int(seconds), 60)
        hrs, minutes = divmod(minutes, 60)
        return f"{hrs:02}:{minutes:02}:{sec:02}"

    def extract_video_id(url: str) -> str:
        pattern1 = r'(?:v=|v\/|embed\/|watch\?v=)([a-zA-Z0-9_-]{11})'
        pattern2 = r'youtu\.be\/([a-zA-Z0-9_-]{11})'
        match1 = re.search(pattern1, url)
        if match1:
            return match1.group(1)
        match2 = re.search(pattern2, url)
        if match2:
            return match2.group(1)
        raise ValueError("Invalid YouTube URL")

    class LingoTranslatePost:
        def __init__(self):
            self.API = "http://164.52.198.205:3001/transcribe"

        @staticmethod
        def encode(audio_path):
            with open(audio_path, "rb") as audio_file:
                return base64.b64encode(audio_file.read()).decode('utf-8')

        def transcribe(self, audio_path):
            payload = {"Input_request": {"audio_string": self.encode(audio_path)}}
            res = requests.post(self.API, json=payload, headers={"accept": "application/json", "Content-Type": "application/json"})
            if res.status_code != 200:
                return None, f"ASR API failed: {res.status_code}"
            return res.json(), None

    try:
        video_id = extract_video_id(video_url)
    except Exception as e:
        return False, f"Invalid YouTube URL: {e}"

    transcript = []
    source = "YouTube"

    try:
        result = YouTubeTranscriptApi.get_transcript(video_id)
        transcript = [[format_timestamp(entry['start']), entry['text']] for entry in result]
    except (NoTranscriptFound, TranscriptsDisabled, VideoUnavailable):
        # fallback to ASR
        try:
            audio_path = f"./temp_audio/{video_id}.mp3"
            os.makedirs("./temp_audio", exist_ok=True)
            ydl_opts = {
                'format': 'bestaudio/best',
                'outtmpl': audio_path,
                'postprocessors': [{
                    'key': 'FFmpegExtractAudio',
                    'preferredcodec': 'mp3',
                    'preferredquality': '192',
                }],
                'quiet': True
            }
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                ydl.download([video_url])

            asr = LingoTranslatePost()
            result, err = asr.transcribe(audio_path)
            if err:
                return False, err
            transcript = [[format_timestamp(seg['start']), seg['text']] for seg in result['segments']]
            source = "ASR"
        except Exception as e:
            return False, f"ASR error: {e}"

    content = f"Source: {source}\nVideo URL: {video_url}\n\n" + "\n".join([f"[{ts}] {txt}" for ts, txt in transcript])
    file_name = video_url.replace("https://", "").replace("http://", "").replace("/", "_") + ".txt"

    upload_url = f"http://164.52.196.111:6860/v1/datasets/{dataset_id}/document/create-by-text"
    headers = {"Authorization": f"Bearer {dify_api_key}", "Content-Type": "application/json"}
    payload = {
        "name": file_name,
        "text": content,
        "indexing_technique": "high_quality",
        "process_rule": {
            "mode": "custom",
            "rules": {
                "pre_processing_rules": [{"id": "remove_extra_spaces", "enabled": True}],
                "segmentation": {"separator": "####", "max_tokens": 3072, "chunk_overlap": 300}
            }
        }
    }

    response = requests.post(upload_url, headers=headers, json=payload)
    if response.status_code == 200:
        return True, f"Transcript uploaded for: {video_url}"
    else:
        return False, f"Upload failed: {response.status_code} - {response.text}"
