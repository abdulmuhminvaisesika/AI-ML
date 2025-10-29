import os
import tempfile
import cv2
import numpy as np
import ffmpeg
import whisper
from PIL import Image
from moviepy.editor import VideoFileClip
import google.generativeai as genai
import torch
import torch.nn.functional as F
from torchvision import models, transforms

# -------------------------
# Initialize External Models
# -------------------------

# Whisper model for audio transcription
whisper_model = whisper.load_model("base")

# Gemini Vision model setup
genai.configure(api_key="YOUR_GOOGLE_API_KEY")
gemini_model = genai.GenerativeModel("gemini-pro-vision")

# ResNet-18 for frame feature extraction (used for similarity detection)
resnet = models.resnet18(pretrained=True)
resnet.eval()

# Image preprocessing pipeline for ResNet
preprocess = transforms.Compose([
    transforms.Resize((224, 224)),
    transforms.ToTensor(),
])


class VideoTextExtractor:
    """
    Class to extract meaningful text from video using captions, audio, or VLM.
    Fallback order: Captions > Audio > Vision (VLM).
    """

    def __init__(self):
        self.whisper = whisper_model
        self.vlm = gemini_model

    def extract(self, video_path: str) -> dict:
        """
        Core function to extract text from a video.
        Dynamically decides whether to use captions, audio, or VLM.
        """
        # Priority 1: Use embedded captions if available
        if caption := self._extract_caption_text(video_path):
            return {"type": "caption", "text": caption}

        # Priority 2: Use audio transcript if available and meaningful
        if self._has_audio(video_path):
            transcript = self._extract_audio_text(video_path)
            if not self._is_music_or_silence(transcript):
                return {"type": "audio", "text": transcript}

        # Priority 3: Use visual content with VLM
        visual = self._extract_visual_text(video_path)
        return {"type": "visual", "text": visual}

    def _extract_caption_text(self, video_path: str) -> str:
        """
        Extract captions if embedded in the video.
        """
        try:
            clip = VideoFileClip(video_path)
            if clip.captions:
                return clip.captions
        except:
            pass
        return None

    def _has_audio(self, video_path: str) -> bool:
        """
        Check whether the video contains an audio stream.
        """
        try:
            probe = ffmpeg.probe(video_path)
            return any(s["codec_type"] == "audio" for s in probe["streams"])
        except:
            return False

    def _extract_audio_text(self, video_path: str) -> str:
        """
        Convert audio to text using Whisper.
        """
        with tempfile.NamedTemporaryFile(suffix=".mp3", delete=False) as tmp:
            clip = VideoFileClip(video_path)
            clip.audio.write_audiofile(tmp.name, logger=None)
            result = self.whisper.transcribe(tmp.name)
            os.remove(tmp.name)
            return result.get("text", "")

    def _is_music_or_silence(self, transcript: str) -> bool:
        """
        Check if audio transcript is meaningless (e.g., music only).
        """
        return not transcript.strip() or len(transcript.strip()) < 10

    def _extract_visual_text(self, video_path: str) -> str:
        """
        Extract and caption visual content using Gemini VLM.
        Uses one frame per second, skips visually similar frames.
        If video > 2 minutes, captions 4-frame grid layouts.
        """
        frames = self._extract_unique_frames(video_path)
        results = []

        if self._get_video_duration(video_path) > 120:
            # Long video: group 4 frames and caption a combined grid image
            grouped = [frames[i:i + 4] for i in range(0, len(frames), 4)]
            for group in grouped:
                grid_image = self._create_image_grid(group)
                results.append(self._caption_with_vlm(grid_image))
        else:
            # Short video: caption each unique frame individually
            for frame in frames:
                results.append(self._caption_with_vlm(frame))

        return "\n".join(filter(None, results))

    def _extract_unique_frames(self, video_path: str, fps=1) -> list:
        """
        Extract 1 frame per second and remove visually similar frames.
        """
        cap = cv2.VideoCapture(video_path)
        video_fps = cap.get(cv2.CAP_PROP_FPS)
        total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
        step = int(video_fps / fps)

        frames = []
        prev_feature = None

        for idx in range(0, total_frames, step):
            cap.set(cv2.CAP_PROP_POS_FRAMES, idx)
            ret, frame = cap.read()
            if not ret:
                continue

            feature = self._get_feature_vector(frame)
            if prev_feature is None or self._feature_difference(prev_feature, feature) > 0.3:
                frames.append(frame)
                prev_feature = feature

        cap.release()
        return frames

    def _get_feature_vector(self, frame: np.ndarray) -> torch.Tensor:
        """
        Convert frame into feature vector using ResNet18.
        """
        image = Image.fromarray(cv2.cvtColor(frame, cv2.COLOR_BGR2RGB))
        input_tensor = preprocess(image).unsqueeze(0)
        with torch.no_grad():
            feature = resnet(input_tensor).squeeze()
        return F.normalize(feature, dim=0)

    def _feature_difference(self, f1: torch.Tensor, f2: torch.Tensor) -> float:
        """
        Cosine distance between two feature vectors.
        """
        return 1 - F.cosine_similarity(f1.unsqueeze(0), f2.unsqueeze(0)).item()

    def _caption_with_vlm(self, image: np.ndarray or Image.Image) -> str:
        """
        Pass image to Gemini VLM and get a descriptive caption.
        """
        if isinstance(image, np.ndarray):
            image = Image.fromarray(image)
        try:
            response = self.vlm.generate_content([
                "Describe the content of this image in one sentence.",
                image
            ])
            return response.text.strip()
        except Exception as e:
            print("Gemini error:", e)
            return ""

    def _create_image_grid(self, frames: list) -> np.ndarray:
        """
        Combine 4 frames into a 2x2 grid image layout.
        """
        h, w, _ = frames[0].shape
        grid = np.zeros((2 * h, 2 * w, 3), dtype=np.uint8)

        for i, frame in enumerate(frames):
            if i >= 4:
                break
            y = (i // 2) * h
            x = (i % 2) * w
            grid[y:y + h, x:x + w] = frame
        return grid

    def _get_video_duration(self, video_path: str) -> float:
        """
        Return video duration in seconds.
        """
        cap = cv2.VideoCapture(video_path)
        fps = cap.get(cv2.CAP_PROP_FPS)
        total_frames = cap.get(cv2.CAP_PROP_FRAME_COUNT)
        cap.release()
        return total_frames / fps



-----------------------------


from fastapi import FastAPI, UploadFile, File
import os
import tempfile
from video_extraction import VideoTextExtractor

app = FastAPI()
extractor = VideoTextExtractor()

@app.post("/extract/")
async def extract_text(file: UploadFile = File(...)):
    """
    Accepts a video file upload via POST and extracts relevant text
    using captions > audio > visual pipeline.
    """
    try:
        # Save uploaded file temporarily
        temp_dir = tempfile.mkdtemp()
        video_path = os.path.join(temp_dir, file.filename)

        with open(video_path, "wb") as f:
            f.write(await file.read())

        # Run extraction pipeline
        result = extractor.extract(video_path)

        return {
            "status": "success",
            "source": result["type"],
            "text": result["text"]
        }

    except Exception as e:
        return {
            "status": "error",
            "message": str(e)
        }






