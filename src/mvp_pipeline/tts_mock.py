from typing import List


def build_tts_mock(script: str) -> str:
    text = (script or "").strip()
    if not text:
        return ""
    # Deterministic segmentation for stable tests.
    separators: List[str] = ["。", "！", "？", ".", "!", "?"]
    for sep in separators:
        text = text.replace(sep, sep + "|")
    parts = [p.strip() for p in text.split("|") if p.strip()]
    return "\n".join(parts)
