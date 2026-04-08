import os
import requests
from google import genai
import re
import time
from datetime import datetime
import youtube_transcript_api 

# --- CONFIGURATION ---
CHANNELS = {
    "Franchino Er Criminale": "UCi0pS-WsnV_m0tC99EqInEw",
    "Mr. RIP": "UCXpV8WIs0fAnu0TeHIhEq_Q",
    "SandRhoman History": "UC7pr_dQxm2Ns2KlzRSx5FZA",
    "Illumina Show": "UCYhJxmRknd1gLZa1dxjm4Hw",
    "HistoryMarche": "UC8MX9ECowgDMTOnFTE8EUJw",
    "What are we eating today?": "UCuApobilcYeWdlbhdp746RA",
    "Kings and Generals": "UCMmaBzfCCwZ2KqaBJjkj0fw",
    "Chris Galbiati": "UClvlYh79P6GKOzgfgISrBHg",
    "Francesco Zini": "UCiGp4I5ehgrCF8cKlXvvX2w",
    "Frank Vlog": "UC9w_-HRrQwkyWlbI2mTedxQ",
    "Giulia Crossbow": "UCLYbP4QpYiwcnIqm_cgAtJg",
    "Wizards and Warriors": "UCwqY9GjXBdSYeUZiinbFXyQ",
    "Mocho": "UC1Qm-YEYyAAQYXgI_vsIqCw",
    "Dami Lee": "UCJ_2hNMxOzNjviJBiLWHMqg",
    "Economics Explained": "UCZ4AMrDcNrfy3X6nsU8-rPg",
    "How Money Works": "UCkCGANrihzExmu9QiqZpPlQ",
    "Iron Snail": "UC-0kjvCj9Z_pL_yH648Uq7w",
    "Signor Franz": "UCLpGbBGYr9yCGbSwDEFvrMQ",
    "Francesco Costa": "UCWIkgZzXznmBgU9uQsvjZAQ",
    "Lost le Blanc": "UCt_NLJ4McJlCyYM-dSPRo7Q"
}

def get_latest_vid(channel_id):
    try:
        url = f"https://www.youtube.com/feeds/videos.xml?channel_id={channel_id}"
        r = requests.get(url, timeout=10)
        vids = re.findall(r'<yt:videoId>(.*?)</yt:videoId>', r.text)
        titles = re.findall(r'<title>(.*?)</title>', r.text)
        return (vids[0], titles[1]) if vids else (None, None)
    except:
        return (None, None)

def get_transcript(video_id):
    try:
        srt = youtube_transcript_api.YouTubeTranscriptApi.get_transcript(video_id, languages=['it', 'en'])
        return " ".join([i['text'] for i in srt])[:15000]
    except Exception as e:
        print(f"DEBUG: Transcript error for {video_id}: {str(e)[:50]}")
        return None

if __name__ == "__main__":
    api_key = os.getenv("GEMINI_API_KEY")
    client = genai.Client(api_key=api_key)
    
    # IMPOSTIAMO IL MODELLO CHE HA 500 RPD NEL TUO SCREENSHOT
    MODEL_NAME = "gemini-3.1-flash-lite"
    
    rss_items = ""
    
    for name, cid in CHANNELS.items():
        vid, v_title = get_latest_vid(cid)
        if vid:
            url = f"https://www.youtube.com/watch?v={vid}"
            print(f"Processing {name}...")
            
            transcript_text = get_transcript(vid)
            label = "TRANSCRIPT" if transcript_text else "TITLE-ONLY"
            
            try:
                source = f"Content: {transcript_text}" if transcript_text else f"Title: {v_title}"
                prompt = (
                    f"Start with **{label}**. Summarize the video '{v_title}' based on: {source}. "
                    "Provide 5 detailed bullet points. Use Italian if the source is Italian, otherwise English."
                )
                
                response = client.models.generate_content(model=MODEL_NAME, contents=prompt)
                summary = response.text.strip().replace("\n", "<br>")
            except Exception as e:
                print(f"Gemini Error for {name}: {e}")
                summary = f"**{label}**<br>Summary unavailable."

            rss_items += f"""
            <item>
                <title>{name}: {v_title}</title>
                <link>{url}</link>
                <description><![CDATA[{summary}]]></description>
                <pubDate>{datetime.now().strftime('%a, %d %b %Y %H:%M:%S +0000')}</pubDate>
                <guid isPermaLink="false">{vid}-{int(time.time())}</guid>
            </item>"""
            time.sleep(1) # Velocizzato dato che hai 15 RPM

    rss_feed = f"""<?xml version="1.0" encoding="UTF-8" ?>
    <rss version="2.0">
    <channel>
        <title>YouTube Intelligence</title>
        {rss_items}
    </channel>
    </rss>"""

    with open("feed.xml", "w", encoding="utf-8") as f:
        f.write(rss_feed)
