import os
import requests
from google import genai
import re
import time
from datetime import datetime
from youtube_transcript_api import YouTubeTranscriptApi

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
    "Wizards and Warriors": "UCwqY9GjXBdSYeUZiinbFXyQ"
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
        # Tenta di recuperare i sottotitoli in italiano o inglese
        srt = YouTubeTranscriptApi.get_transcript(video_id, languages=['it', 'en'])
        return " ".join([i['text'] for i in srt])[:15000]
    except Exception:
        return None

if __name__ == "__main__":
    client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))
    rss_items = ""
    MODEL_NAME = "gemini-3.1-flash-lite-preview"
    
    for name, cid in CHANNELS.items():
        vid, v_title = get_latest_vid(cid)
        if vid:
            url = f"https://www.youtube.com/watch?v={vid}"
            print(f"Processing {name}...")
            
            transcript_text = get_transcript(vid)
            # Etichetta basata sulla disponibilità del transcript
            label = "TRANSCRIPT" if transcript_text else "TITLE-ONLY"
            
            try:
                if transcript_text:
                    source_material = f"TRANSCRIPT: {transcript_text}"
                else:
                    source_material = f"TITLE: {v_title}"

                prompt = (
                    f"Analyze the following content from a YouTube video titled '{v_title}'.\n"
                    f"Source: {source_material}\n\n"
                    "Instructions:\n"
                    f"1. START: Begin the summary with the exact word '{label}' in bold.\n"
                    "2. LANGUAGE: If the content is in Italian, summarize in Italian. If English, summarize in English. Otherwise, use English.\n"
                    "3. STRUCTURE: 5 detailed key points.\n"
                    "4. DEPTH: 2-3 sentences per point explaining the 'what' and 'why'.\n"
                    "5. TONE: Informative and engaging."
                )
                
                response = client.models.generate_content(model=MODEL_NAME, contents=prompt)
                summary = response.text.strip().replace("\n", "<br>")
            except Exception as e:
                print(f"Error for {name}: {e}")
                summary = f"**{label}**<br>Summary skipped due to error."
            
            rss_items += f"""
            <item>
                <title>{name}: {v_title}</title>
                <link>{url}</link>
                <description><![CDATA[{summary}]]></description>
                <pubDate>{datetime.now().strftime('%a, %d %b %Y %H:%M:%S +0000')}</pubDate>
                <guid isPermaLink="false">{vid}-{int(time.time())}</guid>
            </item>"""
            
            time.sleep(5)

    rss_feed = f"""<?xml version="1.0" encoding="UTF-8" ?>
    <rss version="2.0">
    <channel>
        <title>YouTube Intelligence</title>
        <link>https://github.com/lucabenvenuti/ytTranscripts</link>
        <description>AI Summaries with Transcripts</description>
        {rss_items}
    </channel>
    </rss>"""

    with open("feed.xml", "w", encoding="utf-8") as f:
        f.write(rss_feed)
    print("Success: Feed updated with labels.")
