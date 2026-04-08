import os
import requests
from google import genai
import re
import time
from datetime import datetime

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

if __name__ == "__main__":
    client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))
    rss_items = ""
    
    # Use the 3.1 Flash-Lite model which has a higher 2026 Free Quota
    MODEL_NAME = "gemini-3.1-flash-lite-preview"
    
    for name, cid in CHANNELS.items():
        vid, v_title = get_latest_vid(cid)
        if vid:
            url = f"https://www.youtube.com/watch?v={vid}"
            print(f"Processing {name}...")
            
            try:
                # Prompt dinamico per la lingua e profondità
                prompt = (
                    f"Analyze in depth the YouTube video titled: '{v_title}'.\n"
                    f"Video URL: {url}\n\n"
                    "Instructions for the summary:\n"
                    "1. LANGUAGE: If the video/title is in Italian, provide the summary in Italian. "
                    "If the video/title is in English, provide the summary in English. "
                    "In all other cases, provide the summary in English.\n"
                    "2. STRUCTURE: Provide a detailed summary structured in 5 key points.\n"
                    "3. DEPTH: Each point must be descriptive (at least 2-3 sentences) explaining "
                    "not just 'what' happens, but also the 'why' or the context behind the information.\n"
                    "4. TONE: Maintain an informative and engaging tone."
                )
                
                response = client.models.generate_content(
                    model=MODEL_NAME, 
                    contents=prompt
                )
                summary = response.text.strip().replace("\n", "<br>")
                summary = summary.replace("  ", "&nbsp;&nbsp;")
            except Exception as e:
                error_msg = str(e)
                if "429" in error_msg:
                    print(f"!!! QUOTA EXCEEDED for {name}. Stopping AI calls for today.")
                    summary = "Daily AI limit reached. Watch video for details."
                else:
                    print(f"Error for {name}: {error_msg}")
                    summary = "AI processing error."
            
            # Change the rss_items block in your script.py to this:
            rss_items += f"""
            <item>
                <title>{name}: {v_title}</title>
                <link>{url}</link>
                <description><![CDATA[{summary}]]></description>
                <pubDate>{datetime.now().strftime('%a, %d %b %Y %H:%M:%S +0000')}</pubDate>
                <guid isPermaLink="false">{vid}-{datetime.now().strftime('%Y%m%d')}</guid>
            </item>"""
            
            time.sleep(4) # Slight pause to stay safe

    rss_feed = f"""<?xml version="1.0" encoding="UTF-8" ?>
    <rss version="2.0">
    <channel>
        <title>YouTube Intelligence</title>
        <link>https://github.com/lucabenvenuti/ytTranscripts</link>
        <description>AI Summaries</description>
        {rss_items}
    </channel>
    </rss>"""

    with open("feed.xml", "w", encoding="utf-8") as f:
        f.write(rss_feed)
    print("Success: Feed updated.")
