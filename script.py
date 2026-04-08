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
        r = requests.get(url, timeout=15)
        vids = re.findall(r'<yt:videoId>(.*?)</yt:videoId>', r.text)
        titles = re.findall(r'<title>(.*?)</title>', r.text)
        return (vids[0], titles[1]) if vids else (None, None)
    except:
        return (None, None)

if __name__ == "__main__":
    # Setup New Gemini Client
    client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))
    
    rss_items = ""
    
    for name, cid in CHANNELS.items():
        vid, v_title = get_latest_vid(cid)
        if vid:
            url = f"https://www.youtube.com/watch?v={vid}"
            print(f"Processing: {name}...")
            
            try:
                # Using the new gemini-2.0-flash model
                prompt = (
                    f"Summarize this YouTube video in 3 short bullet points in Italian.\n"
                    f"Channel: {name}\n"
                    f"Title: {v_title}\n"
                    f"Link: {url}"
                )
                
                response = client.models.generate_content(
                    model="gemini-2.0-flash", 
                    contents=prompt
                )
                summary = response.text.strip().replace("\n", "<br>")
                
            except Exception as e:
                print(f"Error for {name}: {e}")
                summary = f"Summary failed. <a href='{url}'>Watch video here</a>"
            
            rss_items += f"""
            <item>
                <title>{name}: {v_title}</title>
                <link>{url}</link>
                <description>{summary}</description>
                <pubDate>{datetime.now().strftime('%a, %d %b %Y %H:%M:%S +0000')}</pubDate>
                <guid isPermaLink="false">{vid}</guid>
            </item>"""
            
            # Pause to respect rate limits
            time.sleep(4)

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
    print("Success: feed.xml updated with Gemini 2.0")
