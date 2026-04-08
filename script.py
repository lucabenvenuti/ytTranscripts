import os
import requests
import google.genai as genai
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
        cookies_path = os.getenv("YOUTUBE_COOKIES_FILE", "cookies.txt")
        
        # FIX: Call list_transcripts directly from the module/class
        # Ensure your import at the top is exactly: from youtube_transcript_api import YouTubeTranscriptApi
        transcript_list = YouTubeTranscriptApi.list_transcripts(video_id, cookies=cookies_path)
        
        try:
            transcript = transcript_list.find_transcript(['it', 'en'])
        except:
            transcript = transcript_list.find_generated_transcript(['it', 'en'])

        fetched = transcript.fetch()
        return " ".join([snippet['text'] for snippet in fetched])[:15000]

    except Exception as e:
        # This will now catch real issues like expired cookies 
        # instead of the "AttributeError"
        print(f"DEBUG Error Type: {type(e).__name__}")
        print(f"DEBUG Error Message: {str(e)}")
        return None

if __name__ == "__main__":
    api_key = os.getenv("GEMINI_API_KEY")
    client = genai.Client(api_key=api_key)

    rss_items = ""

    for name, cid in CHANNELS.items():
        vid, v_title = get_latest_vid(cid)
        if vid:
            url = f"https://www.youtube.com/watch?v={vid}"
            print(f"Processing {name}...")

            transcript_text = get_transcript(vid)
            label = "TRANSCRIPT" if transcript_text else "TITLE-ONLY"

            try:
                source = f"Transcript: {transcript_text}" if transcript_text else f"Title: {v_title}"

                prompt = (
                    f"Start the summary with the word **{label}**.\n\n"
                    f"Analyze the following video content: '{v_title}'\n"
                    f"Source: {source}\n\n"
                    "Instructions:\n"
                    "1. Provide a detailed summary in 5 key points.\n"
                    "2. If the source material is in Italian, write the summary in Italian. "
                    "If it is in English, write in English.\n"
                    "3. Each point should be clear and informative."
                )

                response = client.models.generate_content(
                    model="gemini-3.1-flash-lite-preview",
                    contents=prompt,
                )
                summary = response.text.strip().replace("\n", "<br>")

            except Exception as e:
                print(f"Gemini Error for {name}: {e}")
                summary = f"**{label}**<br>Could not generate summary."

            rss_items += f"""
            <item>
                <title>{name}: {v_title}</title>
                <link>{url}</link>
                <description><![CDATA[{summary}]]></description>
                <pubDate>{datetime.now().strftime('%a, %d %b %Y %H:%M:%S +0000')}</pubDate>
                <guid isPermaLink="false">{vid}-{int(time.time())}</guid>
            </item>"""

            # Delay to respect 15 RPM rate limit
            time.sleep(4)

    rss_feed = f"""<?xml version="1.0" encoding="UTF-8" ?>
    <rss version="2.0">
    <channel>
        <title>YouTube Intelligence AI</title>
        <link>https://github.com/lucabenvenuti/ytTranscripts</link>
        <description>Daily AI Video Summaries</description>
        {rss_items}
    </channel>
    </rss>"""

    with open("feed.xml", "w", encoding="utf-8") as f:
        f.write(rss_feed)

    print("Success: feed.xml updated.")
