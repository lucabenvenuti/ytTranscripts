import os
import requests
import google.generativeai as genai
from mailjet_rest import Client
from datetime import datetime
import pytz
import json
import sys

# --- 1. TIME GUARD (Handles Daylight Savings Automatically) ---
def should_run():
    # Set timezone to Germany
    tz = pytz.timezone("Europe/Berlin")
    now = datetime.now(tz)
    current_time = now.strftime("%H:%M")
    
    # Target windows (30-min buffer for GitHub Action delays)
    targets = [
        ("06:00", "06:30"),
        ("13:30", "14:00"),
        ("19:30", "20:00")
    ]
    
    for start, end in targets:
        if start <= current_time <= end:
            return True, current_time
    return False, current_time

# --- 2. CONFIGURATION (Your 77 Channels) ---
# The script will search for these names directly on YouTube.
CHANNELS = [
    "Franchino Er Criminale", "Frank", "Il signor Franz", "Mochohf", 
    "Francesco Costa - Da Costa a Costa", "cavernadiplatone", "The Babylon Bee", 
    "Pirate Software", "motivationaldoc", "Screen Junkies", "The Ramsey Show Highlights", 
    "RaiNews", "Dwarkesh Patel", "John Barrows", "Daniel Greene", "Max Klymenko", 
    "Polimi", "SandRhoman History", "Illumina Show", "HistoryMarche", 
    "What are we eating today?", "The Desirable Truth", "Francesco Zini", 
    "GialloZafferano", "Fatto in Casa da Benedetta", "Silvi's Little World", 
    "Working Dog Productions", "Proactive Thinker", "Danilo Toninelli", 
    "Principles by Ray Dalio", "Practical Wisdom - Interesting Ideas", 
    "Jeremy London, MD", "iStorica", "VisualPolitik EN", "Domus Orobica", 
    "Graham Stephan", "Harry Potter Theory", "The Economist", "Shark Tank Global", 
    "Alux.com", "Chris Galbiati", "Le Coliche", "ViviGermania", "Casa Pappagallo", 
    "Abandoned Films", "Cucina con Ruben", "Paul Chadeisson", "TED-Ed", "TED", 
    "VICE News", "Ian Koniak Sales Coaching", "Sous Vide Everything", 
    "More Perfect Union", "Adult Swim", "Big Think", "Graham Cochrane", 
    "Kings and Generals", "freeCodeCamp.org", "Reynard Lowell", "Real Men Real Style", 
    "Value Investing with Sven Carlin, Ph.D.", "Maurizio Merluzzo", 
    "Vassalli di Barbero (ORIGINALS)", "xMurry", "Pietro Morello", 
    "ThePrimeCronus", "Learn German with GermanPod101.com", "CareerVidz", 
    "Viva La Dirt League", "LegalEagle", "DUST", "Imperial Iterator", 
    "Comedy Kick", "Scripta Manent - Roberto Trizio", "A Life After Layoff", 
    "Psychology with Dr. Ana", "Mr. RIP"
]

# --- 3. MAIN EXECUTION ---
if __name__ == "__main__":
    is_time, german_now = should_run()
    
    if not is_time:
        print(f"Current German time is {german_now}. Not a scheduled slot. Exiting.")
        sys.exit(0)

    print(f"Starting scheduled run at {german_now} Germany time...")

    # Load API Keys
    GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
    MAILJET_API_KEY = os.getenv("MAILJET_API_KEY")
    MAILJET_SECRET_KEY = os.getenv("MAILJET_SECRET_KEY")
    SENDER_EMAIL = os.getenv("SENDER_EMAIL")
    RECEIVER_EMAIL = "benvenutiluca@icloud.com"

    # Setup Clients
    genai.configure(api_key=GEMINI_API_KEY)
    gemini_model = genai.GenerativeModel('gemini-pro')
    mailjet = Client(auth=(MAILJET_API_KEY, MAILJET_SECRET_KEY), version='v3.1')

    # Subject line for your email
    subject = f"YouTube Intelligence Report - {german_now} (Berlin Time)"
    
    # --- LOGIC TO FETCH AND SUMMARIZE ---
    # Here your script will loop through CHANNELS, search for each name,
    # find the latest video, and send the summary.
