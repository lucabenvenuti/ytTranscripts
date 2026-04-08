import os
import requests
import google.generativeai as genai
from mailjet_rest import Client
from datetime import datetime
import pytz
import json
import sys

# --- 1. TIME GUARD ---
def should_run():
    tz = pytz.timezone("Europe/Berlin")
    now = datetime.now(tz)
    current_time = now.strftime("%H:%M")
    targets = [("06:00", "06:40"), ("13:30", "14:10"), ("19:30", "20:10")]
    for start, end in targets:
        if start <= current_time <= end:
            return True, current_time
    return False, current_time

# --- 2. CONFIGURATION ---
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

if __name__ == "__main__":
    is_time, german_now = should_run()
    if not is_time:
        print(f"Skipping: German time is {german_now}")
        sys.exit(0)

    # Validate Environment Variables
    GEMINI_KEY = os.getenv("GEMINI_API_KEY")
    MJ_KEY = os.getenv("MAILJET_API_KEY")
    MJ_SEC = os.getenv("MAILJET_SECRET_KEY")
    
    if not all([GEMINI_KEY, MJ_KEY, MJ_SEC]):
        print("Error: Missing API Keys in GitHub Secrets.")
        sys.exit(1)

    print(f"Starting run for {len(CHANNELS)} channels...")
    # Add your video fetching and Gemini logic here
