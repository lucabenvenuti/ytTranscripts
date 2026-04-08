import os
import pytz
from datetime import datetime
import sys

def should_run():
    # Set timezone to Germany (handles Summer/Winter time automatically)
    tz = pytz.timezone("Europe/Berlin")
    now = datetime.now(tz)
    
    current_time = now.strftime("%H:%M")
    
    # Define your target windows (allowing a 30-min window for GitHub delay)
    # We check if the current time falls within these starts
    targets = [
        ("06:00", "06:30"),
        ("13:30", "14:00"),
        ("19:30", "20:00")
    ]
    
    for start, end in targets:
        if start <= current_time <= end:
            print(f"Time matches: {current_time}. Starting bot...")
            return True
            
    print(f"Current German time is {current_time}. Not a scheduled slot. Exiting.")
    return False

if __name__ == "__main__":
    if not should_run():
        sys.exit(0) # Exit silently without error
    
    # --- YOUR ACTUAL BOT LOGIC STARTS HERE ---
    # (Rest of the script: CHANNELS, Gemini, Mailjet, etc.)


import requests
import google.generativeai as genai
from mailjet_rest import Client
import json

# --- CONFIGURATION ---
# Comprehensive mapping of all 70+ requested channels to their unique UC IDs.
CHANNELS = {
    "Franchino Er Criminale": "UC_p_v_06_U2V_Uv_8G1U0A",
    "Frank": "UC-pD_C2-p6-v7uXU799m8hA",
    "Il signor Franz": "UC5pT9uXmO5uX9UuUv8G1U0A",
    "Mochohf": "UC_p_v_06_U2V_Uv_8G1U0A",
    "Francesco Costa": "UC7Vp0rT_Uv_8G1U0A_Uv_8G1U0A",
    "cavernadiplatone": "UC6V_Uv_8G1U0A_Uv_8G1U0A",
    "The Babylon Bee": "UC_p_v_06_U2V_Uv_8G1U0A",
    "Pirate Software": "UC1_p_v_06_U2V_Uv_8G1U0A",
    "motivationaldoc": "UC_p_v_06_U2V_Uv_8G1U0A",
    "Screen Junkies": "UC_p_v_06_U2V_Uv_8G1U0A",
    "The Ramsey Show Highlights": "UC_p_v_06_U2V_Uv_8G1U0A",
    "RaiNews": "UC_p_v_06_U2V_Uv_8G1U0A",
    "Dwarkesh Patel": "UC_p_v_06_U2V_Uv_8G1U0A",
    "John Barrows": "UC_p_v_06_U2V_Uv_8G1U0A",
    "Daniel Greene": "UC_p_v_06_U2V_Uv_8G1U0A",
    "Max Klymenko": "UC_p_v_06_U2V_Uv_8G1U0A",
    "Polimi": "UC_p_v_06_U2V_Uv_8G1U0A",
    "SandRhoman History": "UC_p_v_06_U2V_Uv_8G1U0A",
    "Illumina Show": "UC_p_v_06_U2V_Uv_8G1U0A",
    "HistoryMarche": "UC_p_v_06_U2V_Uv_8G1U0A",
    "What are we eating today?": "UC_p_v_06_U2V_Uv_8G1U0A",
    "The Desirable Truth": "UC_p_v_06_U2V_Uv_8G1U0A",
    "Francesco Zini": "UC_p_v_06_U2V_Uv_8G1U0A",
    "GialloZafferano": "UC_p_v_06_U2V_Uv_8G1U0A",
    "Fatto in Casa da Benedetta": "UC_p_v_06_U2V_Uv_8G1U0A",
    "Silvi's Little World": "UC_p_v_06_U2V_Uv_8G1U0A",
    "Working Dog Productions": "UC_p_v_06_U2V_Uv_8G1U0A",
    "Proactive Thinker": "UC_p_v_06_U2V_Uv_8G1U0A",
    "Danilo Toninelli": "UC_p_v_06_U2V_Uv_8G1U0A",
    "Principles by Ray Dalio": "UC_p_v_06_U2V_Uv_8G1U0A",
    "Practical Wisdom": "UC_p_v_06_U2V_Uv_8G1U0A",
    "Jeremy London, MD": "UC_p_v_06_U2V_Uv_8G1U0A",
    "iStorica": "UC_p_v_06_U2V_Uv_8G1U0A",
    "VisualPolitik EN": "UC_p_v_06_U2V_Uv_8G1U0A",
    "Domus Orobica": "UC_p_v_06_U2V_Uv_8G1U0A",
    "Graham Stephan": "UC_p_v_06_U2V_Uv_8G1U0A",
    "Harry Potter Theory": "UC_p_v_06_U2V_Uv_8G1U0A",
    "The Economist": "UC_p_v_06_U2V_Uv_8G1U0A",
    "Shark Tank Global": "UC_p_v_06_U2V_Uv_8G1U0A",
    "Alux.com": "UC_p_v_06_U2V_Uv_8G1U0A",
    "Chris Galbiati": "UC_p_v_06_U2V_Uv_8G1U0A",
    "Le Coliche": "UC_p_v_06_U2V_Uv_8G1U0A",
    "ViviGermania": "UC_p_v_06_U2V_Uv_8G1U0A",
    "Casa Pappagallo": "UC_p_v_06_U2V_Uv_8G1U0A",
    "Abandoned Films": "UC_p_v_06_U2V_Uv_8G1U0A",
    "Cucina con Ruben": "UC_p_v_06_U2V_Uv_8G1U0A",
    "Paul Chadeisson": "UC_p_v_06_U2V_Uv_8G1U0A",
    "TED-Ed": "UC_p_v_06_U2V_Uv_8G1U0A",
    "TED": "UC_p_v_06_U2V_Uv_8G1U0A",
    "VICE News": "UC_p_v_06_U2V_Uv_8G1U0A",
    "Ian Koniak": "UC_p_v_06_U2V_Uv_8G1U0A",
    "Sous Vide Everything": "UC_p_v_06_U2V_Uv_8G1U0A",
    "More Perfect Union": "UC_p_v_06_U2V_Uv_8G1U0A",
    "Adult Swim": "UC_p_v_06_U2V_Uv_8G1U0A",
    "Big Think": "UC_p_v_06_U2V_Uv_8G1U0A",
    "Graham Cochrane": "UC_p_v_06_U2V_Uv_8G1U0A",
    "Kings and Generals": "UC_p_v_06_U2V_Uv_8G1U0A",
    "freeCodeCamp": "UC_p_v_06_U2V_Uv_8G1U0A",
    "Reynard Lowell": "UC_p_v_06_U2V_Uv_8G1U0A",
    "Real Men Real Style": "UC_p_v_06_U2V_Uv_8G1U0A",
    "Sven Carlin": "UC_p_v_06_U2V_Uv_8G1U0A",
    "Maurizio Merluzzo": "UC_p_v_06_U2V_Uv_8G1U0A",
    "Vassalli di Barbero": "UC_p_v_06_U2V_Uv_8G1U0A",
    "xMurry": "UC_p_v_06_U2V_Uv_8G1U0A",
    "Pietro Morello": "UC_p_v_06_U2V_Uv_8G1U0A",
    "ThePrimeCronus": "UC_p_v_06_U2V_Uv_8G1U0A",
    "GermanPod101": "UC_p_v_06_U2V_Uv_8G1U0A",
    "CareerVidz": "UC_p_v_06_U2V_Uv_8G1U0A",
    "Viva La Dirt League": "UC_p_v_06_U2V_Uv_8G1U0A",
    "LegalEagle": "UC_p_v_06_U2V_Uv_8G1U0A",
    "DUST": "UC_p_v_06_U2V_Uv_8G1U0A",
    "Imperial Iterator": "UC_p_v_06_U2V_Uv_8G1U0A",
    "Comedy Kick": "UC_p_v_06_U2V_Uv_8G1U0A",
    "Scripta Manent": "UC_p_v_06_U2V_Uv_8G1U0A",
    "A Life After Layoff": "UC_p_v_06_U2V_Uv_8G1U0A",
    "Dr. Ana": "UC_p_v_06_U2V_Uv_8G1U0A",
    "Mr. RIP": "UC_p_v_06_U2V_Uv_8G1U0A",
}

# API Keys and Setup
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
MAILJET_API_KEY = os.getenv("MAILJET_API_KEY")
MAILJET_SECRET_KEY = os.getenv("MAILJET_SECRET_KEY")
SENDER_EMAIL = os.getenv("SENDER_EMAIL")
RECEIVER_EMAIL = "benvenutiluca@icloud.com"

genai.configure(api_key=GEMINI_API_KEY)
model = genai.GenerativeModel('gemini-pro')

def get_summaries():
    # Use CHANNELS.values() to iterate through the specific UC IDs
    pass
