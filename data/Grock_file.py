import os
import requests
from pathlib import Path
# from dotenv import load_dotenv
from datetime import datetime
import time





# ================== CONFIG ==================
GROQ_API_KEY = "g"  # 🔥 CHANGE ONLY THIS
# api_key = os.environ.get("GROQ_API_KEY")
# API_KEY_ENV_VAR = "GROQ_API_KEY"
# GROQ_API_KEY = os.getenv("GROQ_API_KEY")


def load_environment():
    # Load the .env file
    load_dotenv(verbose=True)

MODEL = "llama-3.1-8b-instant"

# Topics to learn (you can modify this list)

# TOPICS = [
#     # "What is Generative AI",
#     "like this what and all you have model and how to use it, explain with examples : llama-3.1-8b-instant",
#     "What is Data Engineering",
#     "how to build a simple chatbot using llama-3.1-8b-instant, explain with code examples",
#     # "Explain ETL vs ELT",
#     # "What is Apache Airflow",
#     # "What is Apache Spark"
# ]

# Base directory (auto current file location)
BASE_DIR = Path(__file__).resolve().parent / "ai_learning_output"


# ================== API CALL ==================
def call_groq_api(topic, retries=1):
    url = "https://api.groq.com/openai/v1/chat/completions"

    headers = {
        "Authorization": f"Bearer {GROQ_API_KEY}",
        "Content-Type": "application/json"
    }

    payload = {
        "model": MODEL,
        "messages": [
            {
                "role": "user",
                "content": f"Explain {topic} clearly and simply for a Data Engineer with examples."
            }
        ]
    }

    for attempt in range(retries):
        try:
            response = requests.post(url, headers=headers, json=payload, timeout=30)

            if response.status_code == 200:
                return response.json()["choices"][0]["message"]["content"]

            print(f"⚠️ Attempt {attempt+1}: {response.status_code}")
            print(response.text)

        except requests.exceptions.Timeout:
            print(f"⏳ Timeout... retrying ({attempt+1})")
        except Exception as e:
            print(f"❌ Error: {e}")

        time.sleep(2)

    return None


# ================== MAIN PIPELINE ==================
def run_daily_learning():
    today_date = datetime.now().strftime("%Y-%m-%d")
    folder_path = BASE_DIR / today_date
    folder_path.mkdir(parents=True, exist_ok=True)

    print(f"\n📅 Running AI Learning for: {today_date}")
    print("=" * 50)

    summary = ""

    for topic in TOPICS:
        print(f"\n📌 Processing: {topic}")

        ai_text = call_groq_api(topic)

        if not ai_text:
            print("❌ Failed, skipping...")
            continue

        # Clean filename
        clean_topic = topic.replace(" ", "_").replace("/", "_")

        # File path
        file_path = folder_path / f"{clean_topic}.txt"

        # Save file
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(f"TOPIC: {topic}\n")
            f.write(f"DATE: {datetime.now()}\n")
            f.write("-" * 50 + "\n\n")
            f.write(ai_text)

        print(f"✅ Saved: {file_path}")

        # Add to summary (first 200 chars)
        summary += f"\n\n=== {topic} ===\n{ai_text[:200]}..."

    # Save summary file
    summary_file = folder_path / "SUMMARY.txt"
    with open(summary_file, "w", encoding="utf-8") as f:
        f.write(summary)

    print("\n📊 Summary saved!")
    print("🎉 ALL DONE SUCCESSFULLY!")

TOPICS = [
   "Hi Team, I am working on Grock files and tomorrow Im tacking leave so I want to share some information about Grock files and how to use it  parapharse need to send mail to manager this in simple language and make it more clear and simple to understand for everyone"
]
# ================== RUN ==================
if __name__ == "__main__":
    run_daily_learning()