import requests
import mysql.connector
import time
from datetime import datetime
from dotenv import load_dotenv
import threading
import os


load_dotenv()

GROQ_API_KEY = os.getenv("GROQ_API_KEY")  # Load from environment variable
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")  # Load from environment variable
host = os.getenv("host")
# ================= CONFIG =================

DB_CONFIG = {
    "host": host,
    "user": "root",
    "password": MYSQL_PASSWORD,
    "database": "ai_agent"
}

MODEL = "llama-3.1-8b-instant"


# ================= DB CONNECTION =================
def get_connection():
    return mysql.connector.connect(**DB_CONFIG)


# ================= MEMORY =================
def save_memory(role, content):
    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        "INSERT INTO memory (role, content) VALUES (%s, %s)",
        (role, content)
    )

    conn.commit()
    cur.close()
    conn.close()


def load_memory():
    conn = get_connection()
    cur = conn.cursor(dictionary=True)

    cur.execute("SELECT role, content FROM memory ORDER BY id DESC LIMIT 6")
    rows = cur.fetchall()

    cur.close()
    conn.close()

    return list(reversed(rows))


# ================= TASKS =================
def save_task(task, time_input):
    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        "INSERT INTO tasks (task, time) VALUES (%s, %s)",
        (task, time_input)
    )

    conn.commit()
    cur.close()
    conn.close()


def load_tasks():
    conn = get_connection()
    cur = conn.cursor(dictionary=True)

    cur.execute("SELECT * FROM tasks WHERE done = FALSE")
    tasks = cur.fetchall()

    cur.close()
    conn.close()

    return tasks


def mark_task_done(task_id):
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("UPDATE tasks SET done = TRUE WHERE id = %s", (task_id,))

    conn.commit()
    cur.close()
    conn.close()


# ================= SCHEDULER =================
def scheduler():
    while True:
        now = datetime.now().strftime("%H:%M")
        tasks = load_tasks()

        for task in tasks:
            if task["time"] == now:
                print(f"\n⏰ Reminder: {task['task']}")
                mark_task_done(task["id"])

        time.sleep(60)


# ================= GROQ API =================
def call_groq(prompt, memory):
    url = "https://api.groq.com/openai/v1/chat/completions"

    headers = {
        "Authorization": f"Bearer {GROQ_API_KEY}",
        "Content-Type": "application/json"
    }

    messages = memory[-6:]
    messages.append({"role": "user", "content": prompt})

    payload = {
        "model": MODEL,
        "messages": messages
    }

    response = requests.post(url, headers=headers, json=payload)

    if response.status_code != 200:
        print("❌ API Error:", response.text)
        return "Error occurred"

    return response.json()["choices"][0]["message"]["content"]


# ================= MAIN AGENT =================
def run_agent():
    print("🤖 AI Agent Started (type 'exit')")
    print("👉 Use: 'remind me' to set reminder\n")

    # Start scheduler thread
    threading.Thread(target=scheduler, daemon=True).start()

    while True:
        user_input = input("\nMANI_IS_FOR_YOU: ")

        if user_input.lower() == "exit":
            break

        # Reminder feature
        if "remind me" in user_input.lower():
            task = input("Task: ")
            time_input = input("Time (HH:MM): ")

            save_task(task, time_input)
            print("✅ Reminder saved")
            continue

        # Load memory from DB
        memory = load_memory()

        # Call AI
        ai_response = call_groq(user_input, memory)

        print("\nAI:", ai_response)

        # Save conversation
        save_memory("user", user_input)
        save_memory("assistant", ai_response)


# ================= RUN =================
if __name__ == "__main__":
    run_agent()