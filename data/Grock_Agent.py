import requests
from datetime import datetime
import json
import os

# ===== CONFIG =====
GROQ_API_KEY = "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"
MODEL = "llama-3.1-8b-instant"

MEMORY_FILE = "memory.json"


# ===== LOAD MEMORY =====
def load_memory():
    if os.path.exists(MEMORY_FILE):
        with open(MEMORY_FILE, "r") as f:
            return json.load(f)
    return []


# ===== SAVE MEMORY =====
def save_memory(memory):
    with open(MEMORY_FILE, "w") as f:
        json.dump(memory, f, indent=2)


# ===== CALL AI =====
def ask_ai(prompt, memory):
    url = "https://api.groq.com/openai/v1/chat/completions"

    headers = {
        "Authorization": f"Bearer {GROQ_API_KEY}",
        "Content-Type": "application/json"
    }

    messages = memory[-5:]  # last 5 messages (context)
    messages.append({"role": "user", "content": prompt})

    payload = {
        "model": MODEL,
        "messages": messages
    }

    response = requests.post(url, headers=headers, json=payload)

    if response.status_code != 200:
        print("Error:", response.text)
        return "Error occurred"

    return response.json()["choices"][0]["message"]["content"]


# ===== MAIN AGENT LOOP =====
def run_agent():
    memory = load_memory()

    print("🤖 Personal AI Agent Started (type 'exit' to stop)\n")

    while True:
        user_input = input("Mani: ")

        if user_input.lower() == "exit":
            break

        ai_response = ask_ai(user_input, memory)

        print("AI:", ai_response)

        # Save conversation
        memory.append({"role": "user", "content": user_input})
        memory.append({"role": "assistant", "content": ai_response})

        save_memory(memory)


if __name__ == "__main__":
    run_agent()
    