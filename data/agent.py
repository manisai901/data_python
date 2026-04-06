import requests
import mysql.connector

GROQ_API_KEY = "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"
MODEL = "llama-3.1-8b-instant"

DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "MyNewPassword123!",
    "database": "ai_agent"
}

def get_connection():
    return mysql.connector.connect(**DB_CONFIG)


def load_memory():
    conn = get_connection()
    cur = conn.cursor(dictionary=True)

    cur.execute("SELECT role, content FROM memory ORDER BY id DESC LIMIT 6")
    rows = cur.fetchall()

    cur.close()
    conn.close()

    return list(reversed(rows))


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


if "chat_history" not in st.session_state:
    st.session_state.chat_history = []

user_input = st.text_input("Ask something...")

if user_input:
    memory = load_memory()
    response = call_groq(user_input, memory)

    # Save to session
    st.session_state.chat_history.append(("You", user_input))
    st.session_state.chat_history.append(("AI", response))

    save_memory("user", user_input)
    save_memory("assistant", response)

# Display history
for role, msg in st.session_state.chat_history:
    if role == "You":
        st.markdown(f"🧑 **You:** {msg}")
    else:
        st.markdown(f"🤖 **AI:** {msg}")

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
        return "Error: " + response.text

    return response.json()["choices"][0]["message"]["content"]

with st.sidebar:
    st.title("⚙️ Options")

    if st.button("🧹 Clear Chat"):
        st.session_state.chat_history = []

    st.write("### Features")
    st.write("✅ AI Chat")
    st.write("✅ Memory (MySQL)")
    st.write("✅ Code Generator")