import streamlit as st
import requests
import mysql.connector
import os
from dotenv import load_dotenv
import streamlit_authenticator as stauth

# ================= LOAD ENV =================
load_dotenv()

GROQ_API_KEY = os.getenv("GROQ_API_KEY")
MODEL = "llama-3.1-8b-instant"

# ================= DB CONNECTION =================
def get_connection():
    return mysql.connector.connect(
        host=os.getenv("MYSQLHOST"),
        user=os.getenv("MYSQLUSER"),
        password=os.getenv("MYSQLPASSWORD"),
        database=os.getenv("MYSQLDATABASE"),
        port=int(os.getenv("MYSQLPORT")),
        ssl_disabled=False,
        connection_timeout=10
    )

# ================= LOGIN =================
passwords = ["1234"]
hashed_passwords = stauth.Hasher(passwords).generate()

credentials = {
    "usernames": {
        "manikanta": {
            "name": "Manikanta",
            "password": hashed_passwords[0]
        }
    }
}

authenticator = stauth.Authenticate(
    credentials,
    "ai_app",
    "abcdef",
    1
)

name, authentication_status, username = authenticator.login("Login", "main")

# ================= MEMORY =================
def load_memory():
    try:
        conn = get_connection()
        cur = conn.cursor(dictionary=True)

        cur.execute("SELECT role, content FROM memory ORDER BY id DESC LIMIT 6")
        rows = cur.fetchall()

        cur.close()
        conn.close()

        return list(reversed(rows))
    except Exception as e:
        st.error(f"DB Error: {e}")
        return []


def save_memory(role, content):
    try:
        conn = get_connection()
        cur = conn.cursor()

        cur.execute(
            "INSERT INTO memory (role, content) VALUES (%s, %s)",
            (role, content)
        )

        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        st.error(f"DB Save Error: {e}")

# ================= GROQ =================
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

    try:
        response = requests.post(url, headers=headers, json=payload)

        if response.status_code != 200:
            return "❌ Error: " + response.text

        return response.json()["choices"][0]["message"]["content"]

    except Exception as e:
        return f"❌ API Error: {e}"

# ================= UI =================
if authentication_status:

    st.set_page_config(page_title="AI Assistant", layout="centered")
    authenticator.logout("Logout", "sidebar")

    st.title(f"🤖 Welcome {name}")

    # Sidebar
    with st.sidebar:
        st.title("⚙️ Options")

        if st.button("🧹 Clear Chat"):
            st.session_state.chat_history = []

        st.write("### Features")
        st.write("✅ AI Chat")
        st.write("✅ MySQL Memory")
        st.write("✅ Login System")

    # Chat state
    if "chat_history" not in st.session_state:
        st.session_state.chat_history = []

    # Input
    user_input = st.text_input("Ask something...")

    if user_input:
        memory = load_memory()
        response = call_groq(user_input, memory)

        st.session_state.chat_history.append(("You", user_input))
        st.session_state.chat_history.append(("AI", response))

        save_memory("user", user_input)
        save_memory("assistant", response)

    # Display chat
    for role, msg in st.session_state.chat_history:
        if role == "You":
            st.markdown(f"🧑 **You:** {msg}")
        else:
            st.markdown(f"🤖 **AI:** {msg}")

elif authentication_status == False:
    st.error("❌ Invalid username/password")

elif authentication_status == None:
    st.warning("⚠️ Please login")
