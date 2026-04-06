import streamlit as st
import requests
import mysql.connector
import os
from dotenv import load_dotenv
import streamlit_authenticator as stauth

# ================= LOAD ENV =================
load_dotenv()

GROQ_API_KEY = os.getenv("GROQ_API_KEY")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")

MODEL = "llama-3.1-8b-instant"

DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": MYSQL_PASSWORD,
    "database": "ai_agent"
}

# ================= LOGIN =================
# names = ["Manikanta"]
# usernames = ["manikanta"]
# passwords = ["1234"]
# 
# # ✅ Correct hashing for version 0.2.2
# hashed_passwords = stauth.Hasher(passwords).generate()
# 
# authenticator = stauth.Authenticate(
#     names,
#     usernames,
#     hashed_passwords,
#     "ai_app",      # cookie name
#     "abcdef",      # key
#     1              # expiry days
# )
# 
# name, authentication_status, username = authenticator.login("Login", "main")

# import streamlit_authenticator as stauth

# Step 1: create hashed password
passwords = ["1234"]
hashed_passwords = stauth.Hasher(passwords).generate()

# Step 2: create credentials dict (IMPORTANT)
credentials = {
    "usernames": {
        "manikanta": {
            "name": "Manikanta",
            "password": hashed_passwords[0]
        }
    }
}

# Step 3: authenticator
authenticator = stauth.Authenticate(
    credentials,
    "ai_app",   # cookie name
    "abcdef",   # key
    1           # expiry days
)

# Step 4: login
name, authentication_status, username = authenticator.login("Login", "main")

# ================= DB =================
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

    response = requests.post(url, headers=headers, json=payload)

    if response.status_code != 200:
        return "❌ Error: " + response.text

    return response.json()["choices"][0]["message"]["content"]

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