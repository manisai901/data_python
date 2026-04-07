import streamlit as st
import requests
import mysql.connector
import os
import bcrypt
from dotenv import load_dotenv

# ================= LOAD ENV =================
load_dotenv()

GROQ_API_KEY = os.getenv("GROQ_API_KEY")
MODEL = "llama-3.1-8b-instant"

# ================= DB =================
def get_connection():
    return mysql.connector.connect(
        host=os.getenv("MYSQLHOST"),
        user=os.getenv("MYSQLUSER"),
        password=os.getenv("MYSQLPASSWORD"),
        database=os.getenv("MYSQLDATABASE"),
        port=int(os.getenv("MYSQLPORT")),
        ssl_disabled=False
    )

# ================= AUTH =================
def hash_password(password):
    return bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()

def verify_password(password, hashed):
    try:
        return bcrypt.checkpw(password.encode(), hashed.encode())
    except:
        return False

def signup(username, password):
    conn = get_connection()
    cur = conn.cursor()

    hashed = hash_password(password)

    try:
        cur.execute(
            "INSERT INTO users (username, password) VALUES (%s, %s)",
            (username, hashed)
        )
        conn.commit()
        return True
    except:
        return False
    finally:
        cur.close()
        conn.close()

def login(username, password):
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("SELECT password FROM users WHERE username=%s", (username,))
    result = cur.fetchone()

    cur.close()
    conn.close()

    if result and verify_password(password, result[0]):
        return True
    return False

# ================= MEMORY =================
def load_memory(username, session_id):
    conn = get_connection()
    cur = conn.cursor(dictionary=True)

    cur.execute(
        "SELECT role, content FROM memory WHERE username=%s AND session_id=%s ORDER BY id ASC",
        (username, session_id)
    )

    data = cur.fetchall()

    cur.close()
    conn.close()

    return data

def save_memory(role, content, username, session_id):
    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        "INSERT INTO memory (role, content, username, session_id) VALUES (%s, %s, %s, %s)",
        (role, content, username, session_id)
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

    messages = memory + [{"role": "user", "content": prompt}]

    payload = {
        "model": MODEL,
        "messages": messages[-10:]
    }

    response = requests.post(url, headers=headers, json=payload)

    return response.json()["choices"][0]["message"]["content"]

# ================= SESSION =================
if "user" not in st.session_state:
    st.session_state.user = None

if "session_id" not in st.session_state:
    st.session_state.session_id = "default"

# ================= AUTH UI =================
if not st.session_state.user:

    st.title("🔐 Login / Signup")

    tab1, tab2 = st.tabs(["Login", "Signup"])

    with tab1:
        u = st.text_input("Username")
        p = st.text_input("Password", type="password")

        if st.button("Login"):
            if login(u, p):
                st.session_state.user = u
                st.success("Login successful")
                st.rerun()
            else:
                st.error("Invalid credentials")

    with tab2:
        new_u = st.text_input("New Username")
        new_p = st.text_input("New Password", type="password")

        if st.button("Signup"):
            if signup(new_u, new_p):
                st.success("Account created")
            else:
                st.error("User already exists")

# ================= MAIN APP =================
else:
    st.title(f"🤖 Welcome {st.session_state.user}")

    # Sidebar
    with st.sidebar:
        st.title("💬 Sessions")

        if st.button("➕ New Chat"):
            st.session_state.session_id = str(len(st.session_state) + 1)
            st.rerun()

        st.write("Current:", st.session_state.session_id)

        if st.button("Logout"):
            st.session_state.user = None
            st.rerun()

    # Load memory
    memory = load_memory(st.session_state.user, st.session_state.session_id)

    # Display chat
    for m in memory:
        st.chat_message(m["role"]).write(m["content"])

    # Input
    user_input = st.chat_input("Ask something...")

    if user_input:
        response = call_groq(user_input, memory)

        save_memory("user", user_input, st.session_state.user, st.session_state.session_id)
        save_memory("assistant", response, st.session_state.user, st.session_state.session_id)

        st.chat_message("user").write(user_input)
        st.chat_message("assistant").write(response)

    # Download chat
    if memory:
        chat_text = "\n".join([f"{m['role']}: {m['content']}" for m in memory])

        st.download_button(
            "📥 Download Chat",
            chat_text,
            file_name="chat.txt"
        )
