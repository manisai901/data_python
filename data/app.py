import streamlit as st
import requests
import os
import bcrypt
import time
from dotenv import load_dotenv
from supabase import create_client

# ================= CONFIG =================
st.set_page_config(page_title="AI Chat App", layout="wide")

# ================= LOAD ENV =================
load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
GROQ_API_KEY = os.getenv("GROQ_API_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
MODEL = "llama-3.1-8b-instant"

# ================= AUTH =================
def hash_password(password):
    return bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()

def verify_password(password, hashed):
    try:
        return bcrypt.checkpw(password.encode(), hashed.encode())
    except:
        return False

def signup(username, password):
    try:
        hashed = hash_password(password)
        supabase.table("users").insert({
            "username": username,
            "password": hashed
        }).execute()
        return True
    except Exception as e:
        print("Signup error:", e)
        return False

def login(username, password):
    try:
        res = supabase.table("users") \
            .select("password") \
            .eq("username", username) \
            .execute()

        if res.data:
            return verify_password(password, res.data[0]["password"])
        return False
    except Exception as e:
        print("Login error:", e)
        return False

# ================= MEMORY =================
def load_memory(username, session_id):
    try:
        res = supabase.table("memory") \
            .select("role, content") \
            .eq("username", username) \
            .eq("session_id", session_id) \
            .order("id") \
            .execute()
        return res.data
    except:
        return []

def save_memory(role, content, username, session_id):
    try:
        supabase.table("memory").insert({
            "role": role,
            "content": content,
            "username": username,
            "session_id": session_id
        }).execute()
    except Exception as e:
        print("Save memory error:", e)

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

    try:
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        return response.json()["choices"][0]["message"]["content"]
    except:
        return "⚠️ AI error"

# ================= SESSION =================
if "user" not in st.session_state:
    st.session_state.user = None

if "session_id" not in st.session_state:
    st.session_state.session_id = "1"

# ================= LOGIN UI =================
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
                st.error("User exists")

# ================= MAIN APP =================
else:
    st.title(f"🤖 Welcome {st.session_state.user}")

    # ===== SIDEBAR =====
    with st.sidebar:
        st.title("💬 Chats")

        res = supabase.table("memory") \
            .select("session_id") \
            .eq("username", st.session_state.user) \
            .execute()

        sessions = sorted(list(set([r["session_id"] for r in res.data])))

        for s in sessions:
            if st.button(f"Chat {s}"):
                st.session_state.session_id = s
                st.rerun()

        if st.button("➕ New Chat"):
            st.session_state.session_id = str(len(sessions) + 1)
            st.rerun()

        if st.button("Logout"):
            st.session_state.user = None
            st.rerun()

    # ===== LOAD CHAT =====
    memory = load_memory(st.session_state.user, st.session_state.session_id)

    # ===== DISPLAY CHAT =====
    chat_container = st.container()

    with chat_container:
        for m in memory:
            st.chat_message(m["role"]).write(m["content"])

    # ===== INPUT =====
    user_input = st.chat_input("Ask something...")

    if user_input:
        # Show user message instantly
        st.chat_message("user").write(user_input)
        save_memory("user", user_input, st.session_state.user, st.session_state.session_id)

        # AI typing animation
        msg_box = st.chat_message("assistant").empty()
        full_response = call_groq(user_input, memory)

        typed = ""
        for char in full_response:
            typed += char
            msg_box.write(typed)
            time.sleep(0.01)

        save_memory("assistant", full_response, st.session_state.user, st.session_state.session_id)

        st.rerun()

    # ===== AUTO REFRESH (REAL-TIME FEEL) =====
    if "last_len" not in st.session_state:
        st.session_state.last_len = 0

    if len(memory) != st.session_state.last_len:
        st.session_state.last_len = len(memory)
        st.rerun()
