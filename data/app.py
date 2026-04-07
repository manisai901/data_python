import streamlit as st
import requests
import os
import bcrypt
from dotenv import load_dotenv
from supabase import create_client

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

        res = supabase.table("users").insert({
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
            stored = res.data[0]["password"]
            return verify_password(password, stored)

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
    except Exception as e:
        print("Load memory error:", e)
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
    except Exception as e:
        print("Groq error:", e)
        return "⚠️ AI error"

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

    with st.sidebar:
        if st.button("➕ New Chat"):
            st.session_state.session_id = str(len(st.session_state))
            st.rerun()

        if st.button("Logout"):
            st.session_state.user = None
            st.rerun()

    memory = load_memory(st.session_state.user, st.session_state.session_id)

    for m in memory:
        st.chat_message(m["role"]).write(m["content"])

    user_input = st.chat_input("Ask something...")

    if user_input:
        response = call_groq(user_input, memory)

        save_memory("user", user_input, st.session_state.user, st.session_state.session_id)
        save_memory("assistant", response, st.session_state.user, st.session_state.session_id)

        st.chat_message("user").write(user_input)
        st.chat_message("assistant").write(response)
