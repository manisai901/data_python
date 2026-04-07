import streamlit as st
import requests
import os
import bcrypt
import time
import io
import PyPDF2
from dotenv import load_dotenv
from supabase import create_client
from duckduckgo_search import DDGS

# ================= CONFIG =================
st.set_page_config(
    page_title="Mani's Personal Agent",
    page_icon="🤖",
    layout="wide"
)

# ================= LOAD ENV =================
load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
GROQ_API_KEY = os.getenv("GROQ_API_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

MODELS = {
    "Llama 3.1 8B (Fast)": "llama-3.1-8b-instant",
    "Llama 3.3 70B (Smart)": "llama-3.3-70b-versatile",
    "Mixtral 8x7B (Balanced)": "mixtral-8x7b-32768",
    "Gemma 2 9B": "gemma2-9b-it"
}

SYSTEM_PROMPT = """You are a highly capable personal AI assistant. 
You help with data engineering, GCP, BigQuery, SQL, Python, daily tasks, planning, research, and general queries.
Be concise, practical, and friendly. Format responses clearly using markdown when helpful.
If you search the web, summarize findings clearly. If analyzing a document, be thorough."""

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
        existing = supabase.table("users").select("username").eq("username", username).execute()
        if existing.data:
            return False, "Username already exists"
        hashed = hash_password(password)
        supabase.table("users").insert({
            "username": username,
            "password": hashed
        }).execute()
        return True, "Account created successfully"
    except Exception as e:
        return False, str(e)

def login(username, password):
    try:
        res = supabase.table("users").select("password").eq("username", username).execute()
        if res.data:
            return verify_password(password, res.data[0]["password"])
        return False
    except Exception as e:
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
        st.error(f"Memory save error: {e}")

def summarize_memory_if_needed(memory, username, session_id, model):
    """Auto-summarize old messages if conversation is too long."""
    if len(memory) > 24:
        old_msgs = memory[:-10]
        recent_msgs = memory[-10:]
        summary_prompt = "Summarize this conversation so far in clear bullet points (max 8 bullets). Be brief."
        summary = call_groq(summary_prompt, old_msgs, model, use_system=False)
        compressed = [{
            "role": "assistant",
            "content": f"[Earlier conversation summary]\n{summary}"
        }]
        return compressed + recent_msgs
    return memory

# ================= SESSIONS =================
def get_sessions(username):
    try:
        res = supabase.table("memory") \
            .select("session_id") \
            .eq("username", username) \
            .execute()
        return sorted(list(set([r["session_id"] for r in res.data])))
    except:
        return []

def get_session_title(username, session_id):
    """Get first user message as title preview."""
    try:
        res = supabase.table("memory") \
            .select("content") \
            .eq("username", username) \
            .eq("session_id", session_id) \
            .eq("role", "user") \
            .order("id") \
            .limit(1) \
            .execute()
        if res.data:
            title = res.data[0]["content"][:30]
            return title + "..." if len(res.data[0]["content"]) > 30 else title
        return f"Chat {session_id}"
    except:
        return f"Chat {session_id}"

def delete_session(username, session_id):
    try:
        supabase.table("memory") \
            .delete() \
            .eq("username", username) \
            .eq("session_id", session_id) \
            .execute()
        return True
    except:
        return False

def new_session_id(sessions):
    existing_nums = []
    for s in sessions:
        try:
            existing_nums.append(int(s))
        except:
            pass
    return str(max(existing_nums) + 1) if existing_nums else "1"

# ================= WEB SEARCH =================
def web_search(query, max_results=4):
    try:
        with DDGS() as ddg:
            results = ddg.text(query, max_results=max_results)
            if not results:
                return "No results found."
            output = []
            for r in results:
                output.append(f"**{r.get('title', '')}**\n{r.get('body', '')}")
            return "\n\n".join(output)
    except Exception as e:
        return f"Search error: {e}"

def needs_search(text):
    keywords = [
        "search", "latest", "news", "today", "current", "2024", "2025",
        "what happened", "recent", "find online", "look up", "trending"
    ]
    return any(k in text.lower() for k in keywords)

# ================= PDF / FILE =================
def extract_text_from_file(uploaded_file):
    name = uploaded_file.name.lower()
    try:
        if name.endswith(".pdf"):
            reader = PyPDF2.PdfReader(io.BytesIO(uploaded_file.read()))
            text = ""
            for page in reader.pages:
                text += page.extract_text() or ""
            return text[:6000]
        elif name.endswith(".txt") or name.endswith(".md"):
            return uploaded_file.read().decode("utf-8")[:6000]
        elif name.endswith(".csv"):
            content = uploaded_file.read().decode("utf-8")
            lines = content.split("\n")
            return "\n".join(lines[:100])
        else:
            return None
    except Exception as e:
        return f"Error reading file: {e}"

# ================= GROQ =================
def call_groq(prompt, memory, model, use_system=True):
    url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {GROQ_API_KEY}",
        "Content-Type": "application/json"
    }

    messages = []
    if use_system:
        messages.append({"role": "system", "content": SYSTEM_PROMPT})

    messages += [{"role": m["role"], "content": m["content"]} for m in memory]
    messages.append({"role": "user", "content": prompt})

    payload = {
        "model": model,
        "messages": messages[-16:]
    }

    try:
        response = requests.post(url, headers=headers, json=payload, timeout=30)
        response.raise_for_status()
        return response.json()["choices"][0]["message"]["content"]
    except requests.exceptions.Timeout:
        return "⚠️ Request timed out. Please try again."
    except Exception as e:
        return f"⚠️ AI error: {str(e)}"

# ================= SESSION STATE =================
for key, default in {
    "user": None,
    "session_id": "1",
    "file_context": None,
    "file_name": None,
}.items():
    if key not in st.session_state:
        st.session_state[key] = default

# ================= LOGIN / SIGNUP UI =================
if not st.session_state.user:
    col1, col2, col3 = st.columns([1, 1.2, 1])
    with col2:
        st.markdown("## 🤖 Personal AI Agent")
        st.markdown("Your intelligent assistant, always ready.")
        st.divider()

        tab1, tab2 = st.tabs(["🔑 Login", "📝 Signup"])

        with tab1:
            u = st.text_input("Username", key="login_u")
            p = st.text_input("Password", type="password", key="login_p")
            if st.button("Login", use_container_width=True, type="primary"):
                if login(u, p):
                    st.session_state.user = u
                    st.success("✅ Logged in!")
                    time.sleep(0.5)
                    st.rerun()
                else:
                    st.error("❌ Invalid credentials")

        with tab2:
            new_u = st.text_input("Choose Username", key="signup_u")
            new_p = st.text_input("Choose Password", type="password", key="signup_p")
            confirm_p = st.text_input("Confirm Password", type="password", key="signup_cp")
            if st.button("Create Account", use_container_width=True, type="primary"):
                if new_p != confirm_p:
                    st.error("❌ Passwords do not match")
                elif len(new_u) < 3:
                    st.error("❌ Username too short")
                elif len(new_p) < 6:
                    st.error("❌ Password must be 6+ characters")
                else:
                    ok, msg = signup(new_u, new_p)
                    if ok:
                        st.success(f"✅ {msg}! Please login.")
                    else:
                        st.error(f"❌ {msg}")

# ================= MAIN APP =================
else:
    sessions = get_sessions(st.session_state.user)

    # ===== SIDEBAR =====
    with st.sidebar:
        st.markdown(f"### 👤 {st.session_state.user}")
        st.divider()

        # Model selector
        selected_model_name = st.selectbox("🧠 Model", list(MODELS.keys()), index=0)
        selected_model = MODELS[selected_model_name]
        st.divider()

        # New Chat
        if st.button("➕ New Chat", use_container_width=True):
            st.session_state.session_id = new_session_id(sessions)
            st.session_state.file_context = None
            st.session_state.file_name = None
            st.rerun()

        # Chat list
        st.markdown("**💬 Chats**")
        for s in reversed(sessions):
            title = get_session_title(st.session_state.user, s)
            is_active = s == st.session_state.session_id
            label = f"{'▶ ' if is_active else ''}{title}"
            if st.button(label, key=f"chat_{s}", use_container_width=True):
                st.session_state.session_id = s
                st.session_state.file_context = None
                st.session_state.file_name = None
                st.rerun()

        st.divider()

        # File uploader
        st.markdown("**📎 Upload File**")
        uploaded_file = st.file_uploader(
            "PDF, TXT, MD, CSV",
            type=["pdf", "txt", "md", "csv"],
            label_visibility="collapsed"
        )
        if uploaded_file:
            file_text = extract_text_from_file(uploaded_file)
            if file_text:
                st.session_state.file_context = file_text
                st.session_state.file_name = uploaded_file.name
                st.success(f"✅ {uploaded_file.name} loaded")
            else:
                st.error("Could not read file")

        if st.session_state.file_context:
            st.info(f"📄 Active: {st.session_state.file_name}")
            if st.button("✖ Remove file", use_container_width=True):
                st.session_state.file_context = None
                st.session_state.file_name = None
                st.rerun()

        st.divider()

        # Export chat
        memory_for_export = load_memory(st.session_state.user, st.session_state.session_id)
        if memory_for_export:
            chat_text = "\n\n".join([
                f"{'🧑 You' if m['role'] == 'user' else '🤖 Agent'}: {m['content']}"
                for m in memory_for_export
            ])
            st.download_button(
                "⬇️ Export Chat",
                chat_text,
                file_name=f"chat_{st.session_state.session_id}.txt",
                use_container_width=True
            )

            # Delete chat
            if st.button("🗑️ Delete This Chat", use_container_width=True):
                delete_session(st.session_state.user, st.session_state.session_id)
                remaining = [s for s in sessions if s != st.session_state.session_id]
                st.session_state.session_id = remaining[-1] if remaining else new_session_id([])
                st.session_state.file_context = None
                st.rerun()

        st.divider()

        if st.button("🚪 Logout", use_container_width=True):
            for key in ["user", "session_id", "file_context", "file_name"]:
                st.session_state[key] = None
            st.rerun()

    # ===== HEADER =====
    header_col1, header_col2 = st.columns([3, 1])
    with header_col1:
        chat_title = get_session_title(st.session_state.user, st.session_state.session_id)
        st.markdown(f"### 🤖 {chat_title}")
    with header_col2:
        st.caption(f"Model: {selected_model_name.split('(')[0].strip()}")

    if st.session_state.file_context:
        st.info(f"📄 File context active: **{st.session_state.file_name}** — ask anything about it")

    st.divider()

    # ===== LOAD + DISPLAY CHAT =====
    memory = load_memory(st.session_state.user, st.session_state.session_id)

    if not memory:
        st.markdown(
            "<div style='text-align:center; color: gray; padding: 3rem 0;'>"
            "👋 Start a conversation below. I can chat, search the web, or analyze your files."
            "</div>",
            unsafe_allow_html=True
        )

    for m in memory:
        icon = "🧑" if m["role"] == "user" else "🤖"
        with st.chat_message(m["role"], avatar=icon):
            st.markdown(m["content"])

    # ===== INPUT =====
    placeholder = "Ask anything... (I'll auto-search if needed)"
    if st.session_state.file_context:
        placeholder = f"Ask about {st.session_state.file_name} or anything else..."

    user_input = st.chat_input(placeholder)

    if user_input:
        with st.chat_message("user", avatar="🧑"):
            st.markdown(user_input)
        save_memory("user", user_input, st.session_state.user, st.session_state.session_id)

        # Build enriched prompt
        enriched_prompt = user_input

        # Inject file context
        if st.session_state.file_context:
            enriched_prompt = (
                f"The user has uploaded a file named '{st.session_state.file_name}'.\n"
                f"File content (first 6000 chars):\n{st.session_state.file_context}\n\n"
                f"User question: {user_input}"
            )

        # Auto web search
        elif needs_search(user_input):
            with st.spinner("🔍 Searching the web..."):
                search_results = web_search(user_input)
            enriched_prompt = (
                f"{user_input}\n\n"
                f"Web search results (use these to answer):\n{search_results}"
            )

        # Compress memory if too long
        working_memory = summarize_memory_if_needed(
            memory, st.session_state.user, st.session_state.session_id, selected_model
        )

        # Get AI response with typing effect
        with st.chat_message("assistant", avatar="🤖"):
            msg_box = st.empty()
            full_response = call_groq(enriched_prompt, working_memory, selected_model)

            typed = ""
            for char in full_response:
                typed += char
                msg_box.markdown(typed + "▌")
                time.sleep(0.008)
            msg_box.markdown(full_response)

        save_memory("assistant", full_response, st.session_state.user, st.session_state.session_id)
        st.rerun()
