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

# ==============================================================
#  CONFIG
# ==============================================================
st.set_page_config(
    page_title="Personal AI Agent",
    page_icon="🤖",
    layout="wide"
)

# ==============================================================
#  ENVIRONMENT VARIABLES
# ==============================================================
load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
GROQ_API_KEY = os.getenv("GROQ_API_KEY")

if not SUPABASE_URL or not SUPABASE_KEY or not GROQ_API_KEY:
    st.error("⚠️ Missing environment variables. Set SUPABASE_URL, SUPABASE_KEY, and GROQ_API_KEY.")
    st.stop()

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# ==============================================================
#  AVAILABLE GROQ MODELS
# ==============================================================
MODELS = {
    "⚡ Llama 3.1 8B  — Fast":       "llama-3.1-8b-instant",
    "🧠 Llama 3.3 70B — Smart":      "llama-3.3-70b-versatile",
    "⚖️  Mixtral 8x7B  — Balanced":  "mixtral-8x7b-32768",
    "💎 Gemma 2 9B    — Efficient":  "gemma2-9b-it",
}

# ==============================================================
#  AGENT SYSTEM PROMPT
# ==============================================================
SYSTEM_PROMPT = """You are a highly capable personal AI assistant.
You help with data engineering, GCP, BigQuery, SQL, Python, daily tasks,
planning, research, and general queries.
Be concise, practical, and friendly.
Format responses clearly using markdown where helpful.
When web search results are provided, summarise them clearly.
When analysing a document, be thorough and reference specific content."""

# ==============================================================
#  AUTH
# ==============================================================
def hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")


def verify_password(plain: str, hashed: str) -> bool:
    try:
        plain_bytes  = plain.encode("utf-8")  if isinstance(plain,  str) else plain
        hashed_bytes = hashed.encode("utf-8") if isinstance(hashed, str) else hashed
        return bcrypt.checkpw(plain_bytes, hashed_bytes)
    except Exception as e:
        st.error(f"Password verification error: {e}")
        return False


def signup(username: str, password: str):
    try:
        username = username.strip().lower()
        existing = supabase.table("users").select("username").eq("username", username).execute()
        if existing.data:
            return False, "Username already exists."
        hashed = hash_password(password)
        supabase.table("users").insert({"username": username, "password": hashed}).execute()
        return True, "Account created successfully!"
    except Exception as e:
        return False, f"Signup error: {e}"


def login(username: str, password: str) -> bool:
    try:
        username = username.strip().lower()
        res = supabase.table("users").select("password").eq("username", username).execute()
        if not res.data:
            st.error("❌ Username not found.")
            return False
        return verify_password(password, res.data[0]["password"])
    except Exception as e:
        st.error(f"Login error: {e}")
        return False

# ==============================================================
#  MEMORY
# ==============================================================
def load_memory(username: str, session_id: str) -> list:
    try:
        res = (
            supabase.table("memory")
            .select("role, content")
            .eq("username", username)
            .eq("session_id", session_id)
            .order("id")
            .execute()
        )
        return res.data or []
    except:
        return []


def save_memory(role: str, content: str, username: str, session_id: str):
    try:
        supabase.table("memory").insert({
            "role":       role,
            "content":    content,
            "username":   username,
            "session_id": session_id,
        }).execute()
    except Exception as e:
        st.warning(f"Could not save message: {e}")


def maybe_compress_memory(memory: list) -> list:
    """Summarise old messages when conversation exceeds 24 turns."""
    if len(memory) <= 24:
        return memory
    old_part    = memory[:-10]
    recent_part = memory[-10:]
    summary = _call_groq_raw(
        "Summarise this conversation in clear bullet points (max 8 bullets). Be brief.",
        old_part,
        "llama-3.1-8b-instant",
        use_system=False,
    )
    return [{"role": "assistant", "content": f"[Earlier conversation summary]\n{summary}"}] + recent_part

# ==============================================================
#  SESSION MANAGEMENT
# ==============================================================
def get_all_sessions(username: str) -> list:
    try:
        res = supabase.table("memory").select("session_id").eq("username", username).execute()
        return sorted(list(set(r["session_id"] for r in res.data)))
    except:
        return []


def get_session_preview(username: str, session_id: str) -> str:
    try:
        res = (
            supabase.table("memory")
            .select("content")
            .eq("username", username)
            .eq("session_id", session_id)
            .eq("role", "user")
            .order("id")
            .limit(1)
            .execute()
        )
        if res.data:
            text = res.data[0]["content"]
            return (text[:28] + "…") if len(text) > 28 else text
        return f"Chat {session_id}"
    except:
        return f"Chat {session_id}"


def new_session_id(sessions: list) -> str:
    nums = []
    for s in sessions:
        try:
            nums.append(int(s))
        except:
            pass
    return str(max(nums) + 1) if nums else "1"


def delete_session(username: str, session_id: str):
    try:
        supabase.table("memory").delete().eq("username", username).eq("session_id", session_id).execute()
    except Exception as e:
        st.warning(f"Could not delete chat: {e}")

# ==============================================================
#  WEB SEARCH
# ==============================================================
_SEARCH_TRIGGERS = [
    "search", "latest", "news", "today", "current", "2024", "2025",
    "what happened", "recent", "find online", "look up", "trending",
    "price of", "stock", "weather",
]

def should_search(text: str) -> bool:
    lower = text.lower()
    return any(k in lower for k in _SEARCH_TRIGGERS)


def web_search(query: str, max_results: int = 4) -> str:
    try:
        with DDGS() as ddg:
            results = ddg.text(query, max_results=max_results)
        if not results:
            return "No web results found."
        return "\n\n".join(f"**{r.get('title','')}**\n{r.get('body','')}" for r in results)
    except Exception as e:
        return f"Web search failed: {e}"

# ==============================================================
#  FILE EXTRACTION
# ==============================================================
def extract_file_text(uploaded_file) -> str | None:
    name = uploaded_file.name.lower()
    try:
        if name.endswith(".pdf"):
            reader = PyPDF2.PdfReader(io.BytesIO(uploaded_file.read()))
            return "".join(page.extract_text() or "" for page in reader.pages)[:6000]
        elif name.endswith((".txt", ".md")):
            return uploaded_file.read().decode("utf-8", errors="ignore")[:6000]
        elif name.endswith(".csv"):
            lines = uploaded_file.read().decode("utf-8", errors="ignore").split("\n")
            return "\n".join(lines[:100])
        return None
    except Exception as e:
        return f"Error reading file: {e}"

# ==============================================================
#  GROQ API
# ==============================================================
def _call_groq_raw(prompt: str, history: list, model: str, use_system: bool = True) -> str:
    headers = {"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"}
    messages = []
    if use_system:
        messages.append({"role": "system", "content": SYSTEM_PROMPT})
    messages += [{"role": m["role"], "content": m["content"]} for m in history]
    messages.append({"role": "user", "content": prompt})
    payload = {"model": model, "messages": messages[-16:]}
    try:
        r = requests.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers=headers, json=payload, timeout=30
        )
        r.raise_for_status()
        return r.json()["choices"][0]["message"]["content"]
    except requests.exceptions.Timeout:
        return "⚠️ Request timed out. Please try again."
    except Exception as e:
        return f"⚠️ AI error: {e}"


def call_groq(prompt: str, history: list, model: str) -> str:
    return _call_groq_raw(prompt, history, model)

# ==============================================================
#  INITIALISE SESSION STATE
# ==============================================================
_DEFAULTS = {"user": None, "session_id": "1", "file_context": None, "file_name": None}
for _k, _v in _DEFAULTS.items():
    if _k not in st.session_state:
        st.session_state[_k] = _v

# ==============================================================
#  LOGIN / SIGNUP PAGE
# ==============================================================
if not st.session_state.user:
    _, center, _ = st.columns([1, 1.2, 1])
    with center:
        st.markdown("## 🤖 Personal AI Agent")
        st.markdown("Your intelligent assistant — available on any device, any time.")
        st.divider()

        tab_login, tab_signup = st.tabs(["🔑 Login", "📝 Sign Up"])

        with tab_login:
            lu = st.text_input("Username", key="li_u")
            lp = st.text_input("Password", type="password", key="li_p")
            if st.button("Login", use_container_width=True, type="primary"):
                if not lu or not lp:
                    st.error("Please enter username and password.")
                elif login(lu, lp):
                    st.session_state.user = lu.strip().lower()
                    st.success("✅ Welcome back!")
                    time.sleep(0.4)
                    st.rerun()
                else:
                    st.error("❌ Invalid credentials.")

        with tab_signup:
            su  = st.text_input("Choose a Username", key="su_u")
            sp  = st.text_input("Choose a Password", type="password", key="su_p")
            sp2 = st.text_input("Confirm Password",  type="password", key="su_p2")
            if st.button("Create Account", use_container_width=True, type="primary"):
                if not su or not sp:
                    st.error("Username and password are required.")
                elif len(su.strip()) < 3:
                    st.error("Username must be at least 3 characters.")
                elif len(sp) < 6:
                    st.error("Password must be at least 6 characters.")
                elif sp != sp2:
                    st.error("Passwords do not match.")
                else:
                    ok, msg = signup(su, sp)
                    st.success(f"✅ {msg} Please log in.") if ok else st.error(f"❌ {msg}")

# ==============================================================
#  MAIN APP  (authenticated users only)
# ==============================================================
else:
    sessions = get_all_sessions(st.session_state.user)

    # ----------------------------------------------------------
    #  SIDEBAR
    # ----------------------------------------------------------
    with st.sidebar:
        st.markdown(f"### 👤 {st.session_state.user}")
        st.divider()

        model_label  = st.selectbox("🧠 Model", list(MODELS.keys()), index=0)
        active_model = MODELS[model_label]
        st.divider()

        if st.button("➕  New Chat", use_container_width=True):
            st.session_state.session_id   = new_session_id(sessions)
            st.session_state.file_context = None
            st.session_state.file_name    = None
            st.rerun()

        st.markdown("**💬 Conversations**")
        for s in reversed(sessions):
            preview   = get_session_preview(st.session_state.user, s)
            is_active = (s == st.session_state.session_id)
            if st.button(
                f"{'▶ ' if is_active else ''}{preview}",
                key=f"sess_{s}",
                use_container_width=True
            ):
                st.session_state.session_id   = s
                st.session_state.file_context = None
                st.session_state.file_name    = None
                st.rerun()

        st.divider()

        # File upload
        st.markdown("**📎 Upload a File**")
        uploaded = st.file_uploader(
            "PDF · TXT · MD · CSV",
            type=["pdf", "txt", "md", "csv"],
            label_visibility="collapsed",
        )
        if uploaded:
            extracted = extract_file_text(uploaded)
            if extracted:
                st.session_state.file_context = extracted
                st.session_state.file_name    = uploaded.name
                st.success(f"✅ {uploaded.name} ready")
            else:
                st.error("Could not read this file.")

        if st.session_state.file_context:
            st.info(f"📄 Active: {st.session_state.file_name}")
            if st.button("✖ Remove file", use_container_width=True):
                st.session_state.file_context = None
                st.session_state.file_name    = None
                st.rerun()

        st.divider()

        # Export & Delete
        current_memory = load_memory(st.session_state.user, st.session_state.session_id)
        if current_memory:
            export_text = "\n\n".join(
                f"{'You' if m['role'] == 'user' else 'Agent'}: {m['content']}"
                for m in current_memory
            )
            st.download_button(
                "⬇️  Export Chat",
                data=export_text,
                file_name=f"chat_{st.session_state.session_id}.txt",
                use_container_width=True,
            )
            if st.button("🗑️  Delete This Chat", use_container_width=True):
                delete_session(st.session_state.user, st.session_state.session_id)
                remaining = [s for s in sessions if s != st.session_state.session_id]
                st.session_state.session_id   = remaining[-1] if remaining else "1"
                st.session_state.file_context = None
                st.rerun()

        st.divider()

        if st.button("🚪  Logout", use_container_width=True):
            for k, v in _DEFAULTS.items():
                st.session_state[k] = v
            st.rerun()

    # ----------------------------------------------------------
    #  CHAT HEADER
    # ----------------------------------------------------------
    col_title, col_model = st.columns([4, 1])
    with col_title:
        st.markdown(f"### 🤖 {get_session_preview(st.session_state.user, st.session_state.session_id)}")
    with col_model:
        st.caption(model_label.split("—")[0].strip())

    if st.session_state.file_context:
        st.info(f"📄 File loaded: **{st.session_state.file_name}** — ask anything about it.")

    st.divider()

    # ----------------------------------------------------------
    #  DISPLAY MESSAGES
    # ----------------------------------------------------------
    memory = load_memory(st.session_state.user, st.session_state.session_id)

    if not memory:
        st.markdown(
            "<div style='text-align:center;color:gray;padding:3rem 0'>"
            "👋 Start a conversation below.<br>"
            "I can chat, search the web, or answer questions about your uploaded files."
            "</div>",
            unsafe_allow_html=True,
        )

    for msg in memory:
        with st.chat_message(msg["role"], avatar="🧑" if msg["role"] == "user" else "🤖"):
            st.markdown(msg["content"])

    # ----------------------------------------------------------
    #  CHAT INPUT
    # ----------------------------------------------------------
    hint = (
        f"Ask about {st.session_state.file_name} or anything else…"
        if st.session_state.file_context
        else "Ask anything… (web search triggers automatically when needed)"
    )

    user_input = st.chat_input(hint)

    if user_input:
        with st.chat_message("user", avatar="🧑"):
            st.markdown(user_input)
        save_memory("user", user_input, st.session_state.user, st.session_state.session_id)

        # Build enriched prompt
        if st.session_state.file_context:
            enriched = (
                f"The user uploaded a file named '{st.session_state.file_name}'.\n"
                f"File content (up to 6000 characters):\n{st.session_state.file_context}\n\n"
                f"User question: {user_input}"
            )
        elif should_search(user_input):
            with st.spinner("🔍 Searching the web…"):
                results = web_search(user_input)
            enriched = (
                f"{user_input}\n\n"
                f"[Web search results — use these to answer accurately]\n{results}"
            )
        else:
            enriched = user_input

        working_memory = maybe_compress_memory(memory)

        # Typing animation
        with st.chat_message("assistant", avatar="🤖"):
            box           = st.empty()
            full_response = call_groq(enriched, working_memory, active_model)
            typed         = ""
            for char in full_response:
                typed += char
                box.markdown(typed + "▌")
                time.sleep(0.008)
            box.markdown(full_response)

        save_memory("assistant", full_response, st.session_state.user, st.session_state.session_id)
        st.rerun()
