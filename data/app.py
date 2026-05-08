import streamlit as st
import requests
import os
from dotenv import load_dotenv

# ================= LOAD ENV =================
load_dotenv()

GROQ_API_KEY = os.getenv("GROQ_API_KEY")
MODEL = "llama-3.1-8b-instant"

# ================= PAGE CONFIG =================
st.set_page_config(
    page_title="Personal AI Assistant",
    page_icon="🤖",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# ================= PREMIUM CSS & ANIMATIONS =================
st.markdown(
    """
    <style>
    /* Root Colors */
    :root {
        --primary: #667eea;
        --secondary: #764ba2;
        --dark-bg: #020617;
        --darker-bg: #0f172a;
        --card-bg: rgba(30, 41, 59, 0.8);
        --border-color: rgba(148, 163, 184, 0.1);
        --text-primary: #f1f5f9;
        --text-secondary: #cbd5e1;
        --accent: #60a5fa;
    }
    
    .stApp {
        background: linear-gradient(135deg, #020617, #0f172a, #111827);
        color: #f1f5f9;
    }

    .block-container {
        padding-top: 2rem;
        padding-bottom: 2rem;
        max-width: 1100px;
    }

    /* ========== HEADER SECTION ========== */
    .header-container {
        text-align: center;
        padding: 3rem 2rem;
        background: linear-gradient(135deg, rgba(102, 126, 234, 0.1), rgba(118, 75, 162, 0.1));
        border-radius: 25px;
        border: 1px solid rgba(148, 163, 184, 0.1);
        margin-bottom: 3rem;
        backdrop-filter: blur(10px);
    }

    .main-title {
        font-size: 3.5rem;
        font-weight: 900;
        margin-bottom: 1rem;
        background: linear-gradient(135deg, #f1f5f9 0%, #cbd5e1 50%, #60a5fa 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        background-clip: text;
        letter-spacing: -1px;
    }

    .sub-text {
        font-size: 1.3rem;
        color: #cbd5e1;
        margin-bottom: 1.5rem;
        font-weight: 500;
    }

    .badge-container {
        display: flex;
        gap: 1rem;
        justify-content: center;
        flex-wrap: wrap;
        margin-top: 1.5rem;
    }

    .badge {
        background: rgba(96, 165, 250, 0.15);
        border: 1px solid rgba(96, 165, 250, 0.3);
        padding: 0.5rem 1.2rem;
        border-radius: 50px;
        font-size: 0.95rem;
        color: #60a5fa;
        font-weight: 600;
    }

    /* ========== FEATURE CARDS ========== */
    .feature-cards-container {
        margin: 3rem 0;
    }

    .feature-card {
        background: linear-gradient(135deg, rgba(30, 41, 59, 0.9), rgba(15, 23, 42, 0.9));
        padding: 2.5rem;
        border-radius: 20px;
        border: 1px solid rgba(148, 163, 184, 0.1);
        backdrop-filter: blur(10px);
        transition: all 0.4s cubic-bezier(0.34, 1.56, 0.64, 1);
        height: 100%;
        position: relative;
        overflow: hidden;
    }

    .feature-card::before {
        content: '';
        position: absolute;
        top: 0;
        left: 0;
        width: 100%;
        height: 3px;
        background: linear-gradient(90deg, #667eea, #764ba2);
        transform: translateX(-100%);
        transition: transform 0.4s ease;
    }

    .feature-card:hover::before {
        transform: translateX(0);
    }

    .feature-card:hover {
        transform: translateY(-10px);
        border-color: #60a5fa;
        box-shadow: 0 20px 50px rgba(102, 126, 234, 0.2);
    }

    .feature-icon {
        font-size: 3rem;
        margin-bottom: 1.5rem;
        display: inline-block;
    }

    .feature-card h3 {
        color: white;
        font-size: 1.8rem;
        margin-bottom: 1rem;
        font-weight: 700;
    }

    .feature-card p {
        color: #cbd5e1;
        font-size: 1.05rem;
        line-height: 1.6;
        margin: 0;
    }

    /* ========== CHAT INTERFACE ========== */
    .stChatMessage {
        background: linear-gradient(135deg, rgba(30, 41, 59, 0.8), rgba(15, 23, 42, 0.8));
        border: 1px solid rgba(148, 163, 184, 0.1);
        border-radius: 15px;
        padding: 1.5rem;
        margin: 1rem 0;
        backdrop-filter: blur(8px);
    }

    /* ========== INPUT STYLING ========== */
    .stChatInputContainer input {
        background: linear-gradient(135deg, rgba(30, 41, 59, 0.9), rgba(15, 23, 42, 0.9)) !important;
        border: 1px solid rgba(96, 165, 250, 0.3) !important;
        border-radius: 15px !important;
        color: #f1f5f9 !important;
        font-size: 1.05rem !important;
        padding: 1.2rem !important;
    }

    .stChatInputContainer input:focus {
        border: 1px solid rgba(102, 126, 234, 0.6) !important;
        box-shadow: 0 0 20px rgba(102, 126, 234, 0.3) !important;
    }

    /* ========== BUTTON STYLING ========== */
    .stButton > button {
        background: linear-gradient(135deg, #667eea, #764ba2) !important;
        color: white !important;
        border: none !important;
        border-radius: 12px !important;
        padding: 0.75rem 2rem !important;
        font-weight: 600 !important;
        font-size: 1rem !important;
        transition: all 0.3s ease !important;
        box-shadow: 0 4px 15px rgba(102, 126, 234, 0.4) !important;
        width: 100% !important;
    }

    .stButton > button:hover {
        transform: translateY(-2px) !important;
        box-shadow: 0 6px 25px rgba(102, 126, 234, 0.6) !important;
    }

    /* ========== DIVIDER ========== */
    .divider {
        border: none;
        border-top: 1px solid rgba(148, 163, 184, 0.1);
        margin: 2rem 0;
    }

    /* ========== METRICS ========== */
    .stMetric {
        background: linear-gradient(135deg, rgba(30, 41, 59, 0.8), rgba(15, 23, 42, 0.8));
        border: 1px solid rgba(148, 163, 184, 0.1);
        border-radius: 15px;
        padding: 1.5rem;
        backdrop-filter: blur(8px);
    }

    /* ========== FOOTER SECTION ========== */
    .footer-section {
        background: linear-gradient(135deg, rgba(15, 23, 42, 0.8), rgba(30, 41, 59, 0.8));
        border-top: 1px solid rgba(148, 163, 184, 0.1);
        padding: 3rem 2rem;
        margin-top: 3rem;
        border-radius: 20px;
        text-align: center;
        backdrop-filter: blur(10px);
    }

    .footer-section h3 {
        color: white;
        font-size: 1.5rem;
        margin-bottom: 1rem;
        font-weight: 700;
    }

    .footer-text {
        color: #cbd5e1;
        font-size: 1.1rem;
        line-height: 1.8;
        margin-bottom: 1.5rem;
    }

    .contact-links {
        display: flex;
        gap: 2rem;
        justify-content: center;
        flex-wrap: wrap;
    }

    .contact-links a {
        color: #60a5fa;
        text-decoration: none;
        font-weight: 600;
        transition: all 0.3s ease;
        padding: 0.5rem 1rem;
        border-radius: 8px;
    }

    .contact-links a:hover {
        color: #93c5fd;
        background: rgba(96, 165, 250, 0.1);
    }

    /* ========== RESPONSIVE ========== */
    @media (max-width: 768px) {
        .main-title {
            font-size: 2.5rem;
        }
        .sub-text {
            font-size: 1.1rem;
        }
        .feature-card {
            padding: 1.8rem;
        }
        .contact-links {
            gap: 1rem;
        }
    }
    </style>
    """,
    unsafe_allow_html=True
)

# ================= HEADER SECTION =================
st.markdown(
    """
    <div class="header-container">
        <h1 class="main-title">🤖 Personal AI Assistant</h1>
        <p class="sub-text">Lightning-Fast AI Powered by Groq</p>
        <div class="badge-container">
            <div class="badge">⚡ Ultra-Fast Inference</div>
            <div class="badge">🧠 Advanced AI Models</div>
            <div class="badge">🔐 Secure & Private</div>
        </div>
    </div>
    """,
    unsafe_allow_html=True
)

# ================= FEATURE CARDS =================
col1, col2, col3 = st.columns(3)

with col1:
    st.markdown(
        """
        <div class="feature-card">
            <div class="feature-icon">⚡</div>
            <h3>Lightning Fast</h3>
            <p>Responses in milliseconds using Groq's ultra-fast inference engine technology.</p>
        </div>
        """,
        unsafe_allow_html=True
    )

with col2:
    st.markdown(
        """
        <div class="feature-card">
            <div class="feature-icon">🧠</div>
            <h3>Highly Intelligent</h3>
            <p>Ask anything: coding, Python, SQL, data engineering, AI, and more.</p>
        </div>
        """,
        unsafe_allow_html=True
    )

with col3:
    st.markdown(
        """
        <div class="feature-card">
            <div class="feature-icon">🌐</div>
            <h3>Completely Free</h3>
            <p>No login required. No hidden fees. Anyone can use it freely 24/7.</p>
        </div>
        """,
        unsafe_allow_html=True
    )

st.markdown("<hr class='divider'>", unsafe_allow_html=True)

# ================= CHAT INTERFACE =================
st.markdown("<h2 style='color: white; text-align: center; font-size: 2rem; margin: 2rem 0;'>💬 Start a Conversation</h2>", unsafe_allow_html=True)

# Initialize chat memory
if "messages" not in st.session_state:
    st.session_state.messages = []

# Display chat messages
for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.write(msg["content"])

# User input
prompt = st.chat_input("Ask anything... (coding, data engineering, AI, general questions)")

if prompt:
    # Add user message to history
    st.session_state.messages.append({
        "role": "user",
        "content": prompt
    })

    # Display user message
    with st.chat_message("user"):
        st.write(prompt)

    # Call Groq API
    try:
        with st.spinner("🤔 Thinking... This may take a few seconds"):
            url = "https://api.groq.com/openai/v1/chat/completions"

            headers = {
                "Authorization": f"Bearer {GROQ_API_KEY}",
                "Content-Type": "application/json"
            }

            # Use last 10 messages for context
            payload = {
                "model": MODEL,
                "messages": st.session_state.messages[-10:],
                "temperature": 0.7,
                "max_tokens": 1024
            }

            response = requests.post(
                url,
                headers=headers,
                json=payload,
                timeout=30
            )

            if response.status_code == 200:
                result = response.json()["choices"][0]["message"]["content"]
            else:
                result = f"❌ API Error: {response.status_code}"

    except Exception as e:
        result = f"❌ Error: {str(e)}"

    # Add assistant message to history
    st.session_state.messages.append({
        "role": "assistant",
        "content": result
    })

    # Display assistant message
    with st.chat_message("assistant"):
        st.write(result)

st.markdown("<hr class='divider'>", unsafe_allow_html=True)

# ================= CHAT STATS =================
col1, col2, col3 = st.columns(3)

with col1:
    st.metric("💬 Messages", len(st.session_state.messages), "in this session")

with col2:
    st.metric("⚡ Model", "Llama 3.1", "8B Parameters")

with col3:
    st.metric("🚀 Speed", "<100ms", "Avg Response Time")

st.markdown("<hr class='divider'>", unsafe_allow_html=True)

# ================= FOOTER =================
st.markdown(
    """
    <div class="footer-section">
        <h3>Built with ❤️ by Manikanta Sai</h3>
        <div class="footer-text">
            <p>A high-performance AI assistant powered by Groq's ultra-fast inference engine.</p>
            <p>Perfect for developers, data engineers, and anyone who needs quick, intelligent answers.</p>
        </div>
        <div class="contact-links">
            <a href="mailto:manikantasaivootla@gmail.com">📧 Email</a>
            <a href="https://github.com/manisai901" target="_blank">🐙 GitHub</a>
            <a href="#" target="_blank">💼 LinkedIn</a>
        </div>
        <p style='color: #94a3b8; margin-top: 2rem; font-size: 0.95rem;'>
            © 2026 Personal AI Assistant. Built with Streamlit + Groq API.
        </p>
    </div>
    """,
    unsafe_allow_html=True
)
