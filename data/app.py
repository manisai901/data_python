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
    layout="wide"
)

# ================= PREMIUM CSS =================
st.markdown(
    """
    <style>

    .stApp {
        background: linear-gradient(
            135deg,
            #020617,
            #0f172a,
            #111827
        );
        color: white;
    }

    .block-container {
        padding-top: 2rem;
        padding-bottom: 2rem;
    }

    .main-title {
        text-align: center;
        font-size: 3rem;
        font-weight: 800;
        color: white;
        margin-top: 10px;
    }

    .sub-text {
        text-align: center;
        font-size: 1.2rem;
        color: #cbd5e1;
        margin-bottom: 40px;
    }

    .feature-card {
        background: rgba(255,255,255,0.05);
        padding: 25px;
        border-radius: 20px;
        border: 1px solid rgba(255,255,255,0.08);
        backdrop-filter: blur(12px);
        box-shadow: 0 10px 30px rgba(0,0,0,0.3);
        height: 220px;
        margin-bottom: 20px;
    }

    .feature-card h3 {
        color: white;
        font-size: 30px;
        margin-bottom: 15px;
    }

    .feature-card p {
        color: #d1d5db;
        font-size: 18px;
        line-height: 1.7;
    }

    .footer {
        text-align: center;
        padding: 40px 20px;
        margin-top: 60px;
        border-top: 1px solid rgba(255,255,255,0.1);
    }

    .footer h3 {
        color: white;
        margin-bottom: 15px;
    }

    .footer-text {
        color: #cbd5e1;
        font-size: 17px;
    }

    .footer a {
        color: #60a5fa;
        text-decoration: none;
        font-weight: 600;
    }

    .footer a:hover {
        color: #93c5fd;
    }

    .stChatMessage {
        border-radius: 18px;
        padding: 12px;
    }

    </style>
    """,
    unsafe_allow_html=True
)

# ================= HEADER =================
st.markdown(
    """
    <div class="main-title">
        🤖 Personal AI Assistant
    </div>
    """,
    unsafe_allow_html=True
)

st.markdown(
    """
    <div class="sub-text">
        Fast AI Assistant powered by Groq + Streamlit
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
            <h3>⚡ Fast</h3>
            <p>
                Powered by Groq ultra-fast inference engine
                for lightning-fast AI responses.
            </p>
        </div>
        """,
        unsafe_allow_html=True
    )

with col2:
    st.markdown(
        """
        <div class="feature-card">
            <h3>🧠 Smart</h3>
            <p>
                Ask coding, AI, SQL, Python,
                Data Engineering, and general questions.
            </p>
        </div>
        """,
        unsafe_allow_html=True
    )

with col3:
    st.markdown(
        """
        <div class="feature-card">
            <h3>🌐 Public</h3>
            <p>
                No login required.
                Anyone can use this AI assistant freely.
            </p>
        </div>
        """,
        unsafe_allow_html=True
    )

# ================= CHAT MEMORY =================
if "messages" not in st.session_state:
    st.session_state.messages = []

# ================= DISPLAY CHAT =================
for msg in st.session_state.messages:

    with st.chat_message(msg["role"]):
        st.write(msg["content"])

# ================= USER INPUT =================
prompt = st.chat_input("Ask anything...")

if prompt:

    # Show user message
    st.session_state.messages.append({
        "role": "user",
        "content": prompt
    })

    with st.chat_message("user"):
        st.write(prompt)

    # ================= API CALL =================
    url = "https://api.groq.com/openai/v1/chat/completions"

    headers = {
        "Authorization": f"Bearer {GROQ_API_KEY}",
        "Content-Type": "application/json"
    }

    payload = {
        "model": MODEL,
        "messages": st.session_state.messages[-10:]
    }

    try:

        response = requests.post(
            url,
            headers=headers,
            json=payload,
            timeout=30
        )

        result = response.json()["choices"][0]["message"]["content"]

    except Exception as e:
        result = f"❌ Error: {e}"

    # Save assistant response
    st.session_state.messages.append({
        "role": "assistant",
        "content": result
    })

    with st.chat_message("assistant"):
        st.write(result)

# ================= FOOTER =================
st.markdown(
    """
    <div class="footer">

        <h3>Built with ❤️ by Manikanta Sai</h3>

        <div class="footer-text">

            📧
            <a href="mailto:manikantasaivootla@gmail.com">
                manikantasaivootla@gmail.com
            </a>

            <br><br>

            🌐
            <a href="https://github.com/manisai901" target="_blank">
                github.com/manisai901
            </a>

        </div>

    </div>
    """,
    unsafe_allow_html=True
)
