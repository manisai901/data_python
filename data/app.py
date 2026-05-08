# Premium Streamlit AI Assistant UI

Replace your current `data/app.py` with this full premium version.

```python
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
    page_title="Manikanta AI Assistant",
    page_icon="🤖",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ================= CUSTOM CSS =================
st.markdown(
    """
    <style>

    .stApp {
        background: linear-gradient(to bottom right, #0f172a, #111827, #1e293b);
        color: white;
    }

    .main-title {
        font-size: 3rem;
        font-weight: 700;
        color: white;
        text-align: center;
        margin-top: 10px;
    }

    .sub-text {
        text-align: center;
        color: #cbd5e1;
        font-size: 1.1rem;
        margin-bottom: 30px;
    }

    .feature-card {
        background: rgba(255,255,255,0.05);
        padding: 20px;
        border-radius: 18px;
        border: 1px solid rgba(255,255,255,0.1);
        margin-bottom: 20px;
        backdrop-filter: blur(10px);
    }

    .footer {
        text-align: center;
        padding: 20px;
        margin-top: 50px;
        color: #94a3b8;
        font-size: 15px;
    }

    .footer a {
        color: #60a5fa;
        text-decoration: none;
    }

    .footer a:hover {
        color: #93c5fd;
    }

    </style>
    """,
    unsafe_allow_html=True
)

# ================= HEADER =================
st.markdown('<div class="main-title">🤖 Personal AI Assistant</div>', unsafe_allow_html=True)
st.markdown(
    '<div class="sub-text">Fast AI Assistant powered by Groq + Streamlit</div>',
    unsafe_allow_html=True
)

# ================= SIDEBAR =================
with st.sidebar:

    st.markdown("## ⚙️ Control Panel")

    if st.button("🧹 Clear Chat"):
        st.session_state.messages = []
        st.rerun()

    st.markdown("---")

    st.markdown("### 🚀 Features")

    st.markdown(
        """
        ✅ Fast AI Responses  
        ✅ Premium UI  
        ✅ Public AI Chat  
        ✅ Groq API  
        ✅ Streamlit Hosted  
        """
    )

    st.markdown("---")

    st.markdown("### 👨‍💻 Developer")
    st.markdown("**Manikanta Sai**")

    st.markdown(
        """
        📧 Email:
        <a href="mailto:yourmail@gmail.com">yourmail@gmail.com</a>
        """,
        unsafe_allow_html=True
    )

    st.markdown(
        """
        🌐 GitHub:
        <a href="https://github.com/manisai901" target="_blank">
        github.com/manisai901
        </a>
        """,
        unsafe_allow_html=True
    )

# ================= SESSION STATE =================
if "messages" not in st.session_state:
    st.session_state.messages = []

# ================= DISPLAY CHAT =================
for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.write(msg["content"])

# ================= CHAT INPUT =================
prompt = st.chat_input("Ask anything...")

if prompt:

    # Save user message
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

# ================= FEATURE SECTION =================
st.markdown("---")

col1, col2, col3 = st.columns(3)

with col1:
    st.markdown(
        '''
        <div class="feature-card">
        <h3>⚡ Fast</h3>
        <p>Powered by Groq ultra-fast inference engine.</p>
        </div>
        ''',
        unsafe_allow_html=True
    )

with col2:
    st.markdown(
        '''
        <div class="feature-card">
        <h3>🧠 Smart</h3>
        <p>Ask coding, AI, data engineering, and general questions.</p>
        </div>
        ''',
        unsafe_allow_html=True
    )

with col3:
    st.markdown(
        '''
        <div class="feature-card">
        <h3>🌐 Public</h3>
        <p>No login required. Anyone can use the assistant.</p>
        </div>
        ''',
        unsafe_allow_html=True
    )

# ================= FOOTER =================
st.markdown(
    '''
    <div class="footer">
        Built with ❤️ by <b>Manikanta Sai</b><br><br>  

        📧 <a href="mailto:yourmail@gmail.com">manikantasaivootla@gmail.com</a>
        &nbsp;&nbsp;|&nbsp;&nbsp;
        🌐 <a href="https://github.com/manisai901" target="_blank">
        GitHub Repository
        </a>
    </div>
    ''',
    unsafe_allow_html=True
)
```

---

# requirements.txt

```text
streamlit
requests
python-dotenv
```

---

# Render Start Command

```bash
streamlit run data/app.py --server.port=10000 --server.address=0.0.0.0
```

---

# Render Environment Variable

| KEY          | VALUE             |
| ------------ | ----------------- |
| GROQ_API_KEY | your_groq_api_key |

---

# Final Steps

1. Replace your current `data/app.py`
2. Update your email address
3. Update your GitHub URL
4. Push to GitHub
5. Render → Manual Deploy → Clear cache & deploy

---

# Result

You will get:

* Premium dark UI
* Public AI assistant
* ChatGPT-like chat
* Sidebar controls
* Footer with email + GitHub
* Mobile-friendly design
* Production-level look
