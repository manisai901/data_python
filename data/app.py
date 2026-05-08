import streamlit as st

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
            to bottom right,
            #0f172a,
            #111827,
            #1e293b
        );
        color: white;
    }

    .main-title {
        text-align: center;
        font-size: 3rem;
        font-weight: 700;
        color: white;
        margin-top: 20px;
    }

    .sub-text {
        text-align: center;
        font-size: 1.2rem;
        color: #cbd5e1;
        margin-bottom: 40px;
    }

    .feature-card {
        background: rgba(255,255,255,0.06);
        padding: 25px;
        border-radius: 20px;
        border: 1px solid rgba(255,255,255,0.1);
        backdrop-filter: blur(10px);
        box-shadow: 0 8px 30px rgba(0,0,0,0.3);
        height: 220px;
    }

    .feature-card h3 {
        color: white;
        font-size: 28px;
    }

    .feature-card p {
        color: #d1d5db;
        font-size: 17px;
        line-height: 1.6;
    }

    .footer {
        text-align: center;
        padding: 30px;
        margin-top: 60px;
        color: white;
    }

    .footer a {
        color: #60a5fa;
        text-decoration: none;
        font-size: 17px;
    }

    .footer a:hover {
        color: #93c5fd;
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
                for lightning speed AI responses.
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
                Ask coding, AI, data engineering,
                SQL, Python, and general questions.
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

# ================= CHAT SECTION =================
st.markdown("---")

user_input = st.chat_input("Ask anything...")

if user_input:

    with st.chat_message("user"):
        st.write(user_input)

    with st.chat_message("assistant"):
        st.write("This is where your Groq AI response will appear.")

# ================= FOOTER =================
st.markdown("---")

st.markdown(
    """
    <div class="footer">

        <h3>
            Built with ❤️ by Manikanta Sai
        </h3>

        <p>

            📧
            <a href="mailto:manikantasaivootla@gmail.com">
                manikantasaivootla@gmail.com
            </a>

            &nbsp;&nbsp;|&nbsp;&nbsp;

            🌐
            <a href="https://github.com/manisai901" target="_blank">
                github.com/manisai901
            </a>

        </p>

    </div>
    """,
    unsafe_allow_html=True
)
