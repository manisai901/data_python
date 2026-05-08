import streamlit as st
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
st.markdown("---")

st.markdown(
    """
    <style>
    .stApp {
        background: linear-gradient(to bottom right, #0f172a, #111827, #1e293b);
        color: white;
    }
    </style>
    """,
    unsafe_allow_html=True
)

st.markdown(
    f"""
    <div style='text-align: center; padding: 20px;'>

        <h4 style='color:white;'>
            Built with ❤️ by Manikanta Sai
        </h4>

        <p style='font-size:16px;'>

            📧 
            <a href='mailto:manikantasaivootla@gmail.com'
               style='color:#60a5fa; text-decoration:none;'>
               manikantasaivootla@gmail.com
            </a>

            &nbsp;&nbsp;|&nbsp;&nbsp;

            🌐 
            <a href='https://github.com/manisai901'
               target='_blank'
               style='color:#60a5fa; text-decoration:none;'>
               GitHub Profile
            </a>

        </p>

    </div>
    """,
    unsafe_allow_html=True
)
