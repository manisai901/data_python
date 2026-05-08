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
st.markdown(
    '''
    <div class="footer">
        Built with ❤️ by <b>Manikanta Sai</b><br><br>

        📧 <a href="mailto:yourmail@gmail.com">manikantasaivootla@gmail.com
</a>
        &nbsp;&nbsp;|&nbsp;&nbsp;
        🌐 <a href="https://github.com/manisai901" target="_blank">
        GitHub Repository
        </a>
    </div>
    ''',
    unsafe_allow_html=True
)
