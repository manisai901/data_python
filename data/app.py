import streamlit as st
import requests
import os
from dotenv import load_dotenv

# ================= LOAD ENV =================
load_dotenv()

GROQ_API_KEY = os.getenv("GROQ_API_KEY")

MODEL = "llama-3.1-8b-instant"

# ================= PAGE =================
st.set_page_config(
    page_title="Personal AI Assistant",
    page_icon="🤖",
    layout="centered"
)

st.title("🤖 Personal AI Assistant")

st.write("Ask anything...")

# ================= CHAT MEMORY =================
if "messages" not in st.session_state:
    st.session_state.messages = []

# ================= DISPLAY OLD CHAT =================
for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.write(msg["content"])

# ================= USER INPUT =================
prompt = st.chat_input("Type your question...")

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
            json=payload
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

# ================= SIDEBAR =================
with st.sidebar:

    st.title("⚙️ Options")

    if st.button("🧹 Clear Chat"):
        st.session_state.messages = []
        st.rerun()

    st.write("---")

    st.write("### 🚀 Features")
    st.write("✅ Groq AI")
    st.write("✅ Fast Responses")
    st.write("✅ Chat Memory")
    st.write("✅ Public AI Assistant")

    st.write("---")
    st.write("Made with ❤️ using Streamlit + Groq")
