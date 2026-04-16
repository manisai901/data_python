# 🤖 Personal AI Agent

A fully-featured personal AI assistant web app built with **Streamlit**, **Groq API**, and **Supabase**.  
Access it from any device — mobile, tablet, or desktop — just by opening a URL.

---

## ✨ Features

| Feature | Description |
|---|---|
| 🔐 Auth | Secure signup & login with bcrypt password hashing |
| 💬 Multi-chat | Create, switch, and delete multiple chat sessions |
| 🧠 Model switcher | Choose from 4 Groq LLM models per conversation |
| 🔍 Auto web search | Detects search-related queries and fetches live results |
| 📄 File Q&A | Upload PDF, TXT, MD, or CSV and ask questions on it |
| 🗜️ Memory compression | Auto-summarises long chats to stay within token limits |
| ⬇️ Export chat | Download any conversation as a `.txt` file |
| 🗑️ Delete chat | Remove any session permanently |
| 💾 Persistent memory | All chats stored in Supabase — never lost on refresh |

---

## 🗂️ Project Structure

```
your-repo/
│
├── app.py               ← Main Streamlit application
├── requirements.txt     ← Python dependencies
├── .env                 ← Local secrets (never commit this)
├── .gitignore           ← Excludes .env from git
└── README.md            ← This file
```

---

## 🗄️ Supabase Setup

You need two tables in your Supabase project.

### 1. `users` table

```sql
create table users (
  id         serial primary key,
  username   text unique not null,
  password   text not null,
  created_at timestamp default now()
);
```

### 2. `memory` table

```sql
create table memory (
  id         serial primary key,
  username   text not null,
  session_id text not null,
  role       text not null,       -- 'user' or 'assistant'
  content    text not null,
  created_at timestamp default now()
);
```

> Go to **Supabase → SQL Editor → New Query**, paste each block above, and click **Run**.

---

## 🔑 Environment Variables

Create a `.env` file in the project root (for local development):

```env
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_KEY=your-supabase-anon-key
GROQ_API_KEY=your-groq-api-key
```

### Where to get each key

| Key | Where to find it |
|---|---|
| `SUPABASE_URL` | Supabase → Project Settings → API → Project URL |
| `SUPABASE_KEY` | Supabase → Project Settings → API → `anon` public key |
| `GROQ_API_KEY` | [console.groq.com](https://console.groq.com) → API Keys → Create key |

> ⚠️ **Never commit `.env` to GitHub.** Add it to `.gitignore`.

---

## 💻 Local Development

### Step 1 — Clone the repo

```bash
git clone https://github.com/your-username/your-repo.git
cd your-repo
```

### Step 2 — Install dependencies

```bash
pip install -r requirements.txt
```

### Step 3 — Add your `.env` file

```bash
# Create .env and paste your keys (see above)
```

### Step 4 — Run the app

```bash
streamlit run app.py
```

Open `http://localhost:8501` in your browser.

---

## 🚀 Deploy to Hugging Face Spaces (Free Hosting)

This is the recommended free deployment — gives you a public URL accessible from any device.

### Step 1 — Push code to GitHub

```bash
git add app.py requirements.txt README.md
git commit -m "initial commit"
git push origin main
```

Make sure `.env` is in `.gitignore` and is **not** pushed.

### Step 2 — Create a new Space

1. Go to [huggingface.co/spaces](https://huggingface.co/spaces)
2. Click **Create new Space**
3. Choose:
   - **SDK:** Streamlit
   - **Visibility:** Public or Private
4. Connect your GitHub repo

### Step 3 — Add secrets

1. Go to your Space → **Settings** → **Variables and secrets**
2. Add these three secrets (click **New secret** for each):

```
SUPABASE_URL   = https://your-project.supabase.co
SUPABASE_KEY   = your-supabase-anon-key
GROQ_API_KEY   = your-groq-api-key
```

### Step 4 — Done!

Your app will build and deploy automatically.  
You get a URL like: `https://your-username-your-space.hf.space`

Open it on your phone, tablet, or any browser. ✅

---

## 🧠 Available Models

| Display Name | Groq Model ID | Best For |
|---|---|---|
| ⚡ Llama 3.1 8B — Fast | `llama-3.1-8b-instant` | Quick answers, daily tasks |
| 🧠 Llama 3.3 70B — Smart | `llama-3.3-70b-versatile` | Complex reasoning, long docs |
| ⚖️ Mixtral 8x7B — Balanced | `mixtral-8x7b-32768` | Balanced speed + quality |
| 💎 Gemma 2 9B — Efficient | `gemma2-9b-it` | Lightweight, efficient |

---

## 🔍 Auto Web Search

The agent automatically detects when a web search is needed based on keywords in your message:

> `search`, `latest`, `news`, `today`, `current`, `trending`, `price of`, `weather`, `what happened`, `recent` …

When triggered, it fetches live results from DuckDuckGo and uses them to answer.  
No API key required for web search — it's completely free.

---

## 📄 File Upload

Supported formats:

| Format | Notes |
|---|---|
| `.pdf` | Extracts text from all pages (up to 6000 chars) |
| `.txt` / `.md` | Reads full content (up to 6000 chars) |
| `.csv` | Reads first 100 rows |

Upload a file in the sidebar, then ask any question about it in the chat.  
Remove it anytime by clicking **✖ Remove file**.

---

## 🗜️ Memory Compression

When a conversation exceeds **24 messages**, the older part is automatically summarised into bullet points. This keeps the context window efficient without losing important history.

---

## 🛡️ Security Notes

- Passwords are hashed with **bcrypt** before storing — never stored in plain text
- Usernames are stored in **lowercase** to prevent duplicate accounts
- API keys are loaded from environment variables — never hardcoded
- `.env` file should always be in `.gitignore`

---

## 🐛 Troubleshooting

| Problem | Fix |
|---|---|
| "Invalid credentials" on login | Make sure you signed up first; usernames are case-insensitive |
| App shows env var error | Check Hugging Face secrets are set correctly (no spaces around `=`) |
| Web search returns no results | DuckDuckGo rate-limits occasionally — try again in a few seconds |
| File upload fails | Make sure file is under 200MB and is PDF/TXT/MD/CSV |
| Slow responses | Switch to **Llama 3.1 8B** model for faster replies |

---

## 📦 Tech Stack

| Layer | Technology |
|---|---|
| Frontend / UI | [Streamlit](https://streamlit.io) |
| LLM API | [Groq](https://groq.com) — Llama, Mixtral, Gemma |
| Database / Memory | [Supabase](https://supabase.com) (PostgreSQL) |
| Web Search | [DuckDuckGo Search](https://pypi.org/project/duckduckgo-search/) |
| PDF Parsing | [PyPDF2](https://pypi.org/project/PyPDF2/) |
| Auth | [bcrypt](https://pypi.org/project/bcrypt/) |
| Hosting | [Hugging Face Spaces](https://huggingface.co/spaces) |

---

## 📄 License

MIT License — free to use, modify, and deploy.
