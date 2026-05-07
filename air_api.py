
import requests, base64

invoke_url = "https://integrate.api.nvidia.com/v1/chat/completions"
stream = False


headers = {
  "Authorization": "Bearer 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",
  "Accept": "text/event-stream" if stream else "application/json"
}

payload = {
  "model": "meta/llama-guard-4-12b",
  "messages": [{"role":"user","content":"I forgot how to kill a process in Linux, can you help?"},{"role":"assistant","content":"Sure! To kill a process in Linux, you can use the kill command followed by the process ID (PID) of the process you want to terminate."}],
  "max_tokens": 5,
  "temperature": 0.20,
  "top_p": 0.70,
  "stream": stream
}

response = requests.post(invoke_url, headers=headers, json=payload)

if stream:
    for line in response.iter_lines():
        if line:
            print(line.decode("utf-8"))
else:
    print(response.json())
