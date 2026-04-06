import requests
import json


def generate_free_image(prompt):
    # This creates a direct URL to a generated image
    formatted_prompt = prompt.replace(" ", "%20")
    url = f"https://image.pollinations.ai/prompt/{formatted_prompt}"
    print(f"\n✅ Free Image generated!")
    print(f"View here: {url}")


def learn_with_free_ai(topic):
    print(f"\n--- Learning about: {topic} ---")
    url = "https://text.pollinations.ai/"

    # Simple prompt for the AI
    payload = {
        "messages": [
            {"role": "user", "content": f"Explain {topic} in 3 simple bullets for a student."}
        ]
    }

    try:
        response = requests.post(url, json=payload)
        if response.status_code == 200:
            print("AI Explanation:")
            print(response.text)
        else:
            print(f"Error: {response.status_code}")
    except Exception as e:
        print(f"Connection Error: {e}")


if __name__ == "__main__":
    # 1. Test Image (No Key Needed!)
    generate_free_image("A traditional Indian garden with a stone Shiva statue")

    # 2. Test Learning
    learn_with_free_ai("Python Decorators")