import os
from dotenv import load_dotenv
from openai import OpenAI

# Load environment variables from .env file
load_dotenv()


def check_api_key():
    """Validate NVIDIA API key"""
    api_key = os.getenv("NVIDIA_API_KEY")

    if not api_key or api_key == "$NVIDIA_API_KEY":
        print("❌ Error: NVIDIA_API_KEY not set in .env file")
        return False

    print("✓ API Key found")
    return True


def chat_with_nvidia(user_message):
    """Send a message to NVIDIA's Nemotron model"""

    try:
        client = OpenAI(
            base_url="https://integrate.api.nvidia.com/v1",
            api_key=os.getenv("NVIDIA_API_KEY")
        )

        print("\n🔄 Sending request to NVIDIA API...")

        completion = client.chat.completions.create(
            model="nvidia/nemotron-3-nano-omni-30b-a3b-reasoning",
            messages=[{"role": "user", "content": user_message}],
            temperature=0.6,
            top_p=0.95,
            max_tokens=65536,
            extra_body={
                "chat_template_kwargs": {"enable_thinking": True},
                "reasoning_budget": 16384
            },
            stream=True
        )

        print("\n📝 Response:\n")

        for chunk in completion:
            if not chunk.choices:
                continue

            # Print reasoning if available
            reasoning = getattr(chunk.choices[0].delta, "reasoning_content", None)
            if reasoning:
                print(f"[REASONING] {reasoning}", end="", flush=True)

            # Print main response
            if chunk.choices[0].delta.content is not None:
                print(chunk.choices[0].delta.content, end="", flush=True)

        print("\n✓ Request completed successfully")

    except Exception as e:
        print(f"❌ Error: {str(e)}")
        return False

    return True


if __name__ == "__main__":
    # Step 1: Check if API key is configured
    if not check_api_key():
        exit(1)

    # Step 2: Send a test message
    test_message = "What are the key features of NVIDIA's Nemotron model?"
    chat_with_nvidia(test_message)