from openai import OpenAI
client = OpenAI()  # Make sure OPENAI_API_KEY environment variable is set

# Try to list assistants first
assistants = client.beta.assistants.list()
print("Available assistants:")
for assistant in assistants.data:
    print(f"ID: {assistant.id}, Name: {assistant.name}")

# Then try to retrieve your specific assistant
assistant_id = "asst_pu1QuFqZnznUCmTj31J1KZkL"
try:
    assistant = client.beta.assistants.retrieve(assistant_id)
    print(f"\nFound assistant: {assistant.name}")
except Exception as e:
    print(f"\nError retrieving assistant: {e}")