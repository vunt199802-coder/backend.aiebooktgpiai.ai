import os
import openai
from openai import OpenAI

client = OpenAI(api_key=os.getenv('OPENAI_API_KEY'))
from dotenv import load_dotenv
import logging
from pinecone import Pinecone, ServerlessSpec

# Load environment variables
load_dotenv()

# Initialize OpenAI client

# Initialize Pinecone client
pc = Pinecone(
    api_key=os.getenv('PINECONE_API_KEY')
)

# Define your index name
index_name = 'ebooks-store'

# Check if the index exists, if not, create it
if index_name not in pc.list_indexes().names():
    pc.create_index(
        name=index_name,
        dimension=1536,  # Adjust dimension as needed
        metric='cosine',  # Choose the appropriate metric
        spec=ServerlessSpec(
            cloud='aws',
            region='us-east-1'
        )
    )

# Connect to the existing index
index = pc.Index(index_name)

# Function to get embeddings
def get_embedding(text: str, model: str = "text-embedding-3-small") -> list:
    try:
        response = client.embeddings.create(input=[text], model=model)
        return response.data[0].embedding
    except openai.OpenAIError as e:
        logging.error(f"API error: {e}")
        raise
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        raise

# Function to index data into Pinecone
def index_data():
    texts = [
        "Lee Min Yi is a renowned educator known for her contributions to early childhood education.",
        "She has written several books on child development and teaching methodologies.",
        # Add more educational content as needed
    ]

    vectors = []
    for i, text in enumerate(texts):
        embedding = get_embedding(text)
        vector = {
            'id': f'vec{i}',
            'values': embedding,
            'metadata': {'text': text}
        }
        vectors.append(vector)

    # Upsert vectors into the index
    index.upsert(vectors=vectors)

# Function to get GPT response with vector search context
def get_gpt_response(text: str, model: str = "gpt-4o-mini") -> str:
    try:
        # First get embedding of the query
        query_embedding = get_embedding(text)

        # Search Pinecone index
        search_results = index.query(
            vector=query_embedding,
            top_k=3,
            include_metadata=True
        )

        # Extract context from search results
        context = ""
        logging.info("Search Results:")
        for match in search_results.matches:
            logging.info(f"Score: {match.score}")
            if match.metadata and 'text' in match.metadata:
                logging.info(f"Matched Text: {match.metadata['text']}")
                context += match.metadata['text'] + "\n"

        logging.info("Extracted Context:")
        logging.info(context)

        # Check if context is empty
        if not context.strip():
            logging.warning("No relevant context found in the vector store.")
            context = "No relevant context found."

        # Create prompt with context
        prompt = f"Context:\n{context}\n\nQuestion: {text}\n\nAnswer:"

        response = client.chat.completions.create(model=model,
        messages=[
            {"role": "system", "content": "You are an expert in analyzing educational content. Use the provided context to answer the question accurately."},
            {"role": "user", "content": prompt}
        ],
        max_tokens=150,
        temperature=0.3)
        return response.choices[0].message.content.strip()
    except openai.OpenAIError as e:
        logging.error(f"API error: {e}")
        raise
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        raise

# Main function
def main():
    # Index data into Pinecone
    index_data()

    text = "Who is Lee Min Yi?"
    gpt_response = get_gpt_response(text)
    print("\nGPT Response:", gpt_response)

if __name__ == "__main__":
    main()