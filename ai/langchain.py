import os
from pinecone import Pinecone
from langchain_openai import ChatOpenAI
from langchain_openai import OpenAIEmbeddings
from langchain.callbacks import StreamingStdOutCallbackHandler
from langchain_pinecone import PineconeVectorStore
from langchain.chains.combine_documents import create_stuff_documents_chain
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain_core.runnables import RunnablePassthrough
from langchain_core.output_parsers import StrOutputParser
from langchain_core.runnables import RunnableBranch
from langchain_core.messages import AIMessage, HumanMessage
from langchain_community.chat_message_histories import ChatMessageHistory
import random
from typing import List
from contants import SYSTEM_PROMPT
from dotenv import load_dotenv

import os
from langchain_openai import OpenAIEmbeddings
from langchain_pinecone import PineconeVectorStore
from pinecone import Pinecone, ServerlessSpec
from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_community.document_loaders import JSONLoader
import json
from pathlib import Path
from langchain_core.documents import Document
import time

from openai import OpenAI

load_dotenv()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
PINECONE_KEY = os.environ.get("PINECONE_API_KEY")
PINECONE_INDEX = os.environ.get("PINECONE_INDEX")
PINECONE_NAMESPACE = os.environ.get("PINECONE_NAMESPACE")
OPENAI_MODEL_NAME = os.environ.get("GPT_MODEL")

# openai.api_key = OPENAI_API_KEY
client = OpenAI()

def get_response(messages: List[any], defaultInput: str, streaming: bool):
    chat = ChatOpenAI(
        openai_api_key=OPENAI_API_KEY,
        model=OPENAI_MODEL_NAME,
        streaming=streaming,
        callbacks=[StreamingStdOutCallbackHandler()],
    )

    pc = Pinecone(api_key=PINECONE_KEY)
    embeddings = OpenAIEmbeddings(openai_api_key=OPENAI_API_KEY)
    index = pc.describe_index(PINECONE_INDEX)

    DEFAULT_PROMPT = defaultInput

    pinecone_namespace = PINECONE_NAMESPACE

    SYSTEM_TEMPLATE = SYSTEM_PROMPT + "\n" + DEFAULT_PROMPT + "\n Answer the user's questions based on the below context.\n" + """ 
        <context>
        {context}
        </context>
    """

    print("SYSTEM_TEMPLATE", SYSTEM_TEMPLATE)

    vectorstore = PineconeVectorStore(pinecone_api_key=PINECONE_KEY, index_name=PINECONE_INDEX, embedding=embeddings,
                                      namespace=pinecone_namespace)
    retriever = vectorstore.as_retriever()

    question_answering_prompt = ChatPromptTemplate.from_messages(
        [
            (
                "system",
                SYSTEM_TEMPLATE,
            ),
            MessagesPlaceholder(variable_name="messages"),
        ]
    )

    document_chain = create_stuff_documents_chain(chat, question_answering_prompt)

    query_transform_prompt = ChatPromptTemplate.from_messages(
        [
            MessagesPlaceholder(variable_name="messages"),
            (
                "user",
                "Given the above conversation, generate a search query to look up in order to get information relevant "
                "to the conversation. Only respond with the query, nothing else.",
            ),
        ]
    )

    query_transforming_retriever_chain = RunnableBranch(
        (
            lambda x: len(x.get("messages", [])) == 1,
            # If only one message, then we just pass that message's content to retriever
            (lambda x: x["messages"][-1].content) | retriever,
        ),
        # If messages, then we pass inputs to LLM chain to transform the query, then pass to retriever
        query_transform_prompt | chat | StrOutputParser() | retriever,
    ).with_config(run_name="chat_retriever_chain")

    conversational_retrieval_chain = RunnablePassthrough.assign(
        context=query_transforming_retriever_chain,
    ).assign(
        answer=document_chain,
    )

    history = []
    for item in messages:
        if item['type'] == "user":
            history.append(HumanMessage(content=item['text']))
        if item['type'] == "bot":
            history.append(AIMessage(content=item['text']))

    if streaming:
        all_content = ""
        
        stream = conversational_retrieval_chain.stream(
            {
                "messages": history,
            }
        )

        for chunk in stream:
            for key in chunk:
                if key == "answer":
                    all_content += chunk[key]
                    processed_chunk = chunk[key].replace("\n", "\\n").replace("\t", "\\t")
                    yield f'data: {processed_chunk}\n\n'
    
    else:
        stream = conversational_retrieval_chain.invoke(
            {
                "messages": history,
            }
        )

        print("stream", stream)

        return stream
    
def get_response_chat(messages: List[any], streaming: bool, SYSTEM_PROMPT: str=""):

    chat = ChatOpenAI(
        openai_api_key=OPENAI_API_KEY,
        model=OPENAI_MODEL_NAME,
        streaming=streaming,
        callbacks=[StreamingStdOutCallbackHandler()],
    )

    pc = Pinecone(api_key=PINECONE_KEY)
    embeddings = OpenAIEmbeddings(openai_api_key=OPENAI_API_KEY)
    index = pc.describe_index(PINECONE_INDEX)

    pinecone_namespace = PINECONE_NAMESPACE

    SYSTEM_TEMPLATE = "Answer the user's questions based on the below context.\n" + SYSTEM_PROMPT + """ 
        <context>
        {context}
        </context>
    """

    print("SYSTEM_TEMPLATE", SYSTEM_TEMPLATE)

    vectorstore = PineconeVectorStore(pinecone_api_key=PINECONE_KEY, index_name=PINECONE_INDEX, embedding=embeddings,
                                      namespace=pinecone_namespace)
    retriever = vectorstore.as_retriever()

    question_answering_prompt = ChatPromptTemplate.from_messages(
        [
            (
                "system",
                SYSTEM_TEMPLATE,
            ),
            MessagesPlaceholder(variable_name="messages"),
        ]
    )

    document_chain = create_stuff_documents_chain(chat, question_answering_prompt)

    query_transform_prompt = ChatPromptTemplate.from_messages(
        [
            MessagesPlaceholder(variable_name="messages"),
            (
                "user",
                "Given the above conversation, generate a search query to look up in order to get information relevant "
                "to the conversation. Only respond with the query, nothing else.",
            ),
        ]
    )

    print("query_transform_prompt", query_transform_prompt)

    query_transforming_retriever_chain = RunnableBranch(
        (
            lambda x: len(x.get("messages", [])) == 1,
            # If only one message, then we just pass that message's content to retriever
            (lambda x: x["messages"][-1].content) | retriever,
        ),
        # If messages, then we pass inputs to LLM chain to transform the query, then pass to retriever
        query_transform_prompt | chat | StrOutputParser() | retriever,
    ).with_config(run_name="chat_retriever_chain")

    conversational_retrieval_chain = RunnablePassthrough.assign(
        context=query_transforming_retriever_chain,
    ).assign(
        answer=document_chain,
    )

    try:
        history = []
        for item in messages:
            if item['type'] == "user":
                history.append(HumanMessage(content=item['text']))
            if item['type'] == "bot":
                history.append(AIMessage(content=item['text']))


        stream = conversational_retrieval_chain.invoke(
            {
                "messages": history,
            }
        )

        return stream['answer']
    except Exception as error:
        return error

def get_image_description(base64_image):

    response = client.chat.completions.create(
    model="gpt-4o-mini",
    messages=[
        {
        "role": "user",
        "content": [
            {
            "type": "text",
            "text": "What is in this image? If the image is not clear, just say 'null'",
            },
            {
            "type": "image_url",
            "image_url": {
                "url":  f"data:image/jpeg;base64,{base64_image}"
            },
            },
        ],
        }
    ],
    )

    return response.choices[0].message.content

def generate_text(messages: List[any], streaming: bool, SYSTEM_PROMPT: str=""):

    chat = ChatOpenAI(
        openai_api_key=OPENAI_API_KEY,
        model=OPENAI_MODEL_NAME,
        streaming=streaming,
        callbacks=[StreamingStdOutCallbackHandler()],
    )

    pc = Pinecone(api_key=PINECONE_KEY)
    embeddings = OpenAIEmbeddings(openai_api_key=OPENAI_API_KEY)
    index = pc.describe_index(PINECONE_INDEX)

    pinecone_namespace = PINECONE_NAMESPACE

    SYSTEM_TEMPLATE = "provide correct response based on the below context.\n" + SYSTEM_PROMPT + """ 
        <context>
        {context}
        </context>
    """

    vectorstore = PineconeVectorStore(pinecone_api_key=PINECONE_KEY, index_name=PINECONE_INDEX, embedding=embeddings,
                                      namespace=pinecone_namespace)
    retriever = vectorstore.as_retriever()

    question_answering_prompt = ChatPromptTemplate.from_messages(
        [
            (
                "system",
                SYSTEM_TEMPLATE,
            ),
            MessagesPlaceholder(variable_name="messages"),
        ]
    )

    document_chain = create_stuff_documents_chain(chat, question_answering_prompt)

    query_transform_prompt = ChatPromptTemplate.from_messages(
        [
            MessagesPlaceholder(variable_name="messages"),
            (
                "user",
                "Given the above conversation, generate a search query to look up in order to get information relevant "
                "to the conversation. Only respond with the query, nothing else.",
            ),
        ]
    )

    query_transforming_retriever_chain = RunnableBranch(
        (
            lambda x: len(x.get("messages", [])) == 1,
            # If only one message, then we just pass that message's content to retriever
            (lambda x: x["messages"][-1].content) | retriever,
        ),
        # If messages, then we pass inputs to LLM chain to transform the query, then pass to retriever
        query_transform_prompt | chat | StrOutputParser() | retriever,
    ).with_config(run_name="chat_retriever_chain")

    conversational_retrieval_chain = RunnablePassthrough.assign(
        context=query_transforming_retriever_chain,
    ).assign(
        answer=document_chain,
    )

    try:
        history = []
        for item in messages:
            if item['type'] == "user":
                history.append(HumanMessage(content=item['text']))
            if item['type'] == "bot":
                history.append(AIMessage(content=item['text']))


        stream = conversational_retrieval_chain.invoke(
            {
                "messages": history,
            }
        )

        return stream['answer']
    except Exception as error:
        return error


def text_embedding (title, data):

    splitter = RecursiveCharacterTextSplitter(chunk_size=500, chunk_overlap=50, separators=[
        "\n\n",
    ])
    
    pagesArray = []
    pagesArray.append(Document(page_content=data))
    
    docs = splitter.split_documents(pagesArray)

    new_pages=[]
    for doc in docs:
        new_pages.append(Document(page_content=doc.page_content, metadata={"title": title}))

    embeddings_model = OpenAIEmbeddings(openai_api_key=OPENAI_API_KEY)
    
    pc = Pinecone(api_key=PINECONE_KEY)

    index_name = PINECONE_INDEX
    namespace = PINECONE_NAMESPACE

    existing_indexes = [index_info["name"] for index_info in pc.list_indexes()]

    if index_name not in existing_indexes:
        pc.create_index(
            name=index_name,
            dimension=1536,
            metric="cosine",
            spec=ServerlessSpec(cloud="aws", region="us-east-1"),
        )
        while not pc.describe_index(index_name).status["ready"]:
            time.sleep(1)

    index = pc.Index(index_name)

    # The OpenAI embedding model `text-embedding-ada-002 uses 1536 dimensions`
    docsearch = PineconeVectorStore.from_documents(
        new_pages,
        embeddings_model,
        index_name=index_name,
        namespace=namespace,
    )

    return len(new_pages)
