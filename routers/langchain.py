# routers/langchain.py
from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse
from ai.langchain import get_response

router = APIRouter(
    prefix="/api/langchain",
    tags=["langchain"],
)

# Chat
@router.post("/chat")
async def sse_request(request: Request):
    data = await request.json()
    messages = data.get('messages')
    defaultInput = data.get('defaultInput')
    
    
    response = StreamingResponse(get_response(messages, defaultInput, True), media_type='text/event-stream')
    return response
