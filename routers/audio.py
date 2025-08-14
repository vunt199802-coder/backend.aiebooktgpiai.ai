from fastapi import APIRouter, File, UploadFile, Response, Form
from pathlib import Path
from openai import OpenAI
import os
from elevenlabs import ElevenLabs
import json
from datetime import datetime
from services.aws_resources import S3_CLIENT

client = OpenAI()
import os
from ai.langchain import get_response_chat

router = APIRouter(
    prefix="/api/audio",
    tags=["audio"],
)

# Chat
@router.post("/transcribe")
async def route(file: UploadFile = File(...), messages: str = Form(...), res: Response = Response()):
    try:
        if "audio" in file.content_type:
            # Save the uploaded file to a local directory
            file_location = file.filename
            with open(file_location, "wb") as buffer:
                buffer.write(await file.read())
            
            with open(file_location, 'rb') as audio_file:
                transcription = client.audio.transcriptions.create(
                    model="whisper-1", 
                    file=audio_file
                )
            os.remove(file_location)
            # Convert messages from JSON string to a list
            messages_list = json.loads(messages)

            # Create a new message object
            new_message = {
                "type": "user",
                "text": transcription.text,
                "timestamp": datetime.now().isoformat()
            }

            # Add the new message to the messages list
            messages_list.append(new_message)

            answer_text = get_response_chat(messages_list, False)

            # Initialize ElevenLabs client
            eleven_labs_client = ElevenLabs(
                api_key=os.getenv("ELEVENLABS_API_KEY")
            )

            # Generate speech using ElevenLabs
            speech_file_path = Path(__file__).parent / "speech.mp3"
            audio_generator = eleven_labs_client.text_to_speech.convert(
                voice_id="FjfxJryh105iTLL4ktHB",
                output_format="mp3_44100_128",
                text=answer_text,
                model_id="eleven_multilingual_v2"
            )

            # Convert generator to bytes and save to file
            audio_data = b"".join(chunk for chunk in audio_generator)
            with open(speech_file_path, "wb") as f:
                f.write(audio_data)

            BUCKET_NAME = 'chatbot-voice-clip'

            # Prepare upload parameters
            current_timestamp = datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
            upload_params = {
                'Bucket': BUCKET_NAME,
                'Key': f"{current_timestamp}_speech.mp3",
                'Body': ''
            }

            # Upload to S3
            try:
                with open(speech_file_path, 'rb') as file_to_upload:
                    upload_params['Body'] = file_to_upload
                    S3_CLIENT.put_object(**upload_params)
                # Delete the file from local after successful upload
                os.remove(speech_file_path)
            except Exception as e:
                print(f"Error uploading file: {e}")
            
            return {"attachment": f"https://chatbot-voice-clip.s3.ap-southeast-2.amazonaws.com/{current_timestamp}_speech.mp3", "text": answer_text}
        else:
            return "This file is not an audio file"

    except Exception as error:
        print(f"Error during transcription: {str(error)}")
        response = {"error": f"Internal Server Error"}
        res.status_code = 500
        return response
