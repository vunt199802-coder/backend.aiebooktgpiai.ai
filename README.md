# AI Analytics Backend

## Overview
This project is an AI-powered backend service designed to handle various analytics tasks, including file uploads, audio transcriptions, and chat-based interactions using OpenAI's GPT models.

## Features
- **File Uploads**: Upload files and process them using AI assistants.
- **Audio Transcriptions**: Transcribe audio files using OpenAI's Whisper model.
- **Chat Interactions**: Engage in chat-based interactions with AI, leveraging OpenAI's GPT models.

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/Ifzat/ai_ebook_backend
   cd ai_ebook_backend
   ```

2. Create and activate a virtual environment:
   ```bash
   # For macOS and Ubuntu
   python3 -m venv venv
   source venv/bin/activate

   # For Windows
   python -m venv venv
   venv\Scripts\activate
   ```

3. Install the dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Usage

1. Run the application:
   ```bash
   # For macOS and Ubuntu
   python3 main.py

   # For Windows
   python main.py
   ```

2. Access the API at `http://localhost:4000`.

## Docker

1. Build and run the Docker container:
   ```bash
   docker compose up
   ```

2. The application will be available at `http://localhost:4000`.

## Contributing
Contributions are welcome! Please open an issue or submit a pull request for any changes.

## License
This project is licensed under the MIT License.
