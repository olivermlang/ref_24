import argparse
from google.cloud import speech_v1
import wave
import json
import os

def process_audio(audio_file,json_basedir,channel_program_id):
    client = speech_v1.SpeechClient()
    
    file_uri = f"gs://tscribe_audio/{channel_program_id}/{os.path.basename(audio_file)}"
    
    audio=speech_v1.RecognitionAudio(uri=file_uri)
    
    config = speech_v1.RecognitionConfig(
        language_code="de-DE",
        model="default",  # Or 'video', 'phone_call', etc.
        enable_word_confidence=True,
        enable_word_time_offsets=True)
        
    operation = client.long_running_recognize(config=config, audio=audio)
    response = operation.result(timeout=10000)
    # Save the JSON string to a file
    response_json = type(response).to_json(response)
    with open(f'{json_basedir}/{os.path.basename(audio_file).replace(".flac", "")}.transcript.json', 'w', encoding='utf-8') as file:
        file.write(response_json)

def main():
    parser = argparse.ArgumentParser(description='Process an audio file for speech recognition.')
    parser.add_argument('audio_file', help='The path to the audio file')
    parser.add_argument('json_basedir', help='The path to the jsons folder')
    parser.add_argument('channel_program_id', help='channel identifier')


    args = parser.parse_args()

    process_audio(args.audio_file, args.json_basedir, args.channel_program_id)

if __name__ == "__main__":
    main()
