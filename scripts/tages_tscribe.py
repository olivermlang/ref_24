import argparse
from google.cloud.speech_v2 import SpeechClient
from google.cloud.speech_v2.types import cloud_speech
import wave
import json
import os

def process_audio(audio_file,json_basedir,channel_program_id):
    client = SpeechClient()

    config = cloud_speech.RecognitionConfig(
        auto_decoding_config=cloud_speech.AutoDetectDecodingConfig(),
        language_codes=["de-DE","de-DE"],
        model="long",
        features=cloud_speech.RecognitionFeatures(
            enable_word_confidence=True,
            enable_word_time_offsets=True
            )
        )

    file_uri = f"gs://tscribe_audio/{channel_program_id}/{os.path.basename(audio_file)}"
    file_metadata = cloud_speech.BatchRecognizeFileMetadata(uri=file_uri)

    request = cloud_speech.BatchRecognizeRequest(
            recognizer="projects/videos-279016/locations/global/recognizers/_",
            config=config,
            files=[file_metadata],
            recognition_output_config=cloud_speech.RecognitionOutputConfig(
                inline_response_config=cloud_speech.InlineOutputConfig(),
            ),
            processing_strategy=cloud_speech.BatchRecognizeRequest.ProcessingStrategy.DYNAMIC_BATCHING,
        )

    operation = client.batch_recognize(request=request)
    response = operation.result(timeout=10000)

    # Save the JSON string to a file
    response_json = type(response).to_json(response)
    with open(f'{json_basedir}/{os.path.basename(audio_file).replace(".wav", "")}.transcript.json', 'w', encoding='utf-8') as file:
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
