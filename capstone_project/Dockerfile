# Filename: Dockerfile
FROM python:3.6
RUN apt-get update
RUN apt-get install -y python3-sphinx swig pulseaudio libasound2-dev libpulse-dev ffmpeg
RUN pip install --no-cache-dir requests pydub kafka SpeechRecognition pocketsphinx
COPY . /scripts
RUN ["chmod", "+x", "/scripts/src/main/python/start_stream.sh"]
RUN ["mkdir", "-p", "/logs/kafka_success/kafka_producers"]
RUN ["mkdir", "-p", "/logs/kafka_success/kafka_consumers"]
RUN ["mkdir", "-p", "/logs/kafka_failure"]
ENTRYPOINT ["/scripts/src/main/python/start_stream.sh"]
