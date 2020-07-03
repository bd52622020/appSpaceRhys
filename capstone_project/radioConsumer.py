#!/usr/bin/env python
from kafka import KafkaProducer
from kafka import KafkaConsumer
import logging
import io
import speech_recognition as sr
from pydub import AudioSegment
from sys import argv, exit
import json



def on_send_success(record_metadata):
    ps_log = logging.getLogger("producer_success_logger")
    log_message = record_metadata.topic + " " + str(record_metadata.partition) + " " + str(record_metadata.offset)
    print(log_message)
    ps_log.info(log_message)

def on_send_error(e):
    pf_log = logging.getLogger("producer_failure_logger")
    pf_log.error(exc_info=e)

def create_logger(name, log_file, level=logging.INFO):
    handler = logging.FileHandler(log_file)        
    handler.setFormatter(logging.Formatter('%(message)s,%(asctime)s,%(levelname)s',"%Y-%m-%d %H:%M:%S"))
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)
    return logger

#define loggers
def logging_init(): 
    #define loggers
    create_logger('general_error_logger', './logs/errors.log', level=logging.ERROR)
    create_logger('producer_success_logger', './logs/kafka_success/kafka_producers_success.log', level=logging.INFO)
    create_logger('producer_failure_logger', './logs/kafka_failure/kafka_producers_failure.log', level=logging.ERROR)
    create_logger('consumer_success_logger', './logs/kafka_success/kafka_consumers_success.log', level=logging.INFO)
    create_logger('consumer_failure_logger', './logs/kafka_failure/kafka_consumers_failure.log', level=logging.ERROR)

def main():
    logging_init()
    
    e_log = logging.getLogger("general_error_logger")
    cs_log = logging.getLogger("consumer_success_logger")
    cf_log = logging.getLogger("consumer_failure_logger")

    try:
        topic_consume = argv[1] + '_mp3'
    except Exception as e:
        e_log.error("Not enough arguments provided to radioConsumer script,",exc_info=e)
        exit()
    try:
        topic_produce = 'transcripts'
        #initialize kafka producer
        producer = KafkaProducer(bootstrap_servers=['data-VirtualBox:9092'])
        consumer = KafkaConsumer(topic_consume,
                                  bootstrap_servers=['data-VirtualBox:9092'],
                                  auto_offset_reset='earliest',
                                  enable_auto_commit=True,
                                  group_id='bbc_4_wav_group')
        
        r = sr.Recognizer()
        
        bytestream = b''
        first = True
        
    except Exception as e:
        e_log.error("Failed to initialise radioConsumer script",exc_info=e)
        exit()
        
    for message in consumer:
        cs_log.info(f"{message.topic} {message.partition} {message.offset}")
        
        try:
            time = int(message.key.decode("utf_8"))
            bytestream += message.value
            
        except Exception:
            cf_log.error(f"{message.topic} {message.partition} {message.offset},Bad Message Formatting",exc_info=e)
            
        if first == True:
            start = time
            first = False
        
        if (time >= (start+30000)):
            test_section = bytestream
            bytestream = b''
            first = True            
            try:
                bytes_io_mp3 = io.BytesIO(test_section)
                bytes_io_wav = io.BytesIO()
                sound = AudioSegment.from_mp3(bytes_io_mp3)
                sound.set_channels(1)
                sound.export(bytes_io_wav, codec="pcm_s16le", format='wav')
                format_b = sr.AudioFile(bytes_io_wav)
                
            except Exception as e:
                cf_log.error(f"{message.topic} {message.partition} {message.offset},Audio Format Conversion Failed",exc_info=e)
                continue
            
            try:
                with format_b as source:
                    r.adjust_for_ambient_noise(source)
                    audio = r.record(source)
                    
            except Exception as e:
                cf_log.error(f"{message.topic} {message.partition} {message.offset},Audio Format Conversion Failed",exc_info=e) 
                continue
                  
            try:
                recog_text = r.recognize_sphinx(audio)
                data = {}
                data["timestamp"] = str(start)
                data["transcript"] = recog_text
                text_value = json.dumps(data,separators=(',', ':')).encode('utf-8')
                text_key = argv[1].encode('utf8')
            
            except Exception as e:
                cf_log.error(f"{message.topic} {message.partition} {message.offset},Speech Transcription Failed",exc_info=e)
                continue
                
            producer.send(topic_produce, key=text_key, value=text_value).add_callback(on_send_success).add_errback(on_send_error)
               
        
if __name__ == "__main__":   
    main()