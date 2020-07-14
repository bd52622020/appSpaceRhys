#!/usr/bin/env python
from kafka import KafkaProducer
import logging
import requests
import re
from urllib.request import urlopen
from sys import argv, exit
from time import time, sleep


#Log messages successfully published to kafka
def on_send_success(record_metadata):
    ps_log = logging.getLogger("producer_success_logger")
    log_message = record_metadata.topic + " " + str(record_metadata.partition) + " " + str(record_metadata.offset)
    # add_unique handler for each log
    for hdlr in ps_log.handlers[:]:
        if isinstance(hdlr,logging.FileHandler):
            ps_log.removeHandler(hdlr)
    handler = logging.FileHandler(f"./logs/kafka_success/kafka_producers/input_send_{str(time())}")        
    handler.setFormatter(logging.Formatter('%(message)s,%(asctime)s,%(levelname)s',"%Y-%m-%d %H:%M:%S"))  
    ps_log.addHandler(handler)
    ps_log.info(log_message)
    handler.close()

#Log kafka publishing errors
def on_send_error(e):
    pf_log = logging.getLogger("producer_failure_logger")
    pf_log.error(exc_info=e)

#Create logger with supplied arguments
def create_logger(name, log_file, level=logging.INFO):
    handler = logging.FileHandler(log_file)        
    handler.setFormatter(logging.Formatter('%(message)s,%(asctime)s,%(levelname)s',"%Y-%m-%d %H:%M:%S"))
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)
    return logger

#define loggers
def logging_init(): 
    create_logger('general_error_logger', './logs/errors.log', level=logging.ERROR)
    create_logger('producer_success_logger', f"./logs/kafka_success/kafka_producers/input_send_{str(time())}", level=logging.INFO)
    create_logger('producer_failure_logger', './logs/kafka_failure/kafka_producers_failure.log', level=logging.ERROR)
 
#get mp3 stream from playlist   
def stream_get(url1):
    if url1.endswith('pls'):
        #get stream url from pls file
        plsMessage = str(requests.get(url1).text)
        return re.findall("(?<=File1=).*",plsMessage)[0]
    elif url1.endswith('m3u'):
        #get stream url from m3u file
        return str(requests.get(url1).text)
    elif url1.endswith('mp3'):
        return url1
    else:
        raise Exception(f"{url1} is either not pls/m3u or cannot be found,")

#Publish 1kb binary chunks to kafka as messages
def input_produce(stream,producer,topic):  
    while True:
        #read 1 kB file chunks
        message = stream.read(1024)
        if(message != b''):
            t = str(int(round(time() * 1000)))
            #publish to kafka
            producer.send(topic, key=t.encode("utf_8"), value=message).add_callback(on_send_success).add_errback(on_send_error)
        else:
            return 0
        

def main(args):
    logging_init()
    
    e_log = logging.getLogger("general_error_logger")
    
    try:   
        url1 = args[2]
        topic_produce = args[1] + '_mp3'
    except Exception as e:
        e_log.error("Not enough arguments provided to radioProducer script,",exc_info=e)
        exit()
        
    try:
        fileUrl = stream_get(url1)
    except Exception as e:
        e_log.error(f"unsupported playlist format or invalid url,",exc_info=e)
        exit()

    try:     
        #initialize kafka producer
        producer = KafkaProducer(bootstrap_servers=['data-VirtualBox:9092'], retries=5)
    except Exception as e:
        e_log.error(f"Kafka could not connect to bootstrap servers,",exc_info=e) 
        exit()
        
    try:
        #open file stream connection
        stream=urlopen(fileUrl)
    except Exception as e:
        e_log.error(f"{args[1]} radioProducer script Failed to connect to stream at {fileUrl},",exc_info=e) 
        exit()
    
    #Produce messages from stream indefinitely with a slight delay if no data available 
    while True:
        input_produce(stream,producer,topic_produce)
        sleep(0.5)

        
if __name__ == "__main__":   
    main(argv)