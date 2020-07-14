import unittest
from transcript import mp3_convert, transcription, get_key, get_value, produce_transcript, consume_transcript
from input import stream_get, input_produce
import magic
from time import time
from kafka import KafkaProducer
from kafka import KafkaConsumer
import os
import io
import json

class TestRadioMethods(unittest.TestCase):
    
    #Test that mp3 to wav conversion is working
    def test_conversion(self):
        conversion_bytes = open("./test_files/superman-radioshow-1940.mp3", "rb")
        self.assertEqual(magic.from_buffer(mp3_convert(conversion_bytes.read()).read()), 'RIFF (little-endian) data, WAVE audio, Microsoft PCM, 16 bit, mono 22050 Hz')
        conversion_bytes.close()
    
    #Test that transcription is working
    def test_transcription(self):
        new_time = time
        transcription_file = open("./test_files/OSR_us_000_0011_8k.wav", "rb")
        test_transcript = transcription(transcription_file,new_time)
        words = test_transcript['transcript'].split(' ')
        self.assertTrue(len(words) > 10)
        self.assertTrue("the" in words)
        self.assertTrue("a" in words)
        for word in words:
            self.assertTrue(isinstance(word, str))
        self.assertTrue(test_transcript['timestamp'] == str(new_time))
        transcription_file.close()

    #Test that key formatting is working
    def test_key(self):
        self.assertEqual(get_key("string"), b'string') 
    
    # Test that value formatting is working                    
    def test_value(self):
        self.assertEqual(get_value("foo bar a b 3"), b'"foo bar a b 3"')
    
    #Test that stream parser is working    
    def test_stream_get(self):  
        self.assertEqual(stream_get('http://www.test.com/test.mp3'),'http://www.test.com/test.mp3') 
    
    #Test that transcript.py produces expected records to kafka   
    def test_produce_transcript(self):
        topic = "tranp_test_" + str(int(time()))
        producer = KafkaProducer(bootstrap_servers=['data-VirtualBox:9092'])
        produce_transcript(producer, topic,b"foo",b"bar")                    
        consumer = KafkaConsumer(topic,
                                  bootstrap_servers=['data-VirtualBox:9092'],
                                  auto_offset_reset='earliest',
                                  enable_auto_commit=True,
                                  group_id='test_produce_transcript')  
        
        for message in consumer:
            self.assertEqual(message.key,b"foo")
            self.assertEqual(message.value,b"bar")
            break
    
    #Test that transcript.py consumes records from kafka as expected   
    def test_transcript_consume(self):
        topic = "tranc_test_" + str(int(time()))
        t = int(round(time() * 1000))
        producer = KafkaProducer(bootstrap_servers=['data-VirtualBox:9092'])
        consumer = KafkaConsumer(topic,
                                  bootstrap_servers=['data-VirtualBox:9092'],
                                  auto_offset_reset='earliest',
                                  enable_auto_commit=True,
                                  group_id='test_consumer_transcript') 
        
        with open("./test_files/superman-radioshow-1940.mp3","rb") as file:
            for i in range(0,301):
                message = file.read(1024)
                producer.send(topic, key=str(t+(100*i)).encode("utf-8"), value=message)
                
        output = json.loads(consume_transcript(consumer))
        words = output["transcript"].split(" ")
        self.assertTrue(output["timestamp"]==str(t))
        self.assertTrue(len(words)>10)
        self.assertTrue("the" in words)
        for word in words:
            self.assertTrue(isinstance(word, str))
          
    #Test that input.py produces expected records to kafka 
    def test_input_produce(self):
        producer = KafkaProducer(bootstrap_servers=['data-VirtualBox:9092'])
        base_message = b''
        topic = "inp_test_" + str(int(time()))
        for _ in range(2048):
            base_message += os.urandom(4)
        message_in = io.BytesIO(base_message)
        message_out = io.BytesIO(base_message)
        input_produce(message_in, producer, topic)
        consumer = KafkaConsumer(topic,
                                  bootstrap_servers=['data-VirtualBox:9092'],
                                  auto_offset_reset='earliest',
                                  enable_auto_commit=True,
                                  group_id='test_input_producer') 
         
        for message in consumer:         
            self.assertEqual(message.value,message_out.read(1024))
            break      
        
if __name__ == '__main__':
    unittest.main()