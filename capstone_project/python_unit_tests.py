import unittest
from transcript import mp3_convert, transcription, get_key, get_value, produce_transcript
from input import stream_get
import magic
from time import time
from kafka import KafkaProducer
from kafka import KafkaConsumer

class TestRadioMethods(unittest.TestCase):

    def test_conversion(self):
        conversion_bytes = open("./test_files/superman-radioshow-1940.mp3", "rb")
        self.assertEqual(magic.from_buffer(mp3_convert(conversion_bytes.read()).read()), 'RIFF (little-endian) data, WAVE audio, Microsoft PCM, 16 bit, mono 22050 Hz')
        conversion_bytes.close()

    def test_transcription(self):
        new_time = time
        transcription_file = open("./test_files/OSR_us_000_0011_8k.wav", "rb")
        test_transcript = transcription(transcription_file,new_time)
        words = test_transcript['transcript'].split(' ')
        self.assertTrue(len(words) > 10)
        self.assertTrue("the" in words)
        self.assertTrue("a" in words)
        self.assertTrue(test_transcript['timestamp'] == str(new_time))
        transcription_file.close()

    def test_key(self):
        self.assertEqual(get_key("string"), b'string') 
                         
    def test_value(self):
        self.assertEqual(get_value("foo bar a b 3"), b'"foo bar a b 3"')
        
    def test_stream_get(self):  
        self.assertEqual(stream_get('http://www.test.com/test.mp3'),'http://www.test.com/test.mp3') 
        
    def test_produce_transcript(self):
        producer = KafkaProducer(bootstrap_servers=['data-VirtualBox:9092'])
        produce_transcript(producer, "test_produce_transcript",b"foo",b"bar")                    
        consumer = KafkaConsumer("test_produce_transcript",
                                  bootstrap_servers=['data-VirtualBox:9092'],
                                  auto_offset_reset='earliest',
                                  enable_auto_commit=True,
                                  group_id='test_produce_transcript')  
        
        for message in consumer:
            self.assertEqual(message.key,b"foo")
            self.assertEqual(message.value,b"bar")
            break
                 
        
if __name__ == '__main__':
    unittest.main()