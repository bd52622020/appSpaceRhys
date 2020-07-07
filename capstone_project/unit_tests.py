import unittest
from transcript import mp3Convert, transcription, getKey, getValue
from input import streamGet
import magic
from time import time

class TestRadioMethods(unittest.TestCase):

    def test_conversion(self):
        conversion_bytes = open("./test_files/file_example.mp3", "rb")
        self.assertEqual(magic.from_buffer(mp3Convert(conversion_bytes.read()).read()), 'RIFF (little-endian) data, WAVE audio, Microsoft PCM, 16 bit, stereo 32000 Hz')
        conversion_bytes.close()

    def test_transcription(self):
        new_time = time
        transcription_file = open("./test_files/OSR_us_000_0011_8k.wav", "rb")
        test_transcript = transcription(transcription_file,new_time)
        self.assertTrue((len(test_transcript['transcript'].split(' '))) > 5)
        self.assertTrue(test_transcript['timestamp'] == str(new_time))
        transcription_file.close()

    def test_key(self):
        self.assertEqual(getKey("string"), b'string') 
                         
    def test_value(self):
        self.assertEqual(getValue("foo bar a b 3"), b'"foo bar a b 3"')
        
    def test_stream_get(self):  
        self.assertEqual(streamGet('http://www.test.com/test.mp3'),'http://www.test.com/test.mp3')                     

if __name__ == '__main__':
    unittest.main()