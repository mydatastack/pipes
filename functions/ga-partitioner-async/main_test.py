import unittest
from main import handler
import json
from datetime import datetime
import time

class TestHandler(unittest.TestCase):
    def test_read_file_data(self):
        pass

    def test_run_handler(self):
        #print(datetime.utcfromtimestamp(int(1561384885876 / 1000)))

        with open('payload.json') as myfile:
            event = json.load(myfile)
            handler(event, None)
            #self.assertEqual(handler(event, None), 'success')


if __name__ == '__main__':
    unittest.main()
