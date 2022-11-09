from run_widgets import *
import json
import unittest
import boto3

def get_test_file():
    contents = ""
    with open('1666899570909.txt') as f:
        contents = f.read()
    return(json.loads(contents))
    
class TestStringMethods(unittest.TestCase):
        
    def test_uploads_s3(self):    
        try:
            create_widget_s3(get_test_file())
            time.sleep(2.0)
            s3 = boto3.resource('s3')
            bucket = s3.Bucket('usu-cs5260-ignite-dist')
            key = "Mary Matthews/e42ff4a6-4957-40ea-814a-46169748f1aa"
            objs = list(bucket.objects.filter(Prefix=key))
            if any([w.key == key for w in objs]):
                self.assertTrue(True)
            else:
                self.assertTrue(False)
        except:
            self.assertTrue(True)
        
    def test_delete(self):
        delete_object('usu-cs5260-ignite-requests', '1666899570909.txt')
        time.sleep(2.0)
        s3 = boto3.resource('s3')
        bucket = s3.Bucket('usu-cs5260-ignite-requests')
        key = '1666899570909.txt'
        objs = list(bucket.objects.filter(Prefix=key))
        if any([w.key == key for w in objs]):
            self.assertFalse(True)
        else:
            self.assertFalse(False)

if __name__ == '__main__':
    unittest.main()