from run_widgets import *

def test_check_list():
    check_items('s3')
    
#Check log file for output

def test_uploads():
    create_widget_dynamodb(1666899570909.txt)
    create_widget_s3(1666899570909.txt)

#Check aws for uploads

def test_delete():
    delete_object('usu-cs5260-ignite-requests', 1666899570909.txt)