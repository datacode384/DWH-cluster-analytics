from behave import *
import pymqi
from hdfs_functions import run_cmd
import numpy as np
import os

@given('the following messages haven been sent to queue "{queue_name}"')
def step_impl(context,queue_name):
    queue = connect_to_mq(queue_name)
    heading = context.table.headings[0]
    for row in context.table:
        message = row[heading]
        queue.put(message)
    queue.close()

@then('read message from queue "{queue_name}" gives')
def step_impl(context, queue_name):
    queue = connect_to_mq(queue_name)
    heading = context.table.headings[0]
    md = pymqi.MD()
    keep_running = True
    new_msg=[]
    expected =[]
    for row in context.table:
        expected.append((int(row[heading])) if row[heading].isdigit() else row[heading])
    while keep_running:
        try:
            message = queue.get(None, md)   
            md.MsgId = pymqi.CMQC.MQMI_NONE
            md.CorrelId = pymqi.CMQC.MQCI_NONE
            md.GroupId = pymqi.CMQC.MQGI_NONE
            new_msg.append(message.decode('utf-8'))
            print(new_msg)
        except pymqi.MQMIError as e:
            if e.comp == pymqi.CMQC.MQCC_FAILED and e.reason == pymqi.CMQC.MQRC_NO_MSG_AVAILABLE:
                break
            else:
                raise
    np.testing.assert_array_equal(new_msg,expected)
    queue.close()


@step('message queue "{queue_name}" is empty')
def step_impl(context,queue_name):
    queue = connect_to_mq(queue_name)
    try:
            message = queue.get()
    except pymqi.MQMIError as e:
            if e.comp == pymqi.CMQC.MQCC_FAILED and e.reason == pymqi.CMQC.MQRC_NO_MSG_AVAILABLE:
                  print("Empty queue!")            
    queue.close()

@step(
    'send "{no_messages}" messages with length "{message_size}" to queue "{queue_name}" #prefixed with running nr, filled with \'#\' to max len')
def step_impl(context,no_messages,message_size,queue_name):
    queue = connect_to_mq(queue_name)
    message = "#" * int(message_size)
    for i in range(int(no_messages)):
        queue.put(message)
    

def connect_to_mq(queue_name):
    queue_manager = 'QMPUBLIC'
    channel = 'DEV.APP.SVRCONN'
    host = '10.194.5.252'
    port = '32090'
    conn_info = '%s(%s)' % (host, port)
    user = 'app'
    password = 'ZrOp73QqAa3'
    qmgr = pymqi.connect(queue_manager, channel, conn_info, user, password)
    queue = pymqi.Queue(qmgr, queue_name)
    return queue


@step('the "{sbt}" execute command is called in path "{path_name}"')
def step_impl(context,sbt,path_name):
    os.chdir(path_name)
    os.system('sbt compile')
    os.system('sbt run')
