#!/usr/bin/env python
import pika
import mq_config
import time
import logging
import logging.handlers
import msg_queque
import traceback

_receive = logging.getLogger('receive')
_receive.setLevel(logging.DEBUG)
ch = logging.handlers.RotatingFileHandler( "logs/receive.log" )
formatter = logging.Formatter("%(asctime)s %(levelname)s: %(message)s") 
ch.setFormatter(formatter)
_receive.addHandler(ch)

cnt = 0
start_time = time.time()
def on_message( data ):
    global cnt
    global start_time
    _receive.info(" msg_queque receive data %s" % data)
    cnt += 1
    if cnt % 10000 == 0:
        use_time = time.time() - start_time
        start_time = time.time()
        _receive.info( " test_rabbitmq receive data 10000 need %ss" % use_time )
    #time.sleep( data['dot'].count('.') )
    #_main.info( " msg_queque Done" )

def main():
    mq_server = None
    try:
        mq_server = msg_queque.RabbitmqClass( mq_config.user,
                                            mq_config.password,
                                            mq_config.host, 
                                            mq_config.exchange, 
                                            mq_config.exchange_type, 
                                            mq_config.queque, 
                                            mq_config.mode, 
                                            mq_config.routings,
                                            mq_config.durable )
        mq_server.receive_data( on_message )
    except:
        if mq_server:
            mq_server.close()
        _receive.info( " main except: %s" % traceback.format_exc())

if __name__=='__main__':
    main()










