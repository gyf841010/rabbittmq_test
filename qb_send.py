#!/usr/bin/env python
import mq_config 
import logging
import logging.handlers
import msg_queque
import time
import traceback

_send = logging.getLogger('send')
_send.setLevel(logging.DEBUG)
ch = logging.handlers.RotatingFileHandler( "logs/send.log")
formatter = logging.Formatter("%(asctime)s %(levelname)s: %(message)s") 
ch.setFormatter(formatter)
_send.addHandler(ch)
    
def main():
    data = {}
    data['user_id'] = 11292388
    data['article_id'] = 88888888
    data['action'] = 'publish'
    data['created_at'] = '2014-10-23 11:11:11'

    mq_client = None
    start_time = time.time()
    try:
        mq_client = msg_queque.RabbitmqClass( mq_config.user,
                                            mq_config.password,
                                            mq_config.host, 
                                            mq_config.exchange, 
                                            mq_config.exchange_type, 
                                            mq_config.queque, 
                                            mq_config.mode, 
                                            mq_config.routings,
                                            mq_config.durable )
        while(True):
            for i in range(0, 10000):
                ret, msg = mq_client.send_data( data, 'warning', 'gongyaofei' )
                if not ret:
                    _send.info( " msg_queque send data failed: %s" % msg)
                _send.info( " msg_queque send data successfully: %s" % data )
            use_time = time.time() - start_time
            start_time = time.time()
            _send.info( "test_rabbitmq send data 10000 need: %ss" % use_time )
    except:
        _send.info( " msg_queque send data except: %s" % traceback.format_exc()) 
    finally:
        if mq_client:
            mq_client.close()

if __name__=='__main__':
    main()


