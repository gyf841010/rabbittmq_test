#!/usr/bin/env python
import pika
import sys
import json
import logging
import logging.handlers
import time
import traceback

_msg_queque = logging.getLogger('msg_queque')
_msg_queque.setLevel(logging.DEBUG)
ch = logging.handlers.RotatingFileHandler( "logs/msg_queque.log" )
formatter = logging.Formatter("%(asctime)s %(levelname)s: %(message)s") 
ch.setFormatter(formatter)
_msg_queque.addHandler(ch)

class  RabbitmqClass(object):
    connection = None
    channel = None

    def __init__(self, user, password, host, exchange, exchange_type, queque, mode, routings, durable):
        self.user = user
        self.password = password
        self.host = host
        self.exchange = exchange
        self.exchange_type = exchange_type
        self.mode = mode
        self.queque = queque
        self.routings = routings.split(',')
        self.durable = True if durable == 'True' else False
        self.connect()

    def connect(self):
        try:
            credentials = pika.PlainCredentials(self.user, self.password)
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host, credentials=credentials))
            self.channel = self.connection.channel()
            self.channel.confirm_delivery()
            self.channel.exchange_declare(exchange=self.exchange, exchange_type=self.exchange_type, durable=self.durable)
            self.channel.queue_declare(queue=self.queque, durable=self.durable)
        except Exception as e:
            err_msg = "error:%s, %s" % (e, traceback.format_exc())
            _msg_queque.error("msg_queque connect failed. %s" % err_msg)
            time.sleep(5)
            self.connect()

    def send_data( self, data, mq_routing, queque ):
        try:
            #buf = pickle.dumps( data )
            buf = json.JSONEncoder().encode( data )
            self.channel.basic_publish(exchange=self.exchange,
                                        routing_key=mq_routing,
                                        body=buf,
                                        properties=pika.BasicProperties(
                                        delivery_mode = self.mode, # make message persistent
                          ))
        except Exception as e:
            msg = "msg_queque send data except:%s, %s" % (e, traceback.format_exc())
            return False, msg

        return True, None

    def worker( self, on_message ):
        def callback( channel, method, properties, body):
            try:
                #data = pickle.loads(body)
                data = json.loads(body)
                on_message(data)
                channel.basic_ack(delivery_tag = method.delivery_tag)
            except:
                _msg_queque.error("msg_queque callback failed. %s" % traceback.format_exc())

        self.channel.basic_qos(prefetch_count=1)
        #self.channel.basic_consume( callback, queue=queque, no_ack=True )
        self.channel.basic_consume( callback, queue=self.queque )
        self.channel.start_consuming()

    def receive_data( self, on_message ):
        try:
            for routing in self.routings:
                self.channel.queue_bind(exchange=self.exchange, queue=self.queque, routing_key=routing)
            self.worker( on_message )

        except Exception as e:
            err_msg = "error:%s, %s" % (e, traceback.format_exc())
            _msg_queque.error("msg_queque receive data failed. %s" % err_msg)
            time.sleep(5)
            self.connect()
            self.receive_data(on_message)

    def close(self):
        try:
            self.channel.stop_consuming()
        finally:
            self.connection.close()




    
