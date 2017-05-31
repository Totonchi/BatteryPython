#!/usr/bin/env python
import sys
import time
import json
import string
import logging
import datetime
import logging.handlers
import unicodedata

#web
import pika
import mysql.connector

#emails
import smtplib
from email.mime.text import MIMEText

#threading
import Queue
from threading import Thread, Lock, RLock, Event

THREAD_LIMIT = 32
QUEUE_LIMIT = 1000

LOG_FORMAT = ('%(threadName) - 15s %(asctime)s %(funcName) -35s %(lineno) -5d: %(message)s')
LOG_FILENAME = 'battery_consumer.log'
LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
#handler = logging.handlers.TimedRotatingFileHandler(LOG_FILENAME, when='H', interval=1)
#handler.setFormatter(logging.Formatter(LOG_FORMAT))
#LOGGER.addHandler(handler)

## QUALITY OF LIFE METHODS
def connect_sql(settings):
   config = {
      'user': settings['user'],
      'password': settings['password'],
      'host': settings['host'],
      'database': settings['database'],
   }
   return mysql.connector.connect(**config)

def isnum(s):
   try:
      float(s)
      return True
   except ValueError:
      return False

def json_uni_to_str (stuff):
   ret = stuff
   for key, value in stuff.iteritems():
      if (isinstance(value, basestring)):
         ret[key] = unicodedata.normalize('NFKD', value).encode('ascii', 'ignore')
   return ret

def memory_usage():
   #Memory usage of the current process in kilobytes
   status = None
   result = {'peak': 0, 'rss': 0}
   try:
       # This will only work on systems with a /proc file system
       # (like Linux).
       status = open('/proc/self/status')
       for line in status:
           parts = line.split()
           key = parts[0][2:-1].lower()
           if key in result:
               result[key] = int(parts[1])
   finally:
       if status is not None:
           status.close()
   return result

## ---- CLASSES ----
class BatteryConsumer(object):
   EXCHANGE = 'battery_diagnostic.fanout'
   EXCHANGE_TYPE = 'fanout'
   QUEUE = 'battery_diagnostic_consumer_1'

   def __init__(self, settings, client):
      self._host = settings['host']
      self._port = int(settings['port'])
      self._vhost = settings['vhost']
      self._user = settings['user']
      self._pass = settings['password']
      self._connection = None
      self._channel = None
      self._closing = False
      self._consumer_tag = None
      self._handler = client

   def connect(self):
      LOGGER.info('Connecting to %s', self._host)
      credentials = pika.PlainCredentials(self._user, self._pass)
      parameters = pika.ConnectionParameters(self._host, self._port, self._vhost, credentials)
      return pika.SelectConnection(parameters, self.on_connection_open, stop_ioloop_on_close=False)

   def close_connection(self):
      LOGGER.info('Closing connection')
      self._connection.close()

   def add_on_connection_close_callback(self):
      LOGGER.info('Adding connection close callback')
      self._connection.add_on_close_callback(self.on_connection_closed)

   def on_connection_closed(self, connection, reply_code, reply_text):
      self._channel = None
      if self._closing:
         self._connection.ioloop.stop()
      else:
         LOGGER.warning('Connection closed, reopening in 5 seconds: (%s) %s', reply_code, reply_text)
         self._connection.add_timeout(5, self.reconnect)

   def on_connection_open(self, unused_connection):
      LOGGER.info('Connection opened')
      self.add_on_connection_close_callback()
      self.open_channel()

   def reconnect(self):
      self._connection.ioloop.stop()
      if not self._closing:
         self._connection = self.connect()
         self._connection.ioloop.start()

   def add_on_channel_close_callback(self):
      LOGGER.info('Adding channel close callback')
      self._channel.add_on_close_callback(self.on_channel_closed)

   def on_channel_closed(self, channel):
      LOGGER.warning('Channel %i', channel)
      self._connection.close()

   def on_channel_open(self, channel):
      LOGGER.info('Channel opened')
      self._channel = channel
      self.add_on_channel_close_callback()
      #self.setup_exchange(self.EXCHANGE)
      self.setup_queue() #remove if setup_exchange is uncommented

   def setup_exchange(self, exchange_name):
      LOGGER.info('Declaring exchange %s', exchange_name)
      self._channel.exchange_declare(self.on_exchange_declareok, exchange_name, self.EXCHANGE_TYPE, durable=True)

   def on_exchange_declareok(self, unused_frame):
      LOGGER.info('Exchange declared')
      self.setup_queue()

   def setup_queue(self):
      LOGGER.info('Declaring queue')
      self._channel.queue_declare(queue=self.QUEUE, callback=self.on_queue_declareok, durable=True, arguments={'x-message-ttl': 21600000})

   def on_queue_declareok(self, method_frame):
      self.QUEUE = method_frame.method.queue
      LOGGER.info('Binding %s to %s', self.EXCHANGE, self.QUEUE)
      self._channel.queue_bind(self.on_bindok, self.QUEUE, self.EXCHANGE)

   def add_on_cancel_callback(self):
      LOGGER.info('Adding consumer cancellation callback')
      self._channel.add_on_cancel_callback(self.on_consumer_cancelled)

   def on_consumer_cancelled(self, method_frame):
      LOGGER.info('Consumer was cancelled remotely, shutting down: %r', method_frame)
      if self._channel:
         self._channel.close()

   def acknowledge_message(self, delivery_tag):
      LOGGER.info('Acknowledging message %s', delivery_tag)
      self._channel.basic_ack(delivery_tag)

   def reject_message(self, delivery_tag):
      LOGGER.info("Rejecting message %s", delivery_tag)
      self._channel.basic_nack(delivery_tag, requeue=True)

   def on_message(self, unused_channel, basic_deliver, properties, body):
      LOGGER.info('Received message # %s', basic_deliver.delivery_tag)
      if self._handler.handle_message(body):
         self.reject_message(basic_deliver.delivery_tag)
      else:
         self.acknowledge_message(basic_deliver.delivery_tag)

   def restart(self):
      LOGGER.info("Restarting consumer")
      self.start_consuming()
      self._connection.ioloop.start()
      sys.exit()

   def on_cancelok(self, unused_frame):
      LOGGER.info('RabbitMQ acknowledged the cancellation of the consumer')
      self.close_channel()

   def on_cancelok_keep_channel(self, unused_frame):
      LOGGER.info('RabbitMQ acknowledged the cancellation of the consumer')

   def stop_consuming(self):
      if self._channel:
         LOGGER.info('Sending a Basic.Cancel RPC command to RabbitMQ')
         self._channel.basic_cancel(self.on_cancelok, self._consumer_tag)


   def stop_consuming_keep_channel(self):
      if self._channel:
         LOGGER.info('Sending a Basic.Cancel RPC command to RabbitMQ')
         self._channel.basic_cancel(self.on_cancelok_keep_channel, self._consumer_tag)

   def start_consuming(self):
      LOGGER.info('Issuing consumer related RPC commands')
      if not self._channel:
         self.open_channel()
         self.add_on_cancel_callback()
      self._consumer_tag = self._channel.basic_consume(self.on_message, self.QUEUE)

   def on_bindok(self, unused_frame):
      LOGGER.info('Queue bound')
      self.start_consuming()

   def close_channel(self):
      LOGGER.info('Closing the channel')
      self._channel.close()

   def open_channel(self):
      LOGGER.info('Creating a new channel')
      self._connection.channel(on_open_callback=self.on_channel_open)

   def run(self):
      self._connection = self.connect()
      self._connection.ioloop.start()

   def stop(self, keep_channel=False):
      LOGGER.info('Stopping')
      self._closing = True
      if keep_channel:
         self.stop_consuming_keep_channel()
      else:
         self.stop_consuming()
         self._connection.ioloop.stop()
      LOGGER.info('Stopped')

class BatteryParser:
   def __init__(self, sql):
      self.sqldb = connect_sql(sql)
      self.last_emails = {}
      LOGGER.info('BatteryParser created')
   def email_user(self, msg, split):
      email_subjects = []
      if (float(split[0]) < 30):
         email_subjects.append("Your SOC is below 30")
      if (float(split[5]) < 1):
         email_subjects.append("Contact status is 0")
      if (email_subjects):
         send = False
         current_time = time.time()
         for sub in email_subjects:
            if (sub in self.last_emails):
               if (current_time - self.last_emails[sub]) < 3600:
                  send = True
                  break
            else:
               send = True
               break
         if (send):
            me = 'devuser@stromcore.tk'
            recipients = ['j.dossantos@stromcore.com', 'm.vidricaire@stromcore.com']
            subject = ', '.join(email_subjects)
            LOGGER.info("%s, emailing %s", subject, you)
            email = MIMEText(msg)
            email['Subject'] = subject
            email['From'] = me 
            email['To'] = ', '.join(recipients)
            s = smtplib.SMTP('localhost')
            s.sendmail(me, recipients, email.as_string())
            s.quit() 
            for sub in email_subjects:
               self.last_emails[sub] = current_time

   def handle_message(self, msg):
      try:
         self.handle(msg)
      except:
         LOGGER.info(sys.exc_info()[0])

   def handle(self, msg):
      split_msg = msg.split(' ')
      if (split_msg[len(split_msg)-1] == ''):
         split_msg.pop(len(split_msg)-1)
      cursor = self.sqldb.cursor()
      #try:
      #   self.email_user(msg, split_msg[5:])
      #except:
      #   LOGGER.info(sys.exc_info()[0])
      #try:
      table_name = split_msg[0]
      current_time = datetime.datetime.now()
      datetime_string = current_time.isoformat(' ')
      float_data = map(float, split_msg[5:10])
      int_data = [int(float(i)) for i in split_msg[10:]]
      data_string_floats = ','.join(str(x) for x in float_data)
      #data_string_ints = ','.join(str(x) for x in int_data) 
      #query = "INSERT INTO %s VALUES ('%s',%s,%s)" % (table_name, datetime_string, data_string_floats, data_string_ints)
      #cursor.execute(query)
      #self.sqldb.commit()
      data = self.process_bit_values(int_data[1:])
      fields = {}
      if hasattr(self, 'fields'):
         fields = self.fields
      else:
         query = "SELECT * from %s LIMIT 1" % (table_name + 'b')
         cursor.execute(query)
         field_names = [i[0] for i in (cursor.description)[7:]]
         stuff = cursor.fetchall()
         for name in field_names:
            field = ''
            for k,v in data.iteritems():
               if name == k:
                  field = k
                  break
               else:
                  a = name.replace('_', '').lower()
                  b = k.replace(' ', '').replace('_', '').lower()
                  if a == b:
                     field = k
                     break
                     field = k
                     break
                  else:
                     b = b.replace('voltage', 'v').replace('temperature', 'temp').replace('discharge', 'dis')\
                        .replace('current', 'curr').replace('system', 'sys').replace('charge', 'chg')
                     if a == b or (a + 'ing') == b:
                        field = k
                        break
            if field:
               fields[name] = field
            else:
               LOGGER.info("field not found: %s", name) 
         fields['fields_names'] = field_names
         self.fields = fields
      bits = []
      for name in fields['fields_names']:
         bits.append(data[fields[name]])
      data_string_bits = ','.join(str(x) for x in bits)
      query = "INSERT INTO %s VALUES ('%s',%s,%s,%s)" % (table_name + 'b', datetime_string, data_string_floats, str(int_data[0]), data_string_bits)
      #print query
      cursor.execute(query)
      self.sqldb.commit()
      cursor.close()
   
   def convert_bit_string(self, bit_string, bitstart, bitlength=1):
      return int(bit_string[bitstart:bitstart+bitlength], 2)

   def process_bit_values(self, data):
      bit_string = ''
      for byte in data:
         foobar = '{0:08b}'.format(byte)
         bit_string += foobar
         #bit_string += foobar[::-1] #reverse string
      can_list = []
      with open('/battery/python/display_can.csv', 'r') as f:
         for line in f:
            split = line.split(',')
            if len(split) < 5:
               continue
            tmp = {}
            tmp['CAN'] = split[0]
            tmp['start'] = int(split[1])
            tmp['length'] = int(split[2])
            tmp['scale'] = float(split[3])
            tmp['offset'] = int(split[4])
            can_list.append(tmp)
      data = {}
      last = {}
      relative_bit = 0
      for row in can_list:
         if row['start'] == 0:
            if last:
               relative_bit = last['start'] + last['length'] 
         entry = data
         #entry[row['CAN']] = row['scale']*self.convert_bit_string(bit_string, 
         #   relative_bit + row['start'], row['length']) + row['offset']
         entry[row['CAN']] = self.convert_bit_string(bit_string, 
            relative_bit + row['start'], row['length'])
         last = row 
      return data

## ---- MAIN ----
def main():
   json_data = open('/battery/settings.json')
   raw = json.load(json_data)
   settings = json_uni_to_str(raw)
   parser = BatteryParser(settings['sql'])
   consumer = BatteryConsumer(settings['rabbitmq'], parser)
   try:
      consumer.run()
   except KeyboardInterrupt:
      consumer.stop()

if __name__ == '__main__':
   main()
