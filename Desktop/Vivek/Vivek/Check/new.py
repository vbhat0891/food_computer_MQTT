#------------------------------------------
#--- Author: Deepika Jantz
#--- Date: 25th June  2017
#--- Version: 1.0
#--- Python Ver: 3.6
#------------------------------------------

import paho.mqtt.client as mqtt
import json
import os
import psycopg2
import sys
import pprint
from sys import argv as sys_argvs

# MQTT Settings 
MQTT_Broker = "iot.eclipse.org"
MQTT_Port = 1883
Keep_Alive_Interval = 45
MQTT_Topic = "Home/BedRoom/#"

#Subscribe to all Sensors at Base Topic
def on_connect(mosq, obj, rc):
	mqttc.subscribe(MQTT_Topic, 0)
	

def DHT22_Humidity_Data_Handler(payload):
	#Parse data 
	json_Dict = json.loads(payload)
	SensorID = json_Dict['Sensor_Type']
	Data_and_Time = json_Dict['Date']
	Humidity = json_Dict['Humidity']
	
	#Connect to the database#Host needs to be changed based on local connection
	conn_string = "host='192.168.1.11' dbname='vivekdb' user='vivek' password='postgres'"
	# print the connection string we will use to connect
	print ("Connecting to database\n	->%s"%(conn_string))
	# get a connection to the database
	conn = psycopg2.connect(conn_string)
	# conn.cursor will return a cursor object, you can use this cursor to perform queries
	cursor = conn.cursor()
	cursor.execute("insert into iot.humidity_data (sensorId, datetime, humidity) values (%s,%s,%s);",(SensorID, Data_and_Time, Humidity))
	conn.commit()
	print ("Inserted Humidity Data into Database.")
	print ("")
	cursor.close()
	conn.close()
		
def DHT22_Temp_Data_Handler(payload):
	#Parse Data 
	json_Dict = json.loads(payload)
	SensorID = json_Dict['Sensor_Type']
	Data_and_Time = json_Dict['Date']
	Temperature = json_Dict['Temperature']
	
	#Push into DB Table
	conn_string = "host='192.168.1.11' dbname='vivekdb' user='vivek' password='postgres'"
	# print the connection string we will use to connect
	print ("Connecting to database\n	->%s"%(conn_string))
	# get a connection, if a connect cannot be made an exception will be raised here
	conn = psycopg2.connect(conn_string)
	# conn.cursor will return a cursor object, you can use this cursor to perform queries
	cursor = conn.cursor()
	cursor.execute("insert into iot.temperature_data (sensorId, datetime, temperature) values (%s,%s,%s);",(SensorID, Data_and_Time, Temperature))
	conn.commit()
	print ("Inserted Temperature Data into Database.")
	print ("")
	cursor.close()
	conn.close()
#Save Data into DB Table
def sensor_Data_Handler(topic, payload):
	if topic == "Home/BedRoom/DHT22/Temperature":
		DHT22_Temp_Data_Handler(payload)
	elif topic == "Home/BedRoom/DHT22/Humidity":
		DHT22_Humidity_Data_Handler(payload)	
def on_message(mosq, obj, msg):
	# This is the Master Call for saving MQTT Data into DB
	# For details of "sensor_Data_Handler" function please refer "store_Sensor_Data_to_DB.py"
	
	print ("MQTT Data Received...")
	print ("MQTT Topic: %s" %msg.topic) 
	print ("Data: %s" %msg.payload)
	#class sensorData:
	sensor_Data_Handler(msg.topic, msg.payload)
	
def on_subscribe(mosq, obj, mid, granted_qos):
	pass

mqttc = mqtt.Client()

# Assign event callbacks
mqttc.on_message = on_message
mqttc.on_connect = on_connect
mqttc.on_subscribe = on_subscribe

# Connect
mqttc.connect(MQTT_Broker, int(MQTT_Port), int(Keep_Alive_Interval))

# Continue the network loop
mqttc.loop_forever()
