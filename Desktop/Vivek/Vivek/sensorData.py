#------------------------------------------
#--- Author: Pradeep Singh
#--- Date: 20th January 2017
#--- Version: 1.0
#--- Python Ver: 2.7
#--- Details At: https://iotbytes.wordpress.com/store-mqtt-data-from-sensors-into-sql-database/
#------------------------------------------

import json
import os
import sys
from sys import argv as sys_argv
from psycopg2 import connect
#from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
#===============================================================
# Database Manager Class
	class DatabaseManager():
	
		def __init__():
			conn_string = "host='137.48.185.205' dbname='vivekdb' user='vivek' password='postgres'"
			# print the connection string we will use to connect
			print ("Connecting to database\n	->%s"%(conn_string))
			# get a connection, if a connect cannot be made an exception will be raised here
			conn = psycopg2.connect(conn_string)
			# conn.cursor will return a cursor object, you can use this cursor to perform queries
			cursor = conn.cursor()
		
		def add_del_update_db_record(sql_query, args=()):
			cursor.execute(sql_query, args)
			conn.commit()
			return

		def __del__():
			cursor.close()
			conn.close()

#===============================================================
# Functions to push Sensor Data into Database

# Function to save Temperature to DB Table
	def DHT22_Temp_Data_Handler(jsonData):
		#Parse Data 
		json_Dict = json.loads(jsonData)
		SensorID = json_Dict['Sensor_ID']
		Data_and_Time = json_Dict['Date']
		Temperature = json_Dict['Temperature']
		
		#Push into DB Table
		dbObj = DatabaseManager()
		dbObj.add_del_update_db_record("insert into iot.dht22_temperature_data (sensorId, datetime, temperature) values (%s,%s,%s);",(SensorID, Data_and_Time, Temperature))
		del dbObj
		print ("Inserted Temperature Data into Database.")
		print ("")
		
# Function to save Humidity to DB Table
	def DHT22_Humidity_Data_Handler(jsonData):
		#Parse Data 
		json_Dict = json.loads(jsonData)
		SensorID = json_Dict['Sensor_ID']
		Data_and_Time = json_Dict['Date']
		Humidity = json_Dict['Humidity']
		
		#Push into DB Table
		conn_string = "137.48.185.205' dbname='vivekdb' user='vivek' password='postgres'"
		# print the connection string we will use to connect
		print ("Connecting to database\n	->%s"%(conn_string))
		# get a connection, if a connect cannot be made an exception will be raised here
		conn = psycopg2.connect(conn_string)
		# conn.cursor will return a cursor object, you can use this cursor to perform queries
		cursor = conn.cursor()
		cursor.execute("insert into iot.dht22_humidity_data (sensorId, datetime, humidity) values (%s,%s,%s);",(SensorID, Data_and_Time, Humidity))
		conn.commit()
		print ("Inserted Humidity Data into Database.")
		print ("")
		cursor.close()
		conn.close()
		
#===============================================================
# Master Function to Select DB Funtion based on MQTT Topic

	def sensor_Data_Handler(Topic, jsonData):
		if Topic == "Home/BedRoom/DHT22/Temperature":
			DHT22_Temp_Data_Handler(jsonData)
		elif Topic == "Home/BedRoom/DHT22/Humidity":
			DHT22_Humidity_Data_Handler(jsonData)	
		

#===============================================================
