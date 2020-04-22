from datetime import datetime
from create import *
import psycopg2
<<<<<<< HEAD
import sys
from create import * 

def archive():
	pass
	# TODO: create archive data bases

=======
def archive(username):
	conn = psycopg2.connect(user=username, database='odin')
	cursor = conn.cursor()
	cursor.execute("SELECT EXISTS(SELECT * FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'archive');")
	if cursor.fetchone()[0] == 'f':
		attr = {"payload": "json", "processed_by": "varchar", "processed_on": "datetime", "archived on": "datetime"}
		createTable("archive", attr, username)


>>>>>>> 56281b9f3b3eceb003bcc809a09816d60ca8b778
def processing(username):
	conn = psycopg2.connect(user=username, database='odin')
	cursor = conn.cursor()
	cursor.execute("SELECT payload from incoming;")
	incoming_data = []
	for row in cursor.fetchall():
		incoming_data.append(row[0])
	conn.close()
	return incoming_data


def execute(username):
	incoming_data = processing(username)
<<<<<<< HEAD
	current_tables = showAllTablesODIN(False, username)
	#print(incoming_data[0]['name'])
	#print(current_tables)
	if (incoming_data[0]['name'] in current_tables):
		pass
	elif (incoming_data[0]['name'] == 'Grouper'):
		var_dict = {}
		max_attribute_len = 0
		index = 0
		for column in incoming_data:
			if (len(column) > max_attribute_len):
				max_attribute_len = len(column)
			index++
		max_data_index = incoming_data.index(max(incoming_data))
		for column in incoming_data[max_data_index]:
			if (type(incoming_data[max_data_index][column]) == type({})):
				var_dict['stemname'] = 'varchar'
				var_dict['numstems'] = 'Int'
			else:
				var_dict[column] = 'varchar'
		createTable(table_name, var_dict, username)
		

	
	
# TODO: insert all info parsed by json and archive info
=======


>>>>>>> 56281b9f3b3eceb003bcc809a09816d60ca8b778
if __name__ == "__main__":

	processing()
	execute()
