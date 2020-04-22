from datetime import datetime
from create import *
import psycopg2
import sys

def archive(username):
	conn = psycopg2.connect(user=username, database='odin')
	cursor = conn.cursor()
	cursor.execute("SELECT EXISTS(SELECT * FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'archive');")
	if cursor.fetchone()[0] == 'f':
		attr = {"payload": "json", "processed_by": "varchar", "processed_on": "datetime", "archived on": "datetime"}
		createTable("archive", attr, username)


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
	current_tables = showAllTablesODIN(False, username)
	if (incoming_data[0]['name'].lower() == 'grouper' and not(incoming_data[0]['name'].lower() in current_tables)):
		var_dict = {}
		max_attribute_len = 0
		max_index = 0
		for i in range(len(incoming_data)):
			if (len(incoming_data[i]) > max_attribute_len):
				max_attribute_len = len(incoming_data[i])
				max_index = i
	
		for column in incoming_data[max_index]:
			if (type(incoming_data[max_index][column]) == type({})):
				var_dict['stemname'] = 'varchar'
				var_dict['numstems'] = 'Int'
			else:
				var_dict[column] = 'varchar'
		createTable(incoming_data[0]['name'], var_dict, username)

	if (incoming_data[0]['name'].lower() in current_tables):
		insertTable(incoming_data,username)	
if __name__ == "__main__":
	processing(sys.argv[1])
	execute(sys.argv[1])
