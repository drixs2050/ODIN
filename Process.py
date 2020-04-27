from datetime import datetime
from create import *
import psycopg2
import sys

def archive(username, infolist):
	conn = psycopg2.connect(user=username, database='odin')
	cursor = conn.cursor()
	cursor.execute("SELECT EXISTS(SELECT * FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'archive');")
	if cursor.fetchone()[0] == False:
		attr = {'payload': 'varchar', 'processed_by': 'varchar', 'processed_on': 'timestamp', 'archived_on': 'timestamp'}
		createTable('archive', attr, username)
	for i in infolist:
		processed_time = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')
		
		i['archived_on'] = processed_time
		i['processed_by'] = username
		insertTableJson(i, username)


def processing(username):
	conn = psycopg2.connect(user=username, database='odin')
	cursor = conn.cursor()
	cursor.execute("SELECT payload from incoming;")
	incoming_data = []
	for row in cursor.fetchall():
		incoming_data.append(row)
	conn.close()
	return incoming_data


def execute(username):
	raw_data = processing(username)
	incoming_data = []
	for data in raw_data:
		incoming_data.append(data[0])
	#For creating tables
	archive_lst = []
	for json in incoming_data:
		current_tables = showAllTablesODIN(False, username)
		if json['name'].lower() == 'grouper' and not (json['name'].lower() in current_tables):
			var_dict = {}
			max_attribute_len = 0
			max_index = 0
			for i in range(len(incoming_data)):
				if (len(incoming_data[i]) > max_attribute_len):
					max_attribute_len = len(incoming_data[i])
					max_index = i
			for column in incoming_data[max_index]:
				if type(incoming_data[max_index][column]) == type({}):
					var_dict['stemname'] = 'varchar'
					var_dict['numstems'] = 'Int'
				else:
					var_dict[column] = 'varchar'
			createTable(json['name'], var_dict, username)
	for raw_json in raw_data:	
		if (raw_json[0]['name'].lower() in current_tables):
			processed_time = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')
			#print(showAllAttributes(username, json['name']))	
			insertTableJson(raw_json[0],username)
			single_archive = {'name': 'archive', 'payload': raw_json, 'processed_on': processed_time}
			print(single_archive)
			break
			archive_lst.append(single_archive)
	pass
	archive(username, archive_lst)
			
			
if __name__ == "__main__":
	processing(sys.argv[1])
	execute(sys.argv[1])
