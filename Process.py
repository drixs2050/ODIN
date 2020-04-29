from datetime import datetime
from create import *
import psycopg2
import sys
import json

def archive(username, infolist):
	conn = psycopg2.connect(user=username, database='odin')
	cursor = conn.cursor()
	cursor.execute("SELECT EXISTS(SELECT * FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'archive');")
	if cursor.fetchone()[0] == False:
		attr = {'payload': 'json', 'processed_by': 'VARCHAR DEFAULT CURRENT_USER', 'processed_on': 'timestamp', 'archived_on': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP'}
		createTable('archive', attr, username)
	for i in infolist:
		processed_time = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')
		i['archived_on'] = processed_time
		i['processed_by'] = username
		insertTableJson(i, username)

def createIncomingTrigger(username):
	conn = psycopg2.connect(user=username, database='odin')
	cursor = conn.cursor()
	cursor.execute("""CREATE OR REPLACE FUNCTION incoming_delete() RETURNS TRIGGER AS $$ BEGIN INSERT INTO archive (payload, processed_on) VALUES (OLD.payload, OLD.processed_on); RETURN OLD; END; $$ LANGUAGE 'plpgsql';""")
	cursor.execute("""CREATE TRIGGER t_incoming_delete BEFORE DELETE ON incoming FOR EACH ROW EXECUTE PROCEDURE incoming_delete();""")
	conn.commit()
	conn.close()

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
	for data in incoming_data:
		table_name = data['name'].lower()
		current_tables = showAllTablesODIN(False, username)
		if table_name == 'grouper' and not (table_name in current_tables):
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
			createTable(table_name, var_dict, username)
		if (table_name in current_tables):
			attribute_lst = showPSQLAttribute(table_name, username)
			for keys in data:
				if keys not in attribute_lst:
					alterTable(data['name'], keys, 'varchar', username)		
			processed_time = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')
			insertTableJson(data,username)
			payload_data = json.dumps(data, indent=4, sort_keys=True)
			single_archive = {'name': 'archive', 'payload': payload_data, 'processed_on': processed_time}
			archive_lst.append(single_archive)
	archive(username, archive_lst)
			
			
if __name__ == "__main__":
	processing(sys.argv[1])
	execute(sys.argv[1])
	createIncomingTrigger(sys.argv[1])
