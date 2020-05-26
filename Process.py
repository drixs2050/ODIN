import datetime
from create import *
import psycopg2
import sys
import json
import getpass

def archive(username, password):
	conn = getConnection(username, password)
	cursor = conn.cursor()
	cursor.execute(
		"SELECT EXISTS(SELECT * FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'archive');")
	if cursor.fetchone()[0] == False:
		attr = {'payload': 'json', 'processed_by': 'VARCHAR DEFAULT CURRENT_USER', 'processed_on': 'timestamp',
				'archived_on': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP'}
		createTable('archive', attr, username, password)
	moveData(username, 'incoming', password)


def moveData(username, tablename, password):
	conn = getConnection(username, password)
	cursor = conn.cursor()
	cursor.execute("DELETE FROM {};".format(tablename))
	conn.commit()
	conn.close()


def checkIncomingTrigger(username, password):
	conn = getConnection(username, password)
	cursor = conn.cursor()
	cursor.execute("""SELECT tgname from pg_trigger where not tgisinternal AND tgname='t_incoming_delete';""")
	trigger_status = None
	for row in cursor.fetchall():
		trigger_status = row[0]
	if trigger_status == None:
		return False
	return True
	conn.close()


def checkArchiveTrigger(username, password):
	conn = getConnection(username, password)
	cursor = conn.cursor()
	cursor.execute("""SELECT tgname from pg_trigger where not tgisinternal AND tgname='t_archive_delete';""")
	trigger_status = None
	for row in cursor.fetchall():
		trigger_status = row[0]
	if trigger_status == None:
		return False
	return True
	conn.close()


def createArchiveTrigger(username, password):
	if (checkArchiveTrigger(username, password) == False):
		conn = getConnection(username, password)
		cursor = conn.cursor()
		cursor.execute(
			"""CREATE OR REPLACE FUNCTION archive_delete() RETURNS TRIGGER AS $$ BEGIN INSERT INTO incoming (payload, processed_on) VALUES (OLD.payload, OLD.processed_on); RETURN OLD; END; $$ LANGUAGE 'plpgsql';""")
		cursor.execute(
			"""CREATE TRIGGER t_archive_delete BEFORE DELETE ON archive FOR EACH ROW EXECUTE PROCEDURE archive_delete();""")
		conn.commit()
		conn.close()


def createIncomingTrigger(username, password):
	if (checkIncomingTrigger(username,password) == False):
		conn = getConnection(username, password)
		cursor = conn.cursor()
		cursor.execute(
			"""CREATE OR REPLACE FUNCTION incoming_delete() RETURNS TRIGGER AS $$ BEGIN INSERT INTO archive (payload, processed_on) VALUES (OLD.payload, OLD.processed_on); RETURN OLD; END; $$ LANGUAGE 'plpgsql';""")
		cursor.execute(
			"""CREATE TRIGGER t_incoming_delete BEFORE DELETE ON incoming FOR EACH ROW EXECUTE PROCEDURE incoming_delete();""")
		conn.commit()
		conn.close()


def processing(username, password):
	conn = getConnection(username, password)
	cursor = conn.cursor()
	cursor.execute("SELECT payload from incoming;")
	incoming_data = []
	for row in cursor.fetchall():
		incoming_data.append(row[0])
	conn.close()
	return incoming_data


def execute(username):
	incoming_data = processing(username, password)
	for data in incoming_data:
		table_name = data['name'].lower()
		current_tables = showAllTablesODIN(False, username, password)
		if (table_name == 'grouper') and not (table_name in current_tables):
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
			createTable(table_name, var_dict, username, password)
		if (table_name == 'etoken') and not (table_name in current_tables):
			var_dict = {}
			for column in data:
				if (type(column) == int):
					var_dict[column] = 'Int'
				else:
					var_dict[column] = 'varchar'
			createTable(table_name, var_dict, username, password)
			
		if (table_name in current_tables):
			attribute_lst = showPSQLAttribute(table_name, username, password)
			for keys in data:
				if keys not in attribute_lst:
					alterTable(data['name'], keys, 'varchar', username, password)
			#processed_time = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')
			insertTableJson(data, username, password)
	if (incoming_data != []):
		archive(username, password)

def etokenJsonify(username, pa):
	payload = {}
	payload['name'] = 'etoken'
	payload['run_date'] = datetime.datetime.now().strftime("%Y-%m-%d")
	payload['run_time'] = datetime.datetime.now().strftime("%H:%M:%S.%f")[:-3]
	payload['service_environment'] = "PROD"
	payload['service_version'] = "9.0"
	payload['service_title'] = "Safenet Authentication Manager Server 9.0"
	payload['virtual_etoken'] = countvirtual(username, pa)
	payload['normal_etoken'] = countNormal(username, pa)
	payload['count_2fa'] = countall(username, pa)
	payload['countsp'] = countServiceProvider(username, pa)
	payload['in2weeks'] = numExpiring(username, pa)
	payload['in1month'] = numExpiringIn(username, pa, 1)
	payload['in2month'] = numExpiringIn(username, pa, 2)
	payload['inventory'] = getInventory(username, pa)
	conn = getConnection(username, pa)
	cursor = conn.cursor()
	cursor.execute("INSERT INTO incoming (payload) VALUES ('%s')" % json.dumps(payload, indent=4))
	conn.commit()
	return payload

	
if __name__ == "__main__":
	password = getpass.getpass()
	processing(sys.argv[1], password)
	etokenJsonify(sys.argv[1], password)
	#json2csv(sys.argv[1])
	execute(sys.argv[1])
	createIncomingTrigger(sys.argv[1], password)
	createArchiveTrigger(sys.argv[1], password)
	if (len(sys.argv[1:]) > 1):
		moveData(sys.argv[1], 'archive', password)
		moveData(sys.argv[1], 'grouper', password)



