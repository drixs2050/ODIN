from datetime import datetime
from create import *
import psycopg2
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


if __name__ == "__main__":

	processing()
	execute()