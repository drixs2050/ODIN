from datetime import datetime
import mysql.connector
import psycopg2
import sys

def archive():
	pass
	# TODO: create archive data bases
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
	print(incoming_data)
# TODO: insert all info parsed by json and archive info
if __name__ == "__main__":
	processing(sys.argv[1])
	execute(sys.argv[1])
