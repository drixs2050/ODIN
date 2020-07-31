import csv 
import datetime
from create import *
import psycopg2
import sys
import json
import getpass
from Process import *

def duoProcessing(username):
        conn = getTestConnection(username)
        cursor = conn.cursor()
        cursor.execute("SELECT payload from incoming;")
        incoming_data = []
        for row in cursor.fetchall():
                incoming_data.append(row[0])
        conn.close()
        return incoming_data

def duoExecute(username):
        incoming_data = duoProcessing(username)
        for data in incoming_data:
                table_name = data['name'].lower()
                current_tables = showAllTestTablesODIN(False, username)
                if ('duo' in table_name) and not (table_name in current_tables):
                        var_dict = {}
                        for column in data:
                                if type(data[column]) == type({}):
                                    for attribute in data[column]:
                                        attribute = '"' + attribute + '"'
                                        var_dict[attribute] = 'Int'
                                elif type(data[column]) == int:
                                    var_dict[column] = 'Int'
                                else:
                                        var_dict[column] = 'varchar'
                        createTestTable(table_name, var_dict, username)

                attribute_lst = showTestPSQLAttribute(table_name, username)
                for keys in data:
                    if keys not in attribute_lst and type(data[keys]) != dict:
                        alterTestTable(data['name'], keys, 'varchar', username)

                insertTestTableJson(data, username)

        #if (incoming_data != []):
        #    archive(username, password)

def duoJsonify(username, password, duotype, file1, file2):
        if (duotype == "archiveDuo"):
            duolst = archiveDuoLogs(file1, file2)
            conn = getConnection(username, password)
            cursor = conn.cursor()
            for blob in duolst:
                if type(blob) != type([]):
                        cursor.execute("INSERT INTO incoming (payload) VALUES ('%s')" % json.dumps(blob, indent=4))
            conn.commit()
        elif (duotype == "statsDuo"):
            duolst = statsDuoLogs(file1, file2)
            current_tables = showAllTablesODIN(False, username, password)
            if (duolst['name'].lower() in current_tables):
                find_str = "SELECT count(*) from {} where exists (select * from {} where ".format(duolst['name'], duolst['name'])
                for column in duolst:
                    column = """{} = '{}' and """.format(column, duolst[column])
                    find_str = find_str + column
                find_str = find_str.strip(" and") + ");"
                conn = getConnection(username, password)
                cursor = conn.cursor()
                cursor.execute(find_str)
                for row in cursor.fetchone():
                    count = row
                    if count == 0:
                        cursor.execute("INSERT INTO incoming (payload) VALUES ('%s')" % json.dumps(duolst, indent=4))
                conn.commit()
            else:
                conn = getConnection(username, password)
                cursor = conn.cursor()
                cursor.execute("INSERT INTO incoming (payload) VALUES ('%s')" % json.dumps (duolst, indent=4))
                conn.commit()
def archiveDuoLogs(file1, file2):
    with open(file1, mode='r') as infile:
        reader = csv.reader(infile)
        with open(file2, mode='w') as outfile:
            writer = csv.writer(outfile)
            duo_log_lst = []
            for rows in reader:
                duo_log_dict = {}
                duo_log_dict['name'] = 'duo_log_archive'
                duo_log_dict['time'] = rows[0]
                duo_log_dict['operation'] = rows[1]
                duo_log_lst.append(duo_log_dict)
    return duo_log_lst

def statsDuoLogs(file1, file2):
    with open(file1, mode='r') as infile:
        reader = csv.reader(infile)
        with open(file2, mode='w') as outfile:
            writer = csv.writer(outfile)
            duo_log_stats = {}
            duo_log_stats['name'] = 'duo_log_stats'
            for rows in reader:
                if rows[1] in duo_log_stats:
                    duo_log_stats[rows[1]] += 1
                else:
                    duo_log_stats[rows[1]] = 1
    return duo_log_stats

if __name__ == '__main__':
    #print(statsDuoLogs('duo-logs.csv', 'duo-logs-new.csv'))
    duoJsonify(sys.argv[1], sys.argv[2], "archiveDuo", 'duo-logs.csv', 'duo-logs-new.csv')
    duoJsonify(sys.argv[1], sys.argv[2], "statsDuo", 'duo-logs.csv', 'duo-logs-new.csv')
    moveData(sys.argv[1], 'incoming', sys.argv[2])    
    #duoExecute(sys.argv[1])
