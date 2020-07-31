import datetime
from create import *
import psycopg2
import sys
import json
import getpass
from Process import *

def vpnProcessing(username, password):
        conn = getConnection(username, password)
        cursor = conn.cursor()
        cursor.execute("SELECT payload from incoming;")
        incoming_data = []
        for row in cursor.fetchall():
                incoming_data.append(row[0])
        conn.close()
        return incoming_data

def vpnexecute(username, password):
        incoming_data = vpnProcessing(username, password)
        for data in incoming_data:
                table_name = data['name'].lower()
                current_tables = showAllTablesODIN(False, username, password)
                if ('vpn' in table_name) and not (table_name in current_tables):
                        var_dict = {}
                        for column in data:
                                if type(data[column]) == type({}):
                                    for attribute in data[column]:
                                        attribute = '"' + attribute + '"'
                                        var_dict[attribute] = 'Int'
                                else:
                                        var_dict[column] = 'varchar'
                        createTable(table_name, var_dict, username, password)
                  
                attribute_lst = showPSQLAttribute(table_name, username, password)
                for keys in data:
                    if keys not in attribute_lst and type(data[keys]) != dict:
                        alterTable(data['name'], keys, 'varchar', username, password)

                insertTableJson(data, username, password)
                
        if (incoming_data != []):
            archive(username, password)

def vpnJsonify(username, password, vpntype):
        if (vpntype == "vpn"):
            vpnlst = getVPNjson()
            conn = getConnection(username, password)
            cursor = conn.cursor()
            for blob in vpnlst:
                if type(blob) != type([]):
                        cursor.execute("INSERT INTO incoming (payload) VALUES ('%s')" % json.dumps(blob, indent=4))
            conn.commit()
        elif (vpntype == "cisco"):
            vpnlst = getCiscojson()
            conn = getConnection(username, password)                                                                                                                
            cursor = conn.cursor()
            for blob in vpnlst:
                if type(blob) != type([]):
                        cursor.execute("INSERT INTO incoming (payload) VALUES ('%s')" % json.dumps(blob, indent=4))
            conn.commit()
        elif (vpntype == "clientsummary"):
            vpnlst = getClientSummary()
            conn = getTestConnection(username)
            cursor = conn.cursor()
            cursor.execute("INSERT INTO incoming (payload) VALUES ('%s')" % json.dumps(vpnlst, indent=4))
            conn.commit()
        elif (vpntype == "disconnection"):
            vpnlst = getDisconnectionReason()
            conn = getTestConnection(username)
            cursor = conn.cursor()
            cursor.execute("INSERT INTO incoming (payload) VALUES ('%s')" % json.dumps(vpnlst, indent=4))
            conn.commit()
        elif (vpntype == "uniqueuser"):
            vpnlst = getUniqueUsers()
            conn = getConnection(username, password)
            cursor = conn.cursor()
            cursor.execute("INSERT INTO incoming (payload) VALUES ('%s')" % json.dumps(vpnlst, indent=4))
            conn.commit() 
        elif (vpntype == "15minblock"):
            vpnlst = get15minblock()
            conn = getTestConnection(username)
            cursor = conn.cursor()
            cursor.execute("INSERT INTO incoming (payload) VALUES ('%s')" % json.dumps (vpnlst, indent=4))
            conn.commit()
        elif (vpntype == "15minblockflags"):
            vpnlst = get15minblockflags(username)
            conn = getTestConnection(username)
            cursor = conn.cursor()
            for blob in vpnlst:
                if type(blob) != type([]):
                    cursor.execute("INSERT INTO incoming (payload) VALUES ('%s')" % json.dumps (blob, indent=4))
            conn.commit() 
        elif (vpntype == "disconnectionUserRequested"):
            vpnlst = getDisconnectionFlagsUserRequested()
            conn = getConnection(username, password)
            cursor = conn.cursor()
            for blob in vpnlst:
                if type(blob) != type([]):
                    cursor.execute("INSERT INTO incoming (payload) VALUES ('%s')" % json.dumps (blob, indent=4))
            conn.commit()
        elif (vpntype == "disconnectionIdleTimeout"):
            vpnlst = getDisconnectionFlagsIdleTimeout()
            conn = getConnection(username, password)
            cursor = conn.cursor()
            for blob in vpnlst:
                if type(blob) != type([]):
                    cursor.execute("INSERT INTO incoming (payload) VALUES ('%s')" % json.dumps (blob, indent=4))
            conn.commit()
        elif (vpntype == "disconnectionConnectionPreempted"):
            vpnlst = getDisconnectionFlagsConnectionPreempted()
            conn = getConnection(username, password)
            cursor = conn.cursor()
            for blob in vpnlst:
                if type(blob) != type([]):
                    cursor.execute("INSERT INTO incoming (payload) VALUES ('%s')" % json.dumps (blob, indent=4))
            conn.commit() 
        elif (vpntype == "disconnectionCertificateExpired"):
            vpnlst = getDisconnectionFlagsCertificateExpired()
            conn = getConnection(username, password)
            cursor = conn.cursor()
            for blob in vpnlst:
                if type(blob) != type([]):
                    cursor.execute("INSERT INTO incoming (payload) VALUES ('%s')" % json.dumps (blob, indent=4))
            conn.commit() 
        elif (vpntype == "getbytetransfer"):
            vpnlst = getByteTransfer()
            conn = getTestConnection(username)
            cursor = conn.cursor() 
            cursor.execute("INSERT INTO incoming (payload) VALUES ('%s')" % json.dumps (vpnlst, indent=4))
            conn.commit()
        elif (vpntype == "getGeoLocation"):
            vpnlst = getGeoLocation(username, password)
            print(vpnlst)
            #conn = getConnection(username, password)
            #cursor = conn.cursor()
            #for blob in vpnlst:
            #    if type(blob) != type([]):
            #        cursor.execute("INSERT INTO incoming (payload) VALUES ('%s')" % json.dumps (blob, indent=4))
            #conn.commit()
        """ 
        conn = getConnection(username, password)
        cursor = conn.cursor()
        for blob in vpnlst:
                if type(blob) != type([]):
                        cursor.execute("INSERT INTO incoming (payload) VALUES ('%s')" % json.dumps(blob, indent=4))
        conn.commit()
        """
if __name__ == "__main__":
    if (len(sys.argv) == 3):
        #getLocation(sys.argv[1], sys.argv[2], '142.1.170.75')
        vpnJsonify(sys.argv[1], sys.argv[2], "uniqueuser")
        vpnJsonify(sys.argv[1], sys.argv[2], "disconnectionUserRequested")
        vpnJsonify(sys.argv[1], sys.argv[2], "disconnectionIdleTimeout")
        vpnJsonify(sys.argv[1], sys.argv[2], "disconnectionCertificateExpired")
        vpnJsonify(sys.argv[1], sys.argv[2], "disconnectionConnectionPreempted")
        vpnJsonify(sys.argv[1], sys.argv[2], "vpn")
        vpnJsonify(sys.argv[1], sys.argv[2], "cisco")
        moveData(sys.argv[1], 'incoming', sys.argv[2])
        #vpnexecute(sys.argv[1], sys.argv[2]) 
        #createIncomingTrigger(sys.argv[1], sys.argv[2])
        #createArchiveTrigger(sys.argv[1], sys.argv[2])
    if (len(sys.argv) > 3):
        if ((sys.argv[3]) == 'restore'):
                moveData(sys.argv[1], 'archive', sys.argv[2])
                dropTable('grouper', sys.argv[1], sys.argv[2])
                dropTable('etoken', sys.argv[1], sys.argv[2]) 
                dropTable('vpn722055', sys.argv[1], sys.argv[2])
                dropTable('vpn113019', sys.argv[1], sys.argv[2])
                dropTable('vpnflag_connection_preempted', sys.argv[1], sys.argv[2])
                dropTable('vpnflag_idle_timeout', sys.argv[1], sys.argv[2])
                dropTable('vpnflag_user_requested', sys.argv[1], sys.argv[2])
                dropTable('vpngeolocation', sys.argv[1], sys.argv[2])
                dropTable('vpnuniqueuser', sys.argv[1], sys.argv[2])
                dropTable('vpnflag_certificate_expired', sys.argv[1], sys.argv[2])
