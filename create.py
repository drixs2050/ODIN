import mysql.connector
import psycopg2
import datetime
import csv
import subprocess
from psycopg2 import OperationalError, errorcodes
inventory_on_20200510 = 157
baseline_date = datetime.date(2020, 5, 10)

def getConnection(username, password):
    conn = psycopg2.connect(host="is-sdt-srv01.is.utoronto.ca", user= username, dbname ="odin", password= password)   
    return conn

def getTestConnection(username):
    conn = psycopg2.connect(user=username, dbname ="odin")
    return conn

def getSQLConnection(username, pass_word):
    new_conn = mysql.connector.connect(user=username, password=pass_word, database='shibboleth')
    return new_conn


def getEtokenConnection(username, pass_word):
    new_conn = mysql.connector.connect(user=username, password=pass_word, database='etoken')
    return new_conn

def getVPNjson():
    ssh = subprocess.Popen(["grep", "113019", "/var/log/cisco"], stdin = subprocess.PIPE, stdout= subprocess.PIPE, stderr = subprocess.PIPE, universal_newlines=True, bufsize =0)

    jsons = []
    for line in ssh.stdout:
        period = line.split('.')
        first = [period[0] +'.'+ period[1]+'.' + period[2]+ '.' + period[3], period[4]]
        second = first[0].split(',') + first[1].split(',')
        stripped = [i.strip() for i in second]
        json = {}
        try:
            json['name'] = "vpn113019"
            json['date'] = stripped[0][0:6].strip()
            json['service_version'] = stripped[0][23:36].strip()
            json['utorid'] = stripped[1][11:].strip()
            json['time'] = stripped[0][7:15].strip()
            json['service'] = stripped[0][16:21].strip()
            json['category']= stripped[0][46:].strip()
            json['ip'] = stripped[2][5:].strip()
            json['session_type'] = stripped[4][14:].strip()
            json['duration'] = stripped[5][10:].strip()
            json['bytes_xmt'] = stripped[6][11:].strip()
            json['bytes_rcv'] = stripped[7][11:].strip()
            json['reason'] = stripped[8][8:].strip()
            jsons.append(json)
        except IndexError:
            print('special case')
            print(stripped)
    return jsons
def getCiscojson():
    ssh = subprocess.Popen(["grep", "722055", "/var/log/cisco"], stdin = subprocess.PIPE, stdout = subprocess.PIPE, stderr = subprocess.PIPE, universal_newlines=True, bufsize = 0)
    jsons = []
    for line in ssh.stdout:
        list = line.split('<')
        json = {}
        try:
            first = list[0].split(' ')
            json['name'] = "vpn722055"
            json['date'] = first[0].strip() + ' '+first[1].strip()
            json['time'] = first[2].strip()
            json['port'] = first[3].strip()
            json['service_version'] = list[0].split(':')[3].strip()
            json['category'] = list[1].split('>')[0].strip()
            json['utorid'] = list[2].split('>')[0].strip()
            json['ip'] = list[3].split('>')[0].strip()
            json['service_type'] = list[3].split(':')[1].strip()
            jsons.append(json)
        except IndexError:
            print('special case')

    return jsons
def getClientSummary():
    ssh = subprocess.Popen(["grep", "722055", "/var/log/cisco"], stdin = subprocess.PIPE, stdout= subprocess.PIPE, stderr = subprocess.PIPE, universal_newlines=True, bufsize =0)
    stats = {}
    stats['name'] = 'vpnclient_summary'
    stats['run_date'] = datetime.datetime.now().strftime("%Y-%m-%d")
    stats['run_time'] = datetime.datetime.now().strftime("%H:%M:%S.%f")[:-3]
    stats['total_connection'] = 0
    stats['service_type'] = {}
    for line in ssh.stdout:
        list = line.split('<')
        first = list[0].split(' ')
        try:
            stats['total_connection'] += 1
            if ''.join(list[3].split(':')[1].strip().split()[:-1]) not in stats['service_type']:
                stats['service_type'][''.join(list[3].split(':')[1].strip().split()[:-1])] = 1
            else:
                stats['service_type'][''.join(list[3].split(':')[1].strip().split()[:-1])] += 1
        except IndexError:
            continue                                                                                                   
    return stats

def getDisconnectionFlagsUserRequested():
    ssh = subprocess.Popen(["grep", "113019", "/var/log/cisco"], stdin = subprocess.PIPE, stdout = subprocess.PIPE, stderr = subprocess.PIPE, universal_newlines=True, bufsize = 0)
    jsons = []
    for line in ssh.stdout:
        stats = {}
        stats['name'] = 'vpnflag_user_requested'
        stats['run_date'] = datetime.datetime.now().strftime("%Y-%m-%d")
        stats['run_time'] = datetime.datetime.now().strftime("%H:%M:%S.%f")[:-3]
        period = line.split('.')
        first = [period[0] + '.' + period[1] + '.' + period[2] + '.' +  period[3], period[4]]
        second = first[0].split(',') + first[1].split(',')
        stripped = [i.strip() for i in second]
        
        try:
            if (stripped[8][8:].strip() == "User Requested"):
                user = stripped[1][stripped[1].find("=")+2:]
                stats['utorid'] = user
                flags = subprocess.Popen(["/opt/local/bin/getuserinfo", "--utorid={}".format(user)], stdin = subprocess.PIPE, stdout = subprocess.PIPE, stderr = subprocess.PIPE, universal_newlines=True, bufsize =0)
                for line in flags.stdout:
                    if "isstaff:" in line:
                        stats['isstaff'] = line[line.find(":")+1: ].strip()
                    if "isstudent:" in line:
                        stats['isstudent'] = line[line.find(":")+1:].strip()
                    if "isfaculty:" in line:
                        stats['isfaculty'] = line[line.find(":")+1:].strip()
                jsons.append(stats)
        except (IndexError, ValueError):
            continue
    return jsons

def getDisconnectionFlagsIdleTimeout():
    ssh = subprocess.Popen(["grep", "113019", "/var/log/cisco"], stdin = subprocess.PIPE, stdout = subprocess.PIPE, stderr = subprocess.PIPE, universal_newlines=True, bufsize = 0)
    jsons = []
    for line in ssh.stdout:
        stats = {}
        stats['name'] = 'vpnflag_idle_timeout'
        stats['run_date'] = datetime.datetime.now().strftime("%Y-%m-%d")
        stats['run_time'] = datetime.datetime.now().strftime("%H:%M:%S.%f")[:-3]
        period = line.split('.')
        first = [period[0] + '.' + period[1] + '.' + period[2] + '.' +  period[3], period[4]]
        second = first[0].split(',') + first[1].split(',')
        stripped = [i.strip() for i in second]

        try:
            if (stripped[8][8:].strip() == "Idle Timeout"):
                user = stripped[1][stripped[1].find("=")+2:]
                stats['utorid'] = user
                flags = subprocess.Popen(["/opt/local/bin/getuserinfo", "--utorid={}".format(user)], stdin = subprocess.PIPE, stdout = subprocess.PIPE, stderr = subprocess.PIPE, universal_newlines=True, bufsize =0)
                for line in flags.stdout:
                    if "isstaff:" in line:
                        stats['isstaff'] = line[line.find(":")+1: ].strip()
                    if "isstudent:" in line:
                        stats['isstudent'] = line[line.find(":")+1:].strip()
                    if "isfaculty:" in line:
                        stats['isfaculty'] = line[line.find(":")+1:].strip()
                jsons.append(stats)
        except (IndexError, ValueError):
            continue
    return jsons

def getDisconnectionFlagsConnectionPreempted():
    ssh = subprocess.Popen(["grep", "113019", "/var/log/cisco"], stdin = subprocess.PIPE, stdout = subprocess.PIPE, stderr = subprocess.PIPE, universal_newlines=True, bufsize = 0)
    jsons = []
    for line in ssh.stdout:
        stats = {}
        stats['name'] = 'vpnflag_connection_preempted'
        stats['run_date'] = datetime.datetime.now().strftime("%Y-%m-%d")
        stats['run_time'] = datetime.datetime.now().strftime("%H:%M:%S.%f")[:-3]
        period = line.split('.')
        first = [period[0] + '.' + period[1] + '.' + period[2] + '.' +  period[3], period[4]]
        second = first[0].split(',') + first[1].split(',')
        stripped = [i.strip() for i in second]

        try:
            if (stripped[8][8:].strip() == "Connection Preempted"):
                user = stripped[1][stripped[1].find("=")+2:]
                stats['utorid'] = user
                flags = subprocess.Popen(["/opt/local/bin/getuserinfo", "--utorid={}".format(user)], stdin = subprocess.PIPE, stdout = subprocess.PIPE, stderr = subprocess.PIPE, universal_newlines=True, bufsize =0)
                for line in flags.stdout:
                    if "isstaff:" in line:
                        stats['isstaff'] = line[line.find(":")+1: ].strip()
                    if "isstudent:" in line:
                        stats['isstudent'] = line[line.find(":")+1:].strip()
                    if "isfaculty:" in line:
                        stats['isfaculty'] = line[line.find(":")+1:].strip()
                jsons.append(stats)
        except (IndexError, ValueError):
            continue
    return jsons

def getDisconnectionFlagsCertificateExpired():
    ssh = subprocess.Popen(["grep", "113019", "/var/log/cisco"], stdin = subprocess.PIPE, stdout = subprocess.PIPE, stderr = subprocess.PIPE, universal_newlines=True, bufsize = 0)
    jsons = []
    for line in ssh.stdout:
        stats = {}
        stats['name'] = 'vpnflag_certificate_expired'
        stats['run_date'] = datetime.datetime.now().strftime("%Y-%m-%d")
        stats['run_time'] = datetime.datetime.now().strftime("%H:%M:%S.%f")[:-3]
        period = line.split('.')
        first = [period[0] + '.' + period[1] + '.' + period[2] + '.' +  period[3], period[4]]
        second = first[0].split(',') + first[1].split(',')
        stripped = [i.strip() for i in second]

        try:
            if (stripped[8][8:].strip() == "Certificate Expired"):
                user = stripped[1][stripped[1].find("=")+2:]
                stats['utorid'] = user
                flags = subprocess.Popen(["/opt/local/bin/getuserinfo", "--utorid={}".format(user)], stdin = subprocess.PIPE, stdout = subprocess.PIPE, stderr = subprocess.PIPE, universal_newlines=True, bufsize =0)
                for line in flags.stdout:
                    if "isstaff:" in line:
                        stats['isstaff'] = line[line.find(":")+1: ].strip()
                    if "isstudent:" in line:
                        stats['isstudent'] = line[line.find(":")+1:].strip()
                    if "isfaculty:" in line:
                        stats['isfaculty'] = line[line.find(":")+1:].strip()
                jsons.append(stats)
        except (IndexError, ValueError):
            continue
    return jsons
                 
def getDisconnectionReason():
    ssh = subprocess.Popen(["grep", "113019", "/var/log/cisco"], stdin = subprocess.PIPE, stdout= subprocess.PIPE, stderr = subprocess.PIPE, universal_newlines=True, bufsize =0)
    stats = {}
    stats['name'] = 'vpndisconnection'
    stats['run_date'] = datetime.datetime.now().strftime("%Y-%m-%d")
    stats['run_time'] = datetime.datetime.now().strftime("%H:%M:%S.%f")[:-3]
    #stats['total_bytes_xmt'] = 0
    #stats['total_bytes_rcv'] = 0
    #stats['total_duration'] = 0
    #stats['total_connection'] = 0
    stats['disconnection_reasons'] = {}
    stats['groups'] = {}
    #stats['session_types'] = {}
    for line in ssh.stdout:
        period = line.split('.')
        first = [period[0] +'.'+ period[1]+'.' + period[2]+ '.' + period[3], period[4]]
        second = first[0].split(',') + first[1].split(',')
        stripped = [i.strip() for i in second]
        try:
            #stats['total_bytes_xmt'] += int(stripped[6][11:].strip())
            #stats['total_bytes_rcv'] += int(stripped[7][11:].strip())
            #stats['total_connection'] += 1
            if stripped[8][8:].strip() not in stats['disconnection_reasons']:
                stats['disconnection_reasons'][stripped[8][8:].strip()] = 1
            else:
                stats['disconnection_reasons'][stripped[8][8:].strip()] += 1
            #if stripped[4][14:].strip() not in stats['session_types']:
            #    stats['session_types'][stripped[4][14:].strip()] = 1
            #else:
            #    stats['session_types'][stripped[4][14:].strip()] += 1
            if stripped[0][46:].strip() not in stats['groups']:
                stats['groups'][stripped[0][46:].strip()] = 1
            else:
                stats['groups'][stripped[0][46:].strip()] += 1
            #reversed = stripped[5][10:].strip()[::-1]
            #seconds = int(reversed[1:3][::-1].strip())
            #minutes = int(reversed[5:7][::-1].strip())
            #rest = reversed[9:].strip()
            #days = 0
            #if 'd' not in rest:
            #    hours = int(rest)
            #else:
            #    hours = int(rest.split('d')[0].strip()[::-1])
            #    days = int(rest.split('d')[1].strip()[::-1])
            #stats['total_duration'] += (seconds + minutes * 60 + hours * 3600 + days * 86400)
        except (IndexError, ValueError):
            #print('special case')
            #print(stripped)
            continue
    #stats['total_duration'] = convert(stats['total_duration'])
    return stats
def convert(n):
    return str(datetime.timedelta(seconds = n))
def getByteTransfer():
    ssh = subprocess.Popen(["grep", "113019", "/var/log/cisco"], stdin = subprocess.PIPE, stdout= subprocess.PIPE, stderr = subprocess.PIPE, universal_newlines=True, bufsize =0)
    stats = {}
    stats['name'] = 'vpn_data_traffic'
    stats['run_date'] = datetime.datetime.now().strftime("%Y-%m-%d")
    stats['run_time'] = datetime.datetime.now().strftime("%H:%M:%S.%f")[:-3]
    stats['total_bytes_xmt'] = 0
    stats['total_bytes_rcv'] = 0
    stats['total_duration'] = 0
    stats['total_connection'] = 0
    for line in ssh.stdout:
        period = line.split('.')
        first = [period[0] +'.'+ period[1]+'.' + period[2]+ '.' + period[3], period[4]]
        second = first[0].split(',') + first[1].split(',')
        stripped = [i.strip() for i in second]
        try:
            stats['total_bytes_xmt'] += int(stripped[6][11:].strip())
            stats['total_bytes_rcv'] += int(stripped[7][11:].strip())
            stats['total_connection'] += 1
            reversed = stripped[5][10:].strip()[::-1]
            seconds = int(reversed[1:3][::-1].strip())
            minutes = int(reversed[5:7][::-1].strip())
            rest = reversed[9:].strip()
            days = 0
            if 'd' not in rest:
                hours = int(rest)
            else:
                hours = int(rest.split('d')[0].strip()[::-1])
                days = int(rest.split('d')[1].strip()[::-1])
            stats['total_duration'] += (seconds + minutes * 60 + hours * 3600 + days * 86400)
        except (IndexError, ValueError):
            #print('special case')
            #print(stripped)
            continue
    stats['total_duration'] = convert(stats['total_duration'])
    return stats
     
def getUniqueUsers():
    ssh = subprocess.Popen(["grep", "113019", "/var/log/cisco"], stdin = subprocess.PIPE, stdout= subprocess.PIPE, stderr = subprocess.PIPE, universal_newlines=True, bufsize =0)
    stats = {}
    stats['name'] = 'vpnUniqueUser'
    stats['run_date'] = datetime.datetime.now().strftime("%Y-%m-%d")
    stats['run_time'] = datetime.datetime.now().strftime("%H:%M:%S.%f")[:-3]
    stats['unique_user'] = 0
    userlst = []
    for line in ssh.stdout:
        period = line.split('.')
        first = [period[0] +'.'+ period[1]+'.' + period[2]+ '.' + period[3], period[4]]
        second = first[0].split(',') + first[1].split(',')
        stripped = [i.strip() for i in second]
        try:
            if "Username" in stripped[1]:
                user = stripped[1][11:].strip()
                if (user not in userlst or user == '025sql04'):
                    userlst.append(user)
        except (IndexError, ValueError):
            continue
    stats['unique_user'] = len(userlst)
    return stats
def getLocation(username, password, ip_address):
    conn = getConnection(username, password)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM geoip WHERE network >> '{}'".format(ip_address))
    location = {}
    for i in cursor.fetchall():
         location['city'] = i[22]
         location['province'] = i[19]
         location['country'] = i[17]
    return location
def getcountryLocation(username, password, ip_address):
    conn = getConnection(username, password)
    cursor = conn.cursor()
    cursor.execute("SELECT country_name FROM geoip WHERE network >> '{}'".format(ip_address))
    for i in cursor.fetchall():
        return i[0]

def getprovinceLocation(username, password, ip_address):
    conn = getConnection(username, password)
    cursor = conn.cursor()
    cursor.execute("SELECT subdivision_1_name FROM geoip WHERE network >> '{}'".format(ip_address))
    for i in cursor.fetchall():
        return i[0] 
def getGeoLocation(username, password):
    ssh = subprocess.Popen(["grep", "113019", "/var/log/cisco"], stdin = subprocess.PIPE, stdout= subprocess.PIPE, stderr = subprocess.PIPE, universal_newlines=True, bufsize =0)
    jsons = []
    for line in ssh.stdout:
        stats = {}
        stats['name'] = 'vpngeolocation'
        stats['run_date'] = datetime.datetime.now().strftime("%Y-%m-%d")
        stats['run_time'] = datetime.datetime.now().strftime("%H:%M:%S.%f")[:-3]
        period = line.split('.')
        first = [period[0] + '.' + period[1] + '.' + period[2] + '.' + period[3], period[4]]
        second = first[0].split(',') + first[1].split(',')
        stripped = [i.strip() for i in second]
        try:
            user = stripped[1][11:].strip()
            ip_address = stripped[2][5:].strip()
            stats['utorid'] = user
            if (ip_address != ''):
                location = getLocation(username, password,ip_address)
                if location['city'].find("'") != -1:
                    city = location['city'].replace("'", "''")
                else:
                    stats['city'] = location['city']
                stats['country']= location['country']
                stats['province'] = location['province']
                jsons.append(stats)

            
        except (IndexError, ValueError, psycopg2.DataError, KeyError, AttributeError):
            continue
        
    return jsons
       
def checkSameDate15MinBlock(username,password):
    current_tables = showAllTablesODIN(False, username, password)
    if ('vpn15minblock_flags' in current_tables):
        conn = getConnection(username, password)
        cursor = conn.cursor()
        cursor.execute("SELECT count (*) from vpn15minblock_flags where run_date = '{}'".format(datetime.datetime.now().strftime("%Y-%m-%d")))
        for row in cursor.fetchall():
            return row[0]
        
    
def get15minblockflags(username, password):
    ssh = subprocess.Popen(["grep", "113019", "/var/log/cisco"], stdin = subprocess.PIPE, stdout= subprocess.PIPE, stderr = subprocess.PIPE, universal_newlines=True, bufsize =0)
    jsons = []
    count = checkSameDate15MinBlock(username, password) 
    if count == 0 or count == None:
        for line in ssh.stdout:
            stats = {}
            stats['name'] = 'vpn15minblock_flags'
            stats['run_date'] = datetime.datetime.now().strftime("%Y-%m-%d")
            stats['run_time'] = datetime.datetime.now().strftime("%H:%M:%S.%f")[:-3] 
            period = line.split('.')
            first = [period[0] + '.' + period[1] + '.' + period[2] + '.' + period[3], period[4]]
            second = first[0].split(',') + first[1].split(',')
            stripped = [i.strip() for i in second]
            try:
                user = stripped[1][11:].strip()
                ip_address = stripped[2][5:].strip()
                stats['utorid'] = user
                stats['bytes_xmt'] = stripped[6][10:].strip()
                stats['bytes_rcv'] = stripped[7][10:].strip()
                flags = subprocess.Popen(["/opt/local/bin/getuserinfo", "--utorid={}".format(user)], stdin = subprocess.PIPE, stdout = subprocess.PIPE, stderr = subprocess.PIPE, universal_newlines=True, bufsize =0)
                for line in flags.stdout:
                    if "isstaff:" in line:
                        stats['isstaff'] = line[line.find(":")+1: ].strip()
                    if "isstudent:" in line:
                        stats['isstudent'] = line[line.find(":")+1:].strip()
                    if "isfaculty:" in line:
                        stats['isfaculty'] = line[line.find(":")+1:].strip()
                if ip_address != '':
                    location = getLocation(username, password, ip_address)
                    if location['city'].find("'") != -1:
                        stats['city'] = location['city'].replace("'", "''")
                    else:
                        stats['city'] = location['city']
                    stats['country'] = location['country']
                    stats['province'] = location['province']
                    jsons.append(stats)
            except (IndexError, ValueError, psycopg2.DataError, KeyError, AttributeError):
                continue
    else:
         i = 0
         for line in ssh.stdout:
             i += 1
             if i > count:
                stats = {}
                stats['name'] = 'vpn15minblock_flags'
                stats['run_date'] = datetime.datetime.now().strftime("%Y-%m-%d")
                stats['run_time'] = datetime.datetime.now().strftime("%H:%M:%S.%f")[:-3]
                period = line.split('.')
                first = [period[0] + '.' + period[1] + '.' + period[2] + '.' + period[3], period[4]]
                second = first[0].split(',') + first[1].split(',')
                stripped = [i.strip() for i in second]
                try:
                    user = stripped[1][11:].strip()
                    ip_address = stripped[2][5:].strip()
                    stats['utorid'] = user
                    stats['bytes_xmt'] = stripped[6][10:].strip()
                    stats['bytes_rcv'] = stripped[7][10:].strip()
                    flags = subprocess.Popen(["/opt/local/bin/getuserinfo", "--utorid={}".format(user)], stdin = subprocess.PIPE, stdout = subprocess.PIPE, stderr = subprocess.PIPE, universal_newlines=True, bufsize =0)
                    for line in flags.stdout:
                        if "isstaff:" in line:
                            stats['isstaff'] = line[line.find(":")+1: ].strip()
                        if "isstudent:" in line:
                            stats['isstudent'] = line[line.find(":")+1:].strip()
                        if "isfaculty:" in line:
                            stats['isfaculty'] = line[line.find(":")+1:].strip()
                    if ip_address != '':
                        location = getLocation(username, password, ip_address)
                        if location['city'].find("'") != -1:
                            stats['city'] = location['city'].replace("'", "''")
                        else:
                            stats['city'] = location['city']
                        stats['country'] = location['country']
                        stats['province'] = location['province']
                        jsons.append(stats)
                except (IndexError, ValueError, psycopg2.DataError, KeyError, AttributeError):
                    continue
    return jsons 
def get15minblock():
    ssh = subprocess.Popen(["grep", "113019", "/var/log/cisco"], stdin = subprocess.PIPE, stdout= subprocess.PIPE, stderr = subprocess.PIPE, universal_newlines=True, bufsize =0)
    stats = {}
    stats['name'] = 'vpn15minblock'
    stats['run_date'] = datetime.datetime.now().strftime("%Y-%m-%d")
    stats['run_time'] = datetime.datetime.now().strftime("%H:%M:%S.%f")[:-3]
    stats['user_count'] = 0
    stats['total_bytes_xmt'] = 0
    stats['total_bytes_rcv'] = 0
    userlst = []
    for line in ssh.stdout:
        period = line.split('.')
        first = [period[0] + '.' + period[1] + '.' + period[2] + '.' + period[3], period[4]]
        second = first[0].split(',') + first[1].split(',')
        stripped = [i.strip() for i in second]
        try:
            stats['total_bytes_xmt'] += int(stripped[6][11:].strip())
            stats['total_bytes_rcv'] += int(stripped[7][11:].strip())
            if "Username" in stripped[1]:
                user = stripped[1][11:].strip()
                userlst.append(user)
        except (IndexError, ValueError):
            continue
    stats['user_count'] = len(userlst)
    return stats
 
def showSQLAttribute(username, pass_word):
    conn = getSQLConnection(username, pass_word)
    cursor = conn.cursor()
    cursor.execute("Describe splist")
    # cursor.execute("SELECT * FROM splist")
    print("\nService Provider Attributes:")
    i = 0
    for row in cursor.fetchall():
        print("[{}]: {}".format(i, row[0]))
        i = i + 1
    conn.close()
    return i

def showTestPSQLAttribute(table, username):
    conn = getTestConnection(username)
    cursor = conn.cursor()
    cursor.execute("SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{}';".format(table))
    list = []
    for i in cursor.fetchall():
        list.append(i[0])
    conn.close()
    return list

def showPSQLAttribute(table, username, password):
    conn = getConnection(username, password)
    cursor = conn.cursor()
    cursor.execute("SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{}';".format(table))
    list = []
    for i in cursor.fetchall():
        list.append(i[0])
    conn.close()
    return list


def selectOneAttribute(username, pass_word, index):
    conn = getSQLConnection(username, pass_word)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM splist")
    for row in cursor.fetchall():
        print(row[index])
    conn.close()


def checkValidSP(username, pass_word, sp):
    conn = getSQLConnection(username, pass_word)
    cursor = conn.cursor()
    cursor.execute("SELECT entityID from splist where entityID = '{}'".format(sp))
    if (cursor.fetchone() == None):
        return -1
    return 0


def checkSP(username, pass_word, sp):
    conn = getSQLConnection(username, pass_word)
    cursor = conn.cursor()
    validSPCheck = checkValidSP(username, pass_word, sp)
    if (validSPCheck == -1):
        conn.close()
        print("\nThe service provider does not exist")
        return -1
    cursor.execute("SELECT contactpname, contactemail FROM splist where entityID = '{}'".format(sp))
    for row in cursor.fetchall():
        print("\nAdmin: {}, Contact: {}".format(row[0], row[1]))
    conn.close()


def searchInOneAttribute(username, pass_word, attribute, key_word):
    conn = getSQLConnection(username, pass_word)
    cursor = conn.cursor()
    searchfor = '%' + key_word + '%'
    print("SELECT * FROM splist WHERE " + attribute + " LIKE " + "'" + searchfor + "'")
    cursor.execute("SELECT * FROM splist WHERE " + attribute + " LIKE " + "'" + searchfor + "'")
    print(cursor.fetchall())
    conn.close()

def showAllTestTablesODIN(print_boolean, username):
    try:
        conn = getTestConnection(username)
        cursor = conn.cursor()
        query = """Select a."Name" from (
SELECT n.nspname as "Schema",
  c.relname as "Name",
  CASE c.relkind WHEN 'r' THEN 'table' WHEN 'v' THEN 'view' WHEN 'i' THEN 'index' WHEN 'S' THEN 'sequence' WHEN 's' THEN 'special' WHEN 'f' THEN 'foreign table' END as "Type",
  pg_catalog.pg_get_userbyid(c.relowner) as "Owner"
FROM pg_catalog.pg_class c
     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind IN ('r','v','S','f','')
      AND n.nspname <> 'pg_catalog'
      AND n.nspname <> 'information_schema'
      AND n.nspname !~ '^pg_toast'
  AND pg_catalog.pg_table_is_visible(c.oid)
ORDER BY 1,2) a"""
        cursor.execute(query)
        table_lst = []
        for row in cursor.fetchall():
            if (print_boolean == True):
                print(row[0])
            else:
                table_lst.append(row[0])
    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)
    finally:
        if (conn):
            cursor.close()
            conn.close()
            if (print_boolean == False):
                return table_lst

def showAllTablesODIN(print_boolean, username, password):
    try:
        conn = getConnection(username, password)
        cursor = conn.cursor()
        query = """Select a."Name" from (
SELECT n.nspname as "Schema",
  c.relname as "Name",
  CASE c.relkind WHEN 'r' THEN 'table' WHEN 'v' THEN 'view' WHEN 'i' THEN 'index' WHEN 'S' THEN 'sequence' WHEN 's' THEN 'special' WHEN 'f' THEN 'foreign table' END as "Type",
  pg_catalog.pg_get_userbyid(c.relowner) as "Owner"
FROM pg_catalog.pg_class c
     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind IN ('r','v','S','f','')
      AND n.nspname <> 'pg_catalog'
      AND n.nspname <> 'information_schema'
      AND n.nspname !~ '^pg_toast'
  AND pg_catalog.pg_table_is_visible(c.oid)
ORDER BY 1,2) a"""
        cursor.execute(query)
        table_lst = []
        for row in cursor.fetchall():
            if (print_boolean == True):
                print(row[0])
            else:
                table_lst.append(row[0])
    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)
    finally:
        if (conn):
            cursor.close()
            conn.close()
            if (print_boolean == False):
                return table_lst


def showAllAttributes(username, table_name, password):
    conn = getConnection(username, password)
    cursor = conn.cursor()
    query = cursor.execute("SELECT * FROM {};".format(table_name))
    for row in cursor.fetchall():
        cursor.close()
        conn.close()
        return row


def addComment(table_name, comment, username, password):
    try:
        conn = getConnection(username, password)
        cursor = conn.cursor()
        cursor.execute("COMMENT ON TABLE {} IS '{}';".format(table_name, comment))
        conn.commit()
    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)
    finally:
        if (conn):
            cursor.close()
            conn.close()

def commitTestQuery(query, output, username):
    try:
        conn = getTestConnection(username)
        cursor = conn.cursor()
        cursor.execute("SELECT version();")
        record = cursor.fetchone()
        cursor.execute(query)
        conn.commit()
    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)
    finally:
        if (conn):
            cursor.close()
            conn.close()
def commitQuery(query, output, username, password):
    """
    Prints out a successful statement if query worked. Else, print a error statement.

    Parameters
    ----------
    query: str
      This is a query in the form of the string. Ex. "SELECT * FROM hello"
    output: str
      This string is the expected output that is printed depending on the query.
    """
    try:
        conn = getConnection(username, password)
        cursor = conn.cursor()
        cursor.execute("SELECT version();")
        record = cursor.fetchone()
        cursor.execute(query)
        conn.commit()
    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)
    finally:
        if (conn):
            cursor.close()
            conn.close()


"""
Alter, add and remove, you don't want to update (change the data)
"""

def createTestTable(table_name, var_dict, username):
    commitTestQuery(createTableQuery(table_name, var_dict), "Table created successfully in PostgreSQL", username)
    commitTestQuery("Alter table {} OWNER TO odin;".format(table_name), "Table created successfully in PostgreSQL", username)

def createTable(table_name, var_dict, username, password):
    """
    Creates a new table in the odin database.

    SHOULD IMPLEMENT: should delete the table if it already exists.

    Parameters
    ----------
    table_name: string
      The name of the table that is to be created.

    Returns
    -------
    Returns a message saying Table successfully created
    or error statement if syntax error.
    """
    commitQuery(createTableQuery(table_name, var_dict), "Table created successfully in PostgreSQL", username, password)
    commitQuery("Alter table {} OWNER TO odin;".format(table_name), "Table created successfully in PostgreSQL",
                username, password)


def insertTableJson(json, username, password):
    """
    Inserts a json blobs that are to be added into the odin database.
    """
    commitQuery(insertTableJsonQuery(json), "Table inserted successfully in PostgreSQL", username, password)

def insertTestTableJson(json, username):
    commitTestQuery(insertTableJsonQuery(json), "Table inserted successfully in PostgreSQL", username)


def insertTable(json, username, password):
    """
    Inserts a list of values into the columns in the odin database.

    SHOULD IMPLEMENT: should give error if either the columnlst or valuelst is not inside the table.

    Parameters
    ----------
    table_name: string
      The name of the table that information should be inserted into.
    attr_dict: dict
      The dictionary where the keys at the column_name and values are the values of the column_name.
    Returns
    -------
    Returns a message saying Table successfully inserted
    or error statement if syntax error.

    """
    commitQuery(insertTableQuery(json), "Table inserted successfully in PostgreSQL", username, password)


def alterTable(table_name, column_name, column_type, username, password):
    """
    Adds one single column into the existing table.

    Parameters
    ----------
    table_name: string
      The name of the table where the column is to be added.
    attr_dict: dict
      A dictionary where the keys are the column name and value is the type of the column.
      The dictionary cannot have a length greater than 1.

    Returns
    -------
    Returns a message saying Table successfully altered
    or error statement if syntax error.
    """
    commitQuery(alterTableQuery(table_name, column_name, column_type), "Table altered successfully in PostgreSQL",
                username, password)

def alterTestTable(table_name, column_name, column_type, username):
    commitTestQuery(alterTableQuery(table_name, column_name, column_type), "Table altered successfully in PostgreSQL",
                username)

def updateTable(table_name, attr_dict, password, condition=None):
    """
    Updates the table_name by the given attr_dict. If the condition is given,
    then updates only at the specific condition.

    Parameters
    ----------
    table_name: string
      The name of the table where the information is to be updated.
    attr_dict: dict
      A dictionary where the key is the column name and
                value is the value that is to be updated to.
    *condition: string
      A string that should be in the format "WHERE .....". This condition
      statement is optional, if this is empty, updates all the columns that are given.

    Returns
    -------
    Returns a message saying Table successfully updated
    or error statement if syntax error.
    """
    commitQuery(updateTableQuery(table_name, attr_dict, *condition), "Table updated successfully in PostgreSQL", password)


def deleteTable(table_name, condition, password):
    """
    Deletes the rows of the specific condition.

    Parameters
    ----------
    table_name: string
      The name of the table in the odin database.
    condition: string
      A string in the format "WHERE ...." The specific condition where
      the rows should be deleted.

    Returns
    -------
    Returns a message saying Table successfully deleted
    or error statement if syntax error.
    """
    commitQuery("DELETE FROM {} {};".format(table_name, condition), "Table deletected successfully in PostgreSQL", password)


def dropTable(table_name, username, password):
    commitQuery("Drop table {};".format(table_name), "Table dropped successfully in PostgreSQL", username, password)


def selectTable(table_name, password, variable=None, condition=None):
    """
    Selects the table to read, if the columnlst is not defined, then reads the entire table,
    otherwise, reads the specific columns given.

    Parameters
    ----------
    table_name : string
      The name of the table that is to be read.
    *columnlst : list
      Optional list of columns that is to be read. If the columnlst is not defined, reads the
      entire table.

    Returns
    -------
    Returns a message saying Table successfully read
    or error statement if syntax error.
    """
    commitQuery(selectTableQuery(table_name, variable, condition), "Table read successfully in PostgreSQL", password)


def selectTableQuery(table_name, variable=None, condition=None):
    """
    This is a helper function that returns the SELECT statement
    """
    if (variable != None and condition == None):
        new_query = "SELECT {} FROM {}".format(variable, table_name)

    elif (variable != None and condition != None):
        new_query = "SELECT {} FROM {} WHERE {}".format(variable, table_name, condition)
    else:
        new_query = "SELECT * FROM {}".format(table_name)
    return new_query


def createTableQuery(table_name, attr_dict):
    """
    This is a helper function that returns the CREATE TABLE statement
    """
    data_str = ''
    for keys in attr_dict:
        statement = keys + ' ' + attr_dict[keys] + ', '
        data_str = data_str + statement
    data_statement = data_str.strip(', ')
    new_query = "CREATE TABLE {} ({});".format(table_name, data_statement)
    return new_query


def insertTableJsonQuery(json):
    """
    This is a helper function that returns the INSERT TABLE statement
    """
    column = ""
    value = ""
    if (json["name"].lower() == 'grouper'):
        columnNotDict = []
        valueNotDict = []
        for column_name in json:
            if (type(json[column_name]) != dict):
                if (column_name not in columnNotDict):
                    columnNotDict.append(column_name)
                valueNotDict.append(json[column_name])

        for column_name in json:
            if (type(json[column_name]) == dict):
                columnDict = ['stemname', 'numstems']
                for stem in json[column_name]:
                    combinedValue = valueNotDict[:]
                    combinedValue.append(stem)
                    combinedValue.append(json[column_name][stem])
                    value = value + str(tuple(combinedValue)) + ','

        columnNotDict += columnDict
        column = str(tuple(columnNotDict))
        column = column.replace("'", "")
        value = value.strip(", ")
        table_name = json["name"]
        insert_query = "INSERT INTO {} {} VALUES {};".format(table_name, column, value)
        return insert_query
    elif (json["name"].lower() == "archive"):
        columnNotDict = []
        valueNotDict = []
        for column_name in json:
            if (column_name != "name"):
                if (column_name not in columnNotDict):
                    columnNotDict.append(column_name)
                if (type(json[column_name]) == tuple):
                    str_dict = str(json[column_name])
                    str_dict = str_dict.replace("'", "")
                    valueNotDict.append(str_dict)
                else:
                    valueNotDict.append(json[column_name])
        value = value + str(tuple(valueNotDict)) + ','
        column = str(tuple(columnNotDict))
        column = column.replace("'", "")
        value = value.strip(", ")
        table_name = json["name"]
        insert_query = "INSERT INTO {} {} VALUES {};".format(table_name, column, value)
        return insert_query
    elif (json["name"].lower() == "etoken") or ("vpn" in json["name"].lower()):
        columnNotDict = []
        valueNotDict = []
        for column_name in json:
            if (column_name not in columnNotDict and type(json[column_name]) != dict):
                columnNotDict.append(column_name)
                if type(json[column_name]) == str and json[column_name].find("'") != -1:
                    changed_value = (json[column_name].replace("'", ""))
                    valueNotDict.append(changed_value)
                else:
                    if json[column_name] == None: 
                        changed_value = 'None'
                        valueNotDict.append(changed_value)
                    else:
                        valueNotDict.append(json[column_name])
            if (column_name not in columnNotDict and type(json[column_name]) == dict):
                for attribute in json[column_name]:
                    if attribute not in columnNotDict:
                        attribute_copy = '"' + attribute + '"'
                        columnNotDict.append(attribute_copy)
                        if json[column_name][attribute].find("'") != -1:
                            changed_value = (json[column_name][attribute].replace("'", ""))
                            valueNotDict.append(changed_value)
                        else: 
                            valueNotDict.append(json[column_name][attribute])
        value = value +str(tuple(valueNotDict)) + ','
        column = str(tuple(columnNotDict))
        column = column.replace("'", "")
        value = value.strip(", ")
        table_name = json["name"]
        insert_query = "INSERT INTO {} {} VALUES {};".format(table_name, column, value)
        return insert_query
        


def insertTableQuery(json):
    """
    This is a helper function that returns the INSERT TABLE statement
    """
    columnNotDict = []
    value = ""
    column = ""
    for obj in json:
        valueNotDict = []
        for column_name in obj:
            if (type(obj[column_name]) != type({})):
                if (column_name not in columnNotDict):
                    columnNotDict.append(column_name)
                valueNotDict.append(obj[column_name])
        for column_name in obj:
            if (type(obj[column_name]) == type({})):
                columnDict = ['stemname', 'numstems']
                for stem in obj[column_name]:
                    combinedValue = valueNotDict[:]
                    combinedValue.append(stem)
                    combinedValue.append(obj[column_name][stem])

                    value = value + str(tuple(combinedValue)) + ', '
    columnNotDict += columnDict
    column = str(tuple(columnNotDict))
    column = column.replace("'", "")
    value = value.strip(", ")
    table_name = json[0]["name"]
    insert_query = "INSERT INTO {} {} VALUES {};".format(table_name, column, value)

    return insert_query


def updateTableQuery(table_name, attr_dict, condition=None):
    """
    This is a helper function that returns the UPDATE statement
    """
    statement = ''
    for keys in attr_dict:
        statement = statement + keys + ' = ' + attr_dict[keys] + ', '
    statement = statement.strip(", ")
    if (condition == ()):
        new_query = "UPDATE {} SET {};".format(table_name, statement)
    else:
        new_query = "UPDATE {} SET {} WHERE {};".format(table_name, statement, condition)
    return new_query


def alterTableQuery(table_name, column_name, column_type):
    """
    This is a helper function that returns the ALTER statement
    """

    alter_query = "ALTER TABLE {} ADD {} {};".format(table_name, column_name, column_type)
    return alter_query


def countall(username, pass_word):
    conn = getEtokenConnection(username, pass_word)
    cursor = conn.cursor()
    cursor.execute("select count(*) from usertokens;")
    return(cursor.fetchone()[0])


def countvirtual(username, pass_word):
    conn = getEtokenConnection(username, pass_word)
    cursor = conn.cursor()
    cursor.execute("select count(*) from usertokens where productname like '%virtual%';")
    return(cursor.fetchone()[0])


def countServiceProvider(username, pass_word):
    conn = getSQLConnection(username, pass_word)
    cursor = conn.cursor()
    cursor.execute("select count(*) from splist;")
    return(cursor.fetchone()[0])


def countNormal(username, pass_word):
    conn = getEtokenConnection(username, pass_word)
    cursor = conn.cursor()
    cursor.execute("select count(*) from usertokens where productname like '%virtual%';")
    virtual = cursor.fetchone()[0]
    cursor.execute("select count(*) from usertokens;")
    all = cursor.fetchone()[0]
    normal = int(all) - int(virtual)
    return(normal)


def showVirtualUsers(username, pass_word):
    conn = getEtokenConnection(username, pass_word)
    cursor = conn.cursor()
    cursor.execute(
        "select distinct utorid from myusers join usertokens on myusers.oid = usertokens.useroid where usertokens.productname like '%virtual%';")
    all = cursor.fetchall()
    for i in all:
        print(i[0])


def numExpiring(username, pass_word):
    conn = getEtokenConnection(username, pass_word)
    cursor = conn.cursor()
    cursor.execute(
        "select distinct utorid from myusers join usertokens on myusers.oid = usertokens.useroid where expirationdate < adddate(curdate(),14);")
    all = cursor.fetchall()
    j = 0
    for i in all:
        #print(i[0])
        j += 1
    return j


def numExpiringIn(username, pass_word, num_month):
    conn = getEtokenConnection(username, pass_word)
    cursor = conn.cursor()
    if(num_month < 0.5):
        cursor.execute(
            "select distinct utorid from myusers join usertokens on myusers.oid = usertokens.useroid where expirationdate < adddate(curdate(), 14);")
    else:
        cursor.execute(
            "select distinct utorid from myusers join usertokens on myusers.oid = usertokens.useroid where expirationdate < adddate(curdate(), {});".format(str(num_month * 30), str(num_month - 1 * 30)))
    all = cursor.fetchall()
    expiring = []
    j = 0
    for i in all:
        expiring.append(i[0])
        #print(i[0])
        j += 1
    return j

def getInventory(username, pass_word):
    conn = getEtokenConnection(username, pass_word)
    cursor = conn.cursor()
    cursor.execute("select * from inventory;")
    curr_inventory = inventory_on_20200510
    for i in cursor.fetchall()[::-1]:
        if i[0] >= baseline_date:
            curr_inventory += i[2]
        if i[0] < baseline_date:
            break
    cursor.execute("select * from orderhistory;")
    curr_inventory = inventory_on_20200510
    for i in cursor.fetchall()[::-1]:
        if i[0] >= baseline_date:
            curr_inventory -= i[-1]
        if i[0] < baseline_date:
            break
    return curr_inventory

def json2csv(username, password):
    conn = getConnection(username, password)
    cursor = conn.cursor()
    cursor.execute("select payload from incoming;")
    
    All = cursor.fetchall()
    csv_columns = list(All[0][0].keys())
    csv_file = "jsons.csv"
    jsons = []
    for i in All:
        if (i[0]["name"] == 'etoken'):
            jsons.append(i[0])
    try:
        with open(csv_file, 'w') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
            writer.writeheader()
            for json in jsons:
                writer.writerow(json)
    except IOError:
        print("I/O error")

