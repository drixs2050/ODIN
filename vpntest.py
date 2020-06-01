import subprocess
ssh = subprocess.Popen(["grep", "113019", "/var/log/cisco"], stdin = subprocess.PIPE, stdout= subprocess.PIPE, stderr = subprocess.PIPE, universal_newlines=True, bufsize =0)

jsons = []
for line in ssh.stdout:
    period = line.split('.')
    first = [period[0] +'.'+ period[1]+'.' + period[2]+ '.' + period[3], period[4]]
    second = first[0].split(',') + first[1].split(',')
    stripped = [i.strip() for i in second]
    json = {}
    info = {}
    try:
        json['Date'] = stripped[0][0:6].strip()
        info['Time'] = stripped[0][7:15].strip()
        info['Service'] = stripped[0][16:36].strip()
        info['Group']= stripped[0][46:].strip()
        info['IP'] = stripped[2][5:].strip()
        info['Session Type'] = stripped[4][14:].strip()
        info['Duration'] = stripped[5][10:].strip()
        info['Bytes xmt'] = stripped[6][11:].strip()
        info['Bytes rcv'] = stripped[7][11:].strip()
        info['Reason'] = stripped[8][8:].strip()
        json[stripped[1][11:].strip()] = info
        jsons.append(json)
        print(json)
    except IndexError:
        print('special case')
        print(stripped)        
#date_index =(line.find(":")-2)
#time = line[date_index:date_index+8]
        #print(time)
        #username_index = line.find("Username")+11
        #IP_index = line.find("IP")-2
        #username = (line[username_index: IP_index])
        #user_info["time_accesed"] = time
        #date_json[username] = user_info
        #print(date_json)
        #print(line)
