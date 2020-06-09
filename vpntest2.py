import subprocess
ssh = subprocess.Popen(["grep", "722055", "/var/log/cisco"], stdin = subprocess.PIPE, stdout= subprocess.PIPE, stderr = subprocess.PIPE, universal_newlines=True, bufsize =0)

jsons = []
for line in ssh.stdout:
    list = line.split('<')
    json = {}
    try:
        first = list[0].split(' ')
        json['Date'] = first[0].strip() + ' '+first[2].strip()
        json['Time'] = first[3].strip()
        json['Port'] = first[4].strip()
        json['Service_version'] = list[0].split(':')[3].strip()
        json['Group'] = list[1].split('>')[0].strip()
        json['userID'] = list[2].split('>')[0].strip()
        json['IP'] = list[3].split('>')[0].strip()
        json['Service type'] = list[3].split(':')[1].strip()
        print(json)
    except IndexError:
        print('special case')
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
