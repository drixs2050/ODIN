import subprocess
ssh = subprocess.Popen(["grep", "113019", "/var/log/cisco"], stdin = subprocess.PIPE, stdout= subprocess.PIPE, stderr = subprocess.PIPE, universal_newlines=True, bufsize =0)

date_json = {}
user_info = {}
for line in ssh.stdout:
        date_index =(line.find(":")-2)
        time = line[date_index:date_index+8]
        print(time)
        username_index = line.find("Username")+11
        IP_index = line.find("IP")-2
        username = (line[username_index: IP_index])
        user_info["time_accesed"] = time
        date_json[username] = user_info
        print(date_json)
        print(line)
