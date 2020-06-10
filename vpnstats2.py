import subprocess
ssh = subprocess.Popen(["grep", "722055", "/var/log/cisco"], stdin = subprocess.PIPE, stdout= subprocess.PIPE, stderr = subprocess.PIPE, universal_newlines=True, bufsize =0)

stats = {}
stats['total connection'] = 0
stats['port'] = {}
stats['group'] = {}
stats['service type'] = {}
for line in ssh.stdout:
    list = line.split('<')
    first = list[0].split(' ')
    try:
        stats['total connection'] += 1
        if first[3].strip() not in stats['port']:
            stats['port'][ first[3].strip()] = 1
        else:
            stats['port'][ first[3].strip()] += 1
        if list[1].split('>')[0].strip() not in stats['group']:
            stats['group'][list[1].split('>')[0].strip()] = 1
        else:
            stats['group'][list[1].split('>')[0].strip()] += 1
        if list[3].split(':')[1].strip() not in stats['service type']:
            stats['service type'][list[3].split(':')[1].strip()] = 1
        else:
            stats['service type'][list[3].split(':')[1].strip()] += 1
        #json['Date'] = first[0].strip() + ' '+first[1].strip()
        #json['Time'] = first[2].strip()
        #json['Port'] = first[3].strip()
        #json['Service_version'] = list[0].split(':')[3].strip()
        #json['Group'] = list[1].split('>')[0].strip()
        #json['userID'] = list[2].split('>')[0].strip()
        #json['IP'] = list[3].split('>')[0].strip()
        #json['Service type'] = list[3].split(':')[1].strip()
    except IndexError:
        print(line)
print(stats)
