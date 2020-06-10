import subprocess
import datetime 

  
def convert(n): 
    return str(datetime.timedelta(seconds = n)) 


ssh = subprocess.Popen(["grep", "113019", "/var/log/cisco"], stdin = subprocess.PIPE, stdout= subprocess.PIPE, stderr = subprocess.PIPE, universal_newlines=True, bufsize =0)
stats = {}
stats['total bytes xmt'] = 0
stats['total bytes rcv'] = 0
stats['total duration'] = 0
stats['total connection'] = 0
stats['disconnection reasons'] = {}
stats['groups'] = {}
stats['session types'] = {}
for line in ssh.stdout:
    period = line.split('.')
    first = [period[0] +'.'+ period[1]+'.' + period[2]+ '.' + period[3], period[4]]
    second = first[0].split(',') + first[1].split(',')
    stripped = [i.strip() for i in second]
    try:
        stats['total bytes xmt'] += int(stripped[6][11:].strip())
        stats['total bytes rcv'] += int(stripped[7][11:].strip())
        stats['total connection'] += 1
        if stripped[8][8:].strip() not in stats['disconnection reasons']:
            stats['disconnection reasons'][stripped[8][8:].strip()] = 1
        else:
            stats['disconnection reasons'][stripped[8][8:].strip()] += 1
        if stripped[4][14:].strip() not in stats['session types']:
            stats['session types'][stripped[4][14:].strip()] = 1
        else:
            stats['session types'][stripped[4][14:].strip()] += 1
        if stripped[0][46:].strip() not in stats['groups']:
            stats['groups'][stripped[0][46:].strip()] = 1
        else:
            stats['groups'][stripped[0][46:].strip()] += 1
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
        stats['total duration'] += (seconds + minutes * 60 + hours * 3600 + days * 86400)
    except (IndexError, ValueError):
        #print('special case')
        #print(stripped)
        continue
stats['total duration'] = convert(stats['total duration'])
print(stats)        
