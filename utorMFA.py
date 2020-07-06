import csv

with open('duo-logs.csv', mode='r') as infile:
    reader = csv.reader(infile)
    with open('duo-logs-new.csv', mode='w') as outfile:
        writer = csv.writer(outfile)
        duo_log_dict = {rows[0]:rows[1] for rows in reader}

print(duo_log_dict)

with open('divisions.csv', mode='r') as infile:
    reader1 = csv.reader(infile)
    first = next(reader1)
    with open('divisions-new.csv', mode='w') as outfile:
        divisions_dict = {}
        divisions_dict[first[0]] = []
        divisions_dict[first[1]] = []
        divisions_dict[first[2]] = []
        divisions_dict[first[3]] = []
        divisions_dict[first[4]] = []
        for rows in reader1:
            divisions_dict[first[0]].append(rows[0])
            divisions_dict[first[1]].append(rows[1])
            divisions_dict[first[2]].append(rows[2])
            divisions_dict[first[3]].append(rows[3])
            divisions_dict[first[4]].append(rows[4])

print(divisions_dict)

with open('auth-types.csv', mode='r') as infile:
    reader2 = csv.reader(infile)
    first = next(reader2)
    with open('auth-types-new.csv', mode='w') as outfile:
        auth_type_dict = {}
        auth_type_dict[first[0]] = []
        auth_type_dict[first[1]] = []
        auth_type_dict[first[2]] = []
        auth_type_dict[first[3]] = []
        auth_type_dict[first[4]] = []
        for rows in reader2:
            auth_type_dict[first[0]].append(rows[0])
            auth_type_dict[first[1]].append(rows[1])
            auth_type_dict[first[2]].append(rows[2])
            auth_type_dict[first[3]].append(rows[3])
            auth_type_dict[first[4]].append(rows[4])

print(auth_type_dict)