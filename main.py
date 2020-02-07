import json
from create import *
import getpass

def main (file_name):
	try:
		with open(file_name, "r") as json_file:
			num_dict_in_obj = 1
			json_data = []
			removed_data = []	
			for line in json_file:
				line = line.replace("+", "")
				line = line.replace(" ", "")
				line = line.replace("\n", "")
				if (line != '' and line.find(":{")== -1):
					json_data.append(line)
				elif(line.find(":{")!= -1):
					line = line.replace(":{", "")
					if (line not in removed_data):
						num_dict_in_obj +=1
						removed_data.append(line)
					json_data.append("}{")
			dictInStr = "".join(json_data)
			dictInStr = dictInStr.replace(",}", "}")
			dictInStr = dictInStr.replace("}{", "},*{")
			dictInStr = dictInStr.replace(",{",",*{")
			dictInStr = dictInStr.replace("}}", "}")
			dictInStr = dictInStr.split(",*")
			data = []
			for i in range(len(dictInStr)):
				if (i%2 == 0):
					obj = json.loads(dictInStr[i])
					for j in range(len(removed_data)):
						obj[removed_data[j]] = json.loads(dictInStr[j+1])
					data.append(obj)
						
						
				
	except IOError:
		print("Error:File does not appear to exist.")
		return
	query_type = raw_input("Type in a query -- create/insert/select or press 2 to quit: ")
		
	if(query_type == "create"):
		print("You have entered: {}".format(query_type))
		table_name = raw_input("Choose the title of your table: ")
		#Should check if the table exists or not.
		if (len(data[0]) == len(data[-1])):
			var_dict = {}
			for column in data[0]:
				if (type(data[0][column]) == type({})):	
					first_column = raw_input("Give first column_name for {}: ".format(column))
					first_var = raw_input("variable type of {}: ".format(first_column))
					second_column = raw_input("Give second column_name for {}: ".format(column))
					second_var = raw_input("variable type of {}: ".format(second_column))
					var_dict[first_column] = first_var
					var_dict[second_column] = second_var
				else:
					var = raw_input("Variable type of {}: ".format(column))
					var_dict[column] = var
				
		createTable(table_name, var_dict)
		
											
	elif(query_type == "insert"):
		print("You have entered: {}".format(query_type))
		#Should implement where it shows all the table_name in the database. Use select for this and show.
		insertTableQuery(data)
	
	elif(query_type == "select"):
		username = raw_input("Type in your username: ")
		pa = getpass.getpass()
		
		selectSQLQuery(username, pa)
		
		
			
		
		
if __name__ == '__main__':
	main("test.json")

	
	
	
