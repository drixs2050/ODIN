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
	while(True):
		query_type = raw_input("Type in a query -- \n [create] Create Table in ODIN Database \
						           \n [insert] Insert into existing Table depending on the table_name specified in json file \
							   \n [select] Look up Service Providers \
						           \n [2] Quit Program \
							   \n Your response:  ")
		
		if(query_type == "create"):
			print("You have entered: {}".format(query_type))
			table_name = raw_input("Choose the title of your table: ")
			print("Possible variable types: \n [varchar] [stemname: varchar] [numstems: Int]")
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
			while (True):
				user_decision = raw_input("\nChoose the following option: \
				 	  \n [1] Input json blob into exisiting table inside ODIN Database \
				  	  \n [2] Add column in the exisitng table \
				 	  \n [-1] Return to previous screen  \
					  \n [Quit] Exit the program  \
				 	  \n Your response: ")
				if (user_decision == "1"):
					#Should implement where it shows all the table_name in the database. Use select for this and show.
					insertTable(data)
				elif(user_decision == "2"):
					print("do something");
				elif (user_decision == "-1"):
					print("Returning to previous screen... \n")
					break;
				elif (user_decision.lower() == "Quit".lower()):
					print("Exiting...")
					exit(1)
				else:
					print("Error: Not a valid option \n")
		
		elif(query_type == "select"):
			print("\nConnecting to the shibboleth database...")
        	        while (True):
				try:
					username = raw_input("Type in your username: ")
               				pa = getpass.getpass()
					getSQLConnection (username, pa)
					break;
				except:
					print("Error: Incorrect username/password\n")
			
               		while (True):
                        	try:
                                	user_decision = raw_input ("\nChoose the index from the following option -- \
								   \n [1] Visualize modularized data \
								   \n [2] View admin(s) for a single Service Provider \
                                                                   \n [-1] Return to Previous screen \
							    	   \n [Quit] Quit Program. \n")
                                	print("You have typed in: {}".format(user_decision))
                                	if (user_decision == "1"):
                                        	showSQLAttribute(username, pa)
                                       	 	index = raw_input("Type in the index of the attribute you want to look at: ")
                                      		index = int(index)
						selectOneAttribute(username, pa, index)
                               		elif (user_decision == "-1"):
                                        	print("Returning to previous screen...\n")
	                                        break;
					elif (user_decision == "2"):
						sp = raw_input ("Type in a service provider that you want to look up: ")
						checkSP(username, pa, sp)
					elif (user_decision.lower() == "Quit".lower()):
						print("Exiting the program ... \n")
						exit(1)
	                                else:
        	                                print("Invalid input")
			        except ValueError:
                        	        print("Non-numeric input detected.")
		elif(query_type == "2"):
			print("Exiting...\n")
			exit(1)
		else:
			print("Invalid option\n")
			
	







		
			
		
		
if  __name__ == '__main__':
	main("test.json")

	
	
	
