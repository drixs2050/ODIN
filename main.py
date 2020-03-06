import json
from create import *
import getpass
import email

def main (file_name):
	try:
		with open(file_name, "r") as json_file:
			num_dict_in_obj = 0
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
						num_dict_in_obj = num_dict_in_obj + 1
						removed_data.append(line)
					json_data.append("}add_name_dict_here,{")
			
			dictInStr = "".join(json_data)
			dictInStr = dictInStr.replace("}{", "}*{")
			dictInStr = dictInStr.replace(",}", "}")
			#dictInStr = dictInStr.replace("}{", "},*{")
			#dictInStr = dictInStr.replace(",{","{")
			#dictInStr = dictInStr.replace("}}", "}")
			
			dictInStr = dictInStr.split("*")
			data = []
			data_len = []
			for i in range(len(dictInStr)):
				blob = dictInStr[i].replace("}}","}")
				blob = blob.split("add_name_dict_here,")
				blob_len = 0
				if (len(blob) > 1):
					for first_index in range(len(blob) - len(removed_data)):
						obj = json.loads(blob[first_index])
						blob_len = blob_len + len(obj)
					for j in range(len(removed_data)):
						obj[removed_data[j]] = json.loads(blob[j+1])
						blob_len = blob_len + len(obj[removed_data[j]])
				else:	
					for fisrt_index in range(len(blob)):
						obj = json.loads(blob[first_index])
					blob_len = len(obj)
				data.append(obj)
				data_len.append(blob_len)
			print(data_len)
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
			showAllTablesODIN(True)
			table_name = raw_input("Choose the title of your table: ")
			my_tables = showAllTablesODIN(False)
			if (table_name in my_tables):
				print("Error: {} already exists in the database\n".format(table_name))
			else:
				print("Possible variable types: \n [varchar] [stemname: varchar] [numstems: Int]")	
				var_dict = {}
				max_data_index = data.index(max(data))
				for column in data[max_data_index]:
					if (type(data[max_data_index][column]) == type({})):
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
					  \n [3] Add comment to table \
				 	  \n [-1] Return to previous screen  \
					  \n [Quit] Exit the program  \
				 	  \n Your response: ")
				if (user_decision == "1"):
					#Should implement where it shows all the table_name in the database. Use select for this and show.
					insertTable(data)
				elif(user_decision == "2"):
					#Show the existing tables 
					print("List of tables in the ODIN database:")
					showAllTablesODIN(True)	
					#ask for user to chose from this.
					
					table = raw_input("\nChoose from the following list of tables to add a description to:  ")
						
					#Ask the user for the column to add and also the type
					column_name = raw_input("\nType in the column_name that you would like to add:  ");
					column_type = raw_input("\nType in the column_type for {}:  ".format(column_name));	
					#alter the table
					alterTable(table, column_name, column_type)	
					
				elif (user_decision == "3"):
					print("List of tables in the ODIN database:")
					showAllTablesODIN(True)
					table = raw_input("\nChoose from the following list of tables to add a description to:  ")
					comment = raw_input("\nType in the comment that you would like to add:  ");
					if ("'" in comment):
						comment = comment.replace("'", "''")
					addComment(table, comment)	
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
                                        	max_index = showSQLAttribute(username, pa)
						while (True):
                                       	 		index = raw_input("Type in the index of the attribute you want to look at: ")
                                      			index = int(index)
							if ((0 <= index) and (index <  max_index)):
								selectOneAttribute(username, pa, index)
								break;
							else:
								print("Error: Invalid input")
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

