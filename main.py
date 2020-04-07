import getpass
import json

from create import *


def main(file_name):
    try:
        with open(file_name, "r") as json_file:
            num_dict_in_obj = 0
            json_data = []
            removed_data = []
            for line in json_file:
                line = line.replace("+", "")
                line = line.replace(" ", "")
                line = line.replace("\n", "")
                if (line != '' and line.find(":{") == -1):
                    json_data.append(line)
                elif (line.find(":{") != -1):
                    line = line.replace(":{", "")
                    if (line not in removed_data):
                        num_dict_in_obj = num_dict_in_obj + 1
                        removed_data.append(line)
                    json_data.append("}add_name_dict_here,{")

            dictInStr = "".join(json_data)
            dictInStr = dictInStr.replace("}{", "}*{")
            dictInStr = dictInStr.replace(",}", "}")
            # dictInStr = dictInStr.replace("}{", "},*{")
            # dictInStr = dictInStr.replace(",{","{")
            # dictInStr = dictInStr.replace("}}", "}")

            dictInStr = dictInStr.split("*")
            data = []
            data_len = []
            for i in range(len(dictInStr)):
                blob = dictInStr[i].replace("}}", "}")
                blob = blob.split("add_name_dict_here,")
                blob_len = 0
                if (len(blob) > 1):
                    for first_index in range(len(blob) - len(removed_data)):
                        obj = json.loads(blob[first_index])
                        blob_len = blob_len + len(obj)
                    for j in range(len(removed_data)):
                        obj[removed_data[j]] = json.loads(blob[j + 1])
                        blob_len = blob_len + len(obj[removed_data[j]])
                else:
                    for fisrt_index in range(len(blob)):
                        obj = json.loads(blob[first_index])
                    blob_len = len(obj)
                data.append(obj)
                data_len.append(blob_len)
    except IOError:
        print("Error:File does not appear to exist.")
        return
    while (True):
        try:
            psql_user = input("Type in your username: ")
            getConnection(psql_user)
            print(psql_user)
            break
        except:
            print("Incorrect username\n")
    while (True):
        query_type = input("Type in a query -- \n [create] Create Table in ODIN Database \
                                   \n [insert] Insert into existing Table depending on the table_name specified in json file \
                               \n [select] Look up Service Providers \
                               \n [etoken] Look up etoken database \
                                   \n [2] Quit Program \
                               \n Your response:  ")

        if (query_type == "create"):
            print("You have entered: {}".format(query_type))
            showAllTablesODIN(True, psql_user)
            table_name = input("Choose the title of your table: ")
            my_tables = showAllTablesODIN(False, psql_user)
            if (table_name in my_tables):
                print("Error: {} already exists in the database\n".format(table_name))
            else:
                print("Possible variable types: \n [varchar] [stemname: varchar] [numstems: Int]")
                var_dict = {}
                max_data_index = data.index(max(data))
                for column in data[max_data_index]:
                    if (type(data[max_data_index][column]) == type({})):
                        first_column = input("Give first column_name for {}: ".format(column))
                        first_var = input("variable type of {}: ".format(first_column))
                        second_column = input("Give second column_name for {}: ".format(column))
                        second_var = input("variable type of {}: ".format(second_column))
                        var_dict[first_column] = first_var
                        var_dict[second_column] = second_var
                    else:
                        var = input("Variable type of {}: ".format(column))
                        var_dict[column] = var

                createTable(table_name, var_dict, psql_user)


        elif (query_type == "insert"):
            print("You have entered: {}".format(query_type))
            while (True):
                user_decision = input("\nChoose the following option: \
                      \n [1] Input json blob into exisiting table inside ODIN Database \
                      \n [2] Add column in the exisitng table \
                      \n [3] Add comment to table \
                      \n [-1] Return to previous screen  \
                      \n [Quit] Exit the program  \
                      \n Your response: ")
                if (user_decision == "1"):
                    # Should implement where it shows all the table_name in the database. Use select for this and show.
                    insertTable(data, psql_user)
                elif (user_decision == "2"):
                    # Show the existing tables
                    print("List of tables in the ODIN database:")
                    showAllTablesODIN(True, psql_user)
                    # ask for user to chose from this.

                    table = input("\nChoose from the following list of tables to add a description to:  ")

                    # Ask the user for the column to add and also the type
                    column_name = input("\nType in the column_name that you would like to add:  ")
                    column_type = input("\nType in the column_type for {}:  ".format(column_name))
                    # alter the table
                    alterTable(table, column_name, column_type, psql_user)

                elif (user_decision == "3"):
                    print("List of tables in the ODIN database:")
                    showAllTablesODIN(True, psql_user)
                    table = input("\nChoose from the following list of tables to add a description to:  ")
                    comment = input("\nType in the comment that you would like to add:  ")
                    if ("'" in comment):
                        comment = comment.replace("'", "''")
                    addComment(table, comment, psql_user)
                elif (user_decision == "-1"):
                    print("Returning to previous screen... \n")
                    break
                elif (user_decision.lower() == "Quit".lower()):
                    print("Exiting...")
                    exit(1)
                else:
                    print("Error: Not a valid option \n")

        elif (query_type == "select"):
            print("\nConnecting to the shibboleth database...")
            while (True):
                try:
                    username = input("Type in your username: ")
                    pa = getpass.getpass()
                    getSQLConnection(username, pa)
                    break
                except:
                    print("Error: Incorrect username/password\n")

            while (True):
                try:
                    user_decision = input("\nChoose the index from the following option -- \
                                   \n [1] Visualize modularized data \
                                   \n [2] View admin(s) for a single Service Provider \
                                   \n [3] Count total SP \
                                                                   \n [-1] Return to Previous screen \
                                       \n [Quit] Quit Program. \n")
                    print("You have typed in: {}".format(user_decision))
                    if (user_decision == "1"):
                        max_index = showSQLAttribute(username, pa)
                        while (True):
                            index = input("Type in the index of the attribute you want to look at: ")
                            index = int(index)
                            if ((0 <= index) and (index < max_index)):
                                selectOneAttribute(username, pa, index)
                                break
                            else:
                                print("Error: Invalid input")
                    elif (user_decision == "-1"):
                        print("Returning to previous screen...\n")
                        break
                    elif (user_decision == "2"):
                        sp = input("Type in a service provider that you want to look up: ")
                        checkSP(username, pa, sp)
                    elif(user_decision == "3"):
                        countServiceProvider(username, pa)
                    elif (user_decision.lower() == "Quit".lower()):
                        print("Exiting the program ... \n")
                        exit(1)
                    else:
                        print("Invalid input")
                except ValueError:
                    print("Non-numeric input detected.")
        elif (query_type == "etoken"):
            username = input("Type in your username: ")
            pa = getpass.getpass()
        
            while (True):
		
                try:
                    user_decision = input("\nChoose the index from the following option -- \
                        \n [1] Get Total virtual etoken count \
                        \n [2] Get Total etoken count \
			\n [3] Get all the UTORID with virtual etoken \
                        \n [-1] Return to Previous screen \
                        \n [Quit] Quit Program. \n")
                    print("You have typed in {}".format(user_decision))
                    if (user_decision == "1"):
                        countvirtual(username, pa)
                    elif (user_decision == "2"):
                        countNormal(username, pa)
                    elif(user_decision == "3"):
                        showVirtualUsers(username, pa)
                    elif(user_decision == "-1"):
                        print("Returning to previous screen...\n")
                        break 
                    elif(user_decision.lower() == "Quit".lower()):
                        print("Exiting the program ... \n")
                        exit(1)
                    else:
                        print("Invalid input")
                except ValueError:
                    print("Non-numeric input detected.")
        elif (query_type == "2"):
            print("Exiting...\n")
            exit(1)
        else:
            print("Invalid option\n")


if __name__ == '__main__':
    main("testv2.json")
