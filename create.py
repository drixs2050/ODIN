import mysql.connector
import psycopg2
import datetime
import csv
inventory_on_20200510 = 132
baseline_date = datetime.date(2020, 5, 10)

def getConnection(username):
    new_conn = psycopg2.connect(user=username, database='odin')

    return new_conn


def getSQLConnection(username, pass_word):
    new_conn = mysql.connector.connect(user=username, password=pass_word, database='shibboleth')
    return new_conn


def getEtokenConnection(username, pass_word):
    new_conn = mysql.connector.connect(user=username, password=pass_word, database='etoken')
    return new_conn


def showSQLAttribute(username, pass_word):
    conn = getSQLConnection(username, pass_word)
    cursor = conn.cursor()
    cursor.execute("Describe splist")
    # cursor.execute("SELECT * FROM splist")
    print("\nService Provider Attributes:")
    i = 0
    for row in cursor.fetchall():
        print("[{}]: {}".format(i, row[0]))
        i = i + 1
    conn.close()
    return i


def showPSQLAttribute(table, username):
    conn = getConnection(username)
    cursor = conn.cursor()
    cursor.execute("SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{}';".format(table))
    list = []
    for i in cursor.fetchall():
        list.append(i[0])
    conn.close()
    return list


def selectOneAttribute(username, pass_word, index):
    conn = getSQLConnection(username, pass_word)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM splist")
    for row in cursor.fetchall():
        print(row[index])
    conn.close()


def checkValidSP(username, pass_word, sp):
    conn = getSQLConnection(username, pass_word)
    cursor = conn.cursor()
    cursor.execute("SELECT entityID from splist where entityID = '{}'".format(sp))
    if (cursor.fetchone() == None):
        return -1
    return 0


def checkSP(username, pass_word, sp):
    conn = getSQLConnection(username, pass_word)
    cursor = conn.cursor()
    validSPCheck = checkValidSP(username, pass_word, sp)
    if (validSPCheck == -1):
        conn.close()
        print("\nThe service provider does not exist")
        return -1
    cursor.execute("SELECT contactpname, contactemail FROM splist where entityID = '{}'".format(sp))
    for row in cursor.fetchall():
        print("\nAdmin: {}, Contact: {}".format(row[0], row[1]))
    conn.close()


def searchInOneAttribute(username, pass_word, attribute, key_word):
    conn = getSQLConnection(username, pass_word)
    cursor = conn.cursor()
    searchfor = '%' + key_word + '%'
    print("SELECT * FROM splist WHERE " + attribute + " LIKE " + "'" + searchfor + "'")
    cursor.execute("SELECT * FROM splist WHERE " + attribute + " LIKE " + "'" + searchfor + "'")
    print(cursor.fetchall())
    conn.close()


def showAllTablesODIN(print_boolean, username):
    try:
        conn = getConnection(username)
        cursor = conn.cursor()
        query = """Select a."Name" from (
SELECT n.nspname as "Schema",
  c.relname as "Name",
  CASE c.relkind WHEN 'r' THEN 'table' WHEN 'v' THEN 'view' WHEN 'i' THEN 'index' WHEN 'S' THEN 'sequence' WHEN 's' THEN 'special' WHEN 'f' THEN 'foreign table' END as "Type",
  pg_catalog.pg_get_userbyid(c.relowner) as "Owner"
FROM pg_catalog.pg_class c
     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind IN ('r','v','S','f','')
      AND n.nspname <> 'pg_catalog'
      AND n.nspname <> 'information_schema'
      AND n.nspname !~ '^pg_toast'
  AND pg_catalog.pg_table_is_visible(c.oid)
ORDER BY 1,2) a"""
        cursor.execute(query)
        table_lst = []
        for row in cursor.fetchall():
            if (print_boolean == True):
                print(row[0])
            else:
                table_lst.append(row[0])
    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)
    finally:
        if (conn):
            cursor.close()
            conn.close()
            if (print_boolean == False):
                return table_lst


def showAllAttributes(username, table_name):
    conn = getConnection(username)
    cursor = conn.cursor()
    query = cursor.execute("SELECT * FROM {};".format(table_name))
    for row in cursor.fetchall():
        cursor.close()
        conn.close()
        return row


def addComment(table_name, comment, username):
    try:
        conn = getConnection(username)
        cursor = conn.cursor()
        cursor.execute("COMMENT ON TABLE {} IS '{}';".format(table_name, comment))
        conn.commit()
    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)
    finally:
        if (conn):
            cursor.close()
            conn.close()


def commitQuery(query, output, username):
    """
    Prints out a successful statement if query worked. Else, print a error statement.

    Parameters
    ----------
    query: str
      This is a query in the form of the string. Ex. "SELECT * FROM hello"
    output: str
      This string is the expected output that is printed depending on the query.
    """
    try:
        conn = getConnection(username)
        cursor = conn.cursor()
        cursor.execute("SELECT version();")
        record = cursor.fetchone()
        cursor.execute(query)
        conn.commit()
    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)
    finally:
        if (conn):
            cursor.close()
            conn.close()


"""
Alter, add and remove, you don't want to update (change the data)
"""


def createTable(table_name, var_dict, username):
    """
    Creates a new table in the odin database.

    SHOULD IMPLEMENT: should delete the table if it already exists.

    Parameters
    ----------
    table_name: string
      The name of the table that is to be created.

    Returns
    -------
    Returns a message saying Table successfully created
    or error statement if syntax error.
    """
    commitQuery(createTableQuery(table_name, var_dict), "Table created successfully in PostgreSQL", username)
    commitQuery("Alter table {} OWNER TO odin;".format(table_name), "Table created successfully in PostgreSQL",
                username)


def insertTableJson(json, username):
    """
    Inserts a json blobs that are to be added into the odin database.
    """
    commitQuery(insertTableJsonQuery(json), "Table inserted successfully in PostgreSQL", username)


def insertTable(json, username):
    """
    Inserts a list of values into the columns in the odin database.

    SHOULD IMPLEMENT: should give error if either the columnlst or valuelst is not inside the table.

    Parameters
    ----------
    table_name: string
      The name of the table that information should be inserted into.
    attr_dict: dict
      The dictionary where the keys at the column_name and values are the values of the column_name.
    Returns
    -------
    Returns a message saying Table successfully inserted
    or error statement if syntax error.

    """
    commitQuery(insertTableQuery(json), "Table inserted successfully in PostgreSQL", username)


def alterTable(table_name, column_name, column_type, username):
    """
    Adds one single column into the existing table.

    Parameters
    ----------
    table_name: string
      The name of the table where the column is to be added.
    attr_dict: dict
      A dictionary where the keys are the column name and value is the type of the column.
      The dictionary cannot have a length greater than 1.

    Returns
    -------
    Returns a message saying Table successfully altered
    or error statement if syntax error.
    """
    commitQuery(alterTableQuery(table_name, column_name, column_type), "Table altered successfully in PostgreSQL",
                username)


def updateTable(table_name, attr_dict, condition=None):
    """
    Updates the table_name by the given attr_dict. If the condition is given,
    then updates only at the specific condition.

    Parameters
    ----------
    table_name: string
      The name of the table where the information is to be updated.
    attr_dict: dict
      A dictionary where the key is the column name and
                value is the value that is to be updated to.
    *condition: string
      A string that should be in the format "WHERE .....". This condition
      statement is optional, if this is empty, updates all the columns that are given.

    Returns
    -------
    Returns a message saying Table successfully updated
    or error statement if syntax error.
    """
    commitQuery(updateTableQuery(table_name, attr_dict, *condition), "Table updated successfully in PostgreSQL")


def deleteTable(table_name, condition):
    """
    Deletes the rows of the specific condition.

    Parameters
    ----------
    table_name: string
      The name of the table in the odin database.
    condition: string
      A string in the format "WHERE ...." The specific condition where
      the rows should be deleted.

    Returns
    -------
    Returns a message saying Table successfully deleted
    or error statement if syntax error.
    """
    commitQuery("DELETE FROM {} {};".format(table_name, condition), "Table deletected successfully in PostgreSQL")


def dropTable(table_name, username):
    commitQuery("Drop table {};".format(table_name), "Table dropped successfully in PostgreSQL", username)


def selectTable(table_name, variable=None, condition=None):
    """
    Selects the table to read, if the columnlst is not defined, then reads the entire table,
    otherwise, reads the specific columns given.

    Parameters
    ----------
    table_name : string
      The name of the table that is to be read.
    *columnlst : list
      Optional list of columns that is to be read. If the columnlst is not defined, reads the
      entire table.

    Returns
    -------
    Returns a message saying Table successfully read
    or error statement if syntax error.
    """
    commitQuery(selectTableQuery(table_name, variable, condition), "Table read successfully in PostgreSQL")


def selectTableQuery(table_name, variable=None, condition=None):
    """
    This is a helper function that returns the SELECT statement
    """
    if (variable != None and condition == None):
        new_query = "SELECT {} FROM {}".format(variable, table_name)

    elif (variable != None and condition != None):
        new_query = "SELECT {} FROM {} WHERE {}".format(variable, table_name, condition)
    else:
        new_query = "SELECT * FROM {}".format(table_name)
    return new_query


def createTableQuery(table_name, attr_dict):
    """
    This is a helper function that returns the CREATE TABLE statement
    """
    data_str = ''
    for keys in attr_dict:
        statement = keys + ' ' + attr_dict[keys] + ', '
        data_str = data_str + statement
    data_statement = data_str.strip(', ')
    new_query = "CREATE TABLE {} ({});".format(table_name, data_statement)
    return new_query


def insertTableJsonQuery(json):
    """
    This is a helper function that returns the INSERT TABLE statement
    """
    column = ""
    value = ""
    if (json["name"].lower() == 'grouper'):
        columnNotDict = []
        valueNotDict = []
        for column_name in json:
            if (type(json[column_name]) != dict):
                if (column_name not in columnNotDict):
                    columnNotDict.append(column_name)
                valueNotDict.append(json[column_name])

        for column_name in json:
            if (type(json[column_name]) == dict):
                columnDict = ['stemname', 'numstems']
                for stem in json[column_name]:
                    combinedValue = valueNotDict[:]
                    combinedValue.append(stem)
                    combinedValue.append(json[column_name][stem])
                    value = value + str(tuple(combinedValue)) + ','

        columnNotDict += columnDict
        column = str(tuple(columnNotDict))
        column = column.replace("'", "")
        value = value.strip(", ")
        table_name = json["name"]
        insert_query = "INSERT INTO {} {} VALUES {};".format(table_name, column, value)
        return insert_query
    elif (json["name"].lower() == "archive"):
        columnNotDict = []
        valueNotDict = []
        for column_name in json:
            if (column_name != "name"):
                if (column_name not in columnNotDict):
                    columnNotDict.append(column_name)
                if (type(json[column_name]) == tuple):
                    str_dict = str(json[column_name])
                    str_dict = str_dict.replace("'", "")
                    valueNotDict.append(str_dict)
                else:
                    valueNotDict.append(json[column_name])
        value = value + str(tuple(valueNotDict)) + ','
        column = str(tuple(columnNotDict))
        column = column.replace("'", "")
        value = value.strip(", ")
        table_name = json["name"]
        insert_query = "INSERT INTO {} {} VALUES {};".format(table_name, column, value)
        return insert_query
    elif (json["name"].lower() == "etoken"):
        columnNotDict = []
        valueNotDict = []
        for column_name in json:
            if (column_name not in columnNotDict):
                columnNotDict.append(column_name)
            valueNotDict.append(json[column_name])
        value = value +str(tuple(valueNotDict)) + ','
        column = str(tuple(columnNotDict))
        column = column.replace("'", "")
        value = value.strip(", ")
        table_name = json["name"]
        insert_query = "INSERT INTO {} {} VALUES {};".format(table_name, column, value)
        return insert_query
        


def insertTableQuery(json):
    """
    This is a helper function that returns the INSERT TABLE statement
    """
    columnNotDict = []
    value = ""
    column = ""
    for obj in json:
        valueNotDict = []
        for column_name in obj:
            if (type(obj[column_name]) != type({})):
                if (column_name not in columnNotDict):
                    columnNotDict.append(column_name)
                valueNotDict.append(obj[column_name])
        for column_name in obj:
            if (type(obj[column_name]) == type({})):
                columnDict = ['stemname', 'numstems']
                for stem in obj[column_name]:
                    combinedValue = valueNotDict[:]
                    combinedValue.append(stem)
                    combinedValue.append(obj[column_name][stem])

                    value = value + str(tuple(combinedValue)) + ', '
    columnNotDict += columnDict
    column = str(tuple(columnNotDict))
    column = column.replace("'", "")
    value = value.strip(", ")
    table_name = json[0]["name"]
    insert_query = "INSERT INTO {} {} VALUES {};".format(table_name, column, value)

    return insert_query


def updateTableQuery(table_name, attr_dict, condition=None):
    """
    This is a helper function that returns the UPDATE statement
    """
    statement = ''
    for keys in attr_dict:
        statement = statement + keys + ' = ' + attr_dict[keys] + ', '
    statement = statement.strip(", ")
    if (condition == ()):
        new_query = "UPDATE {} SET {};".format(table_name, statement)
    else:
        new_query = "UPDATE {} SET {} WHERE {};".format(table_name, statement, condition)
    return new_query


def alterTableQuery(table_name, column_name, column_type):
    """
    This is a helper function that returns the ALTER statement
    """

    alter_query = "ALTER TABLE {} ADD {} {};".format(table_name, column_name, column_type)
    return alter_query


def countall(username, pass_word):
    conn = getEtokenConnection(username, pass_word)
    cursor = conn.cursor()
    cursor.execute("select count(*) from usertokens;")
    return(cursor.fetchone()[0])


def countvirtual(username, pass_word):
    conn = getEtokenConnection(username, pass_word)
    cursor = conn.cursor()
    cursor.execute("select count(*) from usertokens where productname like '%virtual%';")
    return(cursor.fetchone()[0])


def countServiceProvider(username, pass_word):
    conn = getSQLConnection(username, pass_word)
    cursor = conn.cursor()
    cursor.execute("select count(*) from splist;")
    return(cursor.fetchone()[0])


def countNormal(username, pass_word):
    conn = getEtokenConnection(username, pass_word)
    cursor = conn.cursor()
    cursor.execute("select count(*) from usertokens where productname like '%virtual%';")
    virtual = cursor.fetchone()[0]
    cursor.execute("select count(*) from usertokens;")
    all = cursor.fetchone()[0]
    normal = int(all) - int(virtual)
    return(normal)


def showVirtualUsers(username, pass_word):
    conn = getEtokenConnection(username, pass_word)
    cursor = conn.cursor()
    cursor.execute(
        "select distinct utorid from myusers join usertokens on myusers.oid = usertokens.useroid where usertokens.productname like '%virtual%';")
    all = cursor.fetchall()
    for i in all:
        print(i[0])


def numExpiring(username, pass_word):
    conn = getEtokenConnection(username, pass_word)
    cursor = conn.cursor()
    cursor.execute(
        "select distinct utorid from myusers join usertokens on myusers.oid = usertokens.useroid where expirationdate < adddate(curdate(),14);")
    all = cursor.fetchall()
    j = 0
    for i in all:
        #print(i[0])
        j += 1
    return j


def numExpiringIn(username, pass_word, num_month):
    conn = getEtokenConnection(username, pass_word)
    cursor = conn.cursor()
    if(num_month < 0.5):
        cursor.execute(
            "select distinct utorid from myusers join usertokens on myusers.oid = usertokens.useroid where expirationdate < adddate(curdate(), 14);")
    else:
        cursor.execute(
            "select distinct utorid from myusers join usertokens on myusers.oid = usertokens.useroid where expirationdate < adddate(curdate(), {});".format(str(num_month * 30), str(num_month - 1 * 30)))
    all = cursor.fetchall()
    expiring = []
    j = 0
    for i in all:
        expiring.append(i[0])
        #print(i[0])
        j += 1
    return j

def getInventory(username, pass_word):
    conn = getEtokenConnection(username, pass_word)
    cursor = conn.cursor()
    cursor.execute("select * from inventory;")
    curr_inventory = inventory_on_20200510
    for i in cursor.fetchall()[::-1]:
        if i[0] >= baseline_date:
            curr_inventory += 300
        if i[0] < baseline_date:
            break
    cursor.execute("select * from orderhistory;")
    curr_inventory = inventory_on_20200510
    for i in cursor.fetchall()[::-1]:
        if i[0] >= baseline_date:
            curr_inventory -= i[-1]
        if i[0] < baseline_date:
            break
    return curr_inventory

def json2csv(username):
    conn = getConnection(username)
    cursor = conn.cursor()
    cursor.execute("select payload from incoming;")
    
    All = cursor.fetchall()
    csv_columns = list(All[0][0].keys())
    csv_file = "jsons.csv"
    jsons = []
    for i in All:
        if (i[0]["name"] == 'etoken'):
            jsons.append(i[0])
    try:
        with open(csv_file, 'w') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
            writer.writeheader()
            for json in jsons:
                writer.writerow(json)
    except IOError:
        print("I/O error")
