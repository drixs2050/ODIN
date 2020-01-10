from create import *
import unittest

class TestCreateTableMethods(unittest.TestCase):
	
	def test_createTableWithOneColumn(self):
		self.assertEqual(createTableQuery('etoken', {'user_count': 'int'}), 'CREATE TABLE etoken (user_count int);')
		print('CREATE TABLE etoken (user_count int);')
	
	def test_createTableWithMultipleColumn(self):
		self.assertEqual(createTableQuery('zebra', {'population': 'varchar(255)', 'age': 'int', 'location': 'varchar(255)'}), 'CREATE TABLE zebra (age int, location varchar(255), population varchar(255));')
		print('CREATE TABLE zebra (age int, location varchar(255), population varchar(255));')
class TestInsertTableMethods (unittest.TestCase):
	
	def test_insertTableWithOneColumn(self):
		self.assertEqual(insertTableQuery('persons', {'personid': '5'}), 'INSERT INTO persons (personid) VALUES (5);')
		print('INSERT INTO persons (personid) VALUES (5);')
	def test_insertTableWithMultipleColumn(self):
		self.assertEqual(insertTableQuery('persons', {'personid': '5', 'lastname':'park'}), 'INSERT INTO persons (personid, lastname) VALUES (5, park);')
		print('INSERT INTO persons (personid, lastname) VALUES (5, park);')
class TestAlterTableMethods (unittest.TestCase):

	def test_alterTableAddColumn (self):
		self.assertEqual(alterTableQuery('etoken', {'ID': 'int'}), "ALTER TABLE etoken ADD ID int;")
		print("ALTER TABLE etoken ADD ID int;")
		
class TestSelectMethods (unittest.TestCase):
	
	def test_selectWhenOneColumnPresentAndConditionEmpty(self):
		self.assertEqual(selectTableQuery('etoken', 'ID'), "SELECT ID FROM etoken")
		print("SELECT ID FROM etoken")
		
	def test_selectWhenMulitpleColumnPresentAndConditionEmpty(self):
		self.assertEqual(selectTableQuery('etoken', 'ID, user_count'), "SELECT ID, user_count FROM etoken")
		print("SELECT ID, user_count FROM etoken")
	
	def test_selectWhenVariableAndConditionPresent(self):
		self.assertEqual(selectTableQuery('etoken', 'ID, user_count', 'ID = 4 AND user_count > 4'), "SELECT ID, user_count FROM etoken WHERE ID = 4 AND user_count > 4")
		print("SELECT ID, user_count FROM etoken WHERE ID = 4 AND user_count > 4")
	
	def test_selectWhenColumnAndConditionEmpty(self):
		self.assertEqual(selectTableQuery('etoken'), "SELECT * FROM etoken")
		print("SELECT * FROM etoken")
		
if __name__ == '__main__':
	unittest.main()
	
