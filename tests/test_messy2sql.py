""" Testing for Messy2SQL """

import unittest
import StringIO

import nose

import re

from messytables import (CSVTableSet, CSVRowSet, type_guess, headers_guess,
                         offset_processor, DateType, StringType,
                         DecimalType, IntegerType,
                         DateUtilType)

# isn't being imported by default
from ..messy2sql.core import Messy2SQL, MESSYTABLES_TO_SQL_DIALECT_MAPPING

class Messy2SQLTest(unittest.TestCase):
	def setUp(self):
		self.csv_file = StringIO.StringIO('''
			id,  datetime,  num, otherdate, texter
			1,   2012/2/12, 2,   02 October 2011, "hola bueno"
			2,   2012/2/12, 2,   02 October 2011, "more text here"
			2.4, 2012/2/12, 1,   1 May 2011, "further text indeed"
			foo, bar,       1000, ,
			4.3, ,          42,  24 October 2012, an even greater amount
			 ,   2012/2/12, 21,  24 December 2013, ''')

		self.rows = CSVTableSet(self.csv_file).tables[0]

		# need a messy2sql object for testing using this faked file and pretending we are using MySQL
		self.m2s = Messy2SQL(self.csv_file, db_type="mysql", table_name="test")


	def test_celltype_as_string(self):
		""" should fail unless we have proper cell types working """

		assert DateType in MESSYTABLES_TO_SQL_DIALECT_MAPPING['mysql']
		assert StringType in MESSYTABLES_TO_SQL_DIALECT_MAPPING['mysql']
		assert DecimalType in MESSYTABLES_TO_SQL_DIALECT_MAPPING['mysql']
		assert IntegerType in MESSYTABLES_TO_SQL_DIALECT_MAPPING['mysql']

	def test_create_sql_table(self):
		""" should fail unless a proper rowset is returned w/the headers and types """
		create_table_query = self.m2s.create_sql_table(self.rows)
		# create_table_query = re.sub(r'\s+', ' ', create_table_query)
		# print create_table_query

		self.assertEqual(create_table_query, \
			"CREATE TABLE test (id DECIMAL, datetime DATE, num INT, otherdate DATE, texter TEXT);", \
			"Create SQL Table did not pass.")

	def test_create_sql_db(self):
		""" should fail unless a proper database query is returned """
		self.assertEqual(self.m2s.create_sql_db("test_db"), \
			"CREATE DATABASE test_db;", \
			"Create SQL Database did not pass.") 

	def test_create_sql_insert(self):
		"""
		should fail unless sql statement to create a new table based on CSV executes
		"""
		sql_test_insert = """INSERT INTO test VALUES (1, 2012-02-12, 2, 2011-10-02, "hola bueno"), (2, 2012-02-12, 2, 2011-10-02, "more text here"), (2.4, 2012-02-12, 1, 2011-05-01, "further text indeed"), (foo, bar, 1000, , ), (4.3, , 42, 2012-10-24, "an even greater amount"), (, 2012-02-12, 21, 2013-12-24, );"""
		print sql_test_insert + "\n"

		headers = ['Decimal', 'Date', 'Integer', 'Date', 'String']
		sql_query = self.m2s.create_sql_insert(self.rows, headers=headers)
		sql_query = re.sub(r'\s+', ' ', sql_query)

		self.assertEqual(sql_query, \
			sql_test_insert, \
			"Creating INSERT statements did not pass.")

	def test_create_sql_insert_no_headers(self):
		"""
		should fail unless sql statement to create a new table based on CSV executes
		"""
		sql_test_insert = """INSERT INTO test VALUES (1, 2012-02-12, 2, 2011-10-02, "hola bueno"), (2, 2012-02-12, 2, 2011-10-02, "more text here"), (2.4, 2012-02-12, 1, 2011-05-01, "further text indeed"), (foo, bar, 1000, , ), (4.3, , 42, 2012-10-24, "an even greater amount"), (, 2012-02-12, 21, 2013-12-24, );"""

		sql_query = self.m2s.create_sql_insert(self.rows)
		sql_query = re.sub(r'\s+', ' ', sql_query)

		self.assertEqual(sql_query, \
			sql_test_insert, \
			"Creating INSERT statements did not pass.")