"""
MessyTables does great detection of disparate file types to its own set of internal MessyTables types
Messy2SQL generates SQL from MessyTypes

Messy2SQL returns SQL strings 
"""

# Python standard libraries
import os, re
from dateutil import parser

# External libraries
import messytables


"""
Need a slightly different 'dialect' to be spoken depending on which db type we are
wanting to insert into


NEED TO INCREASE GRANULARITY HERE AND DO SOME GUESSING BASED ON SAMPLING SO WE CAN BE
EFFICIENT WITH FILE SPACE INSTEAD OF USING FULL SPACE
"""
MESSYTABLES_TO_SQL_DIALECT_MAPPING = {
	'mysql': {
		# messytables.StringType: {
		# 						'char': 'VARCHAR(255)',
		# 						'text': 'TEXT',
		# 					}, 
		messytables.StringType: 'TEXT', # this is temp, return to above when working
		messytables.IntegerType: 'INT',
		messytables.FloatType: 'FLOAT',
		messytables.DecimalType: 'DECIMAL',
		messytables.DateType: 'DATE',
		# messytables.DateUtilType: 'date'
	},
	'postgres': {
		messytables.StringType: 'text', # this is temp, return to above when working
		messytables.IntegerType: 'integer',
		messytables.FloatType: 'real',
		messytables.DecimalType: 'decimal',
		messytables.DateType: 'timestamp',
	},
	'sqlite': {
		messytables.StringType: 'text', # this is temp, return to above when working
		messytables.IntegerType: 'integer',
		messytables.FloatType: 'numeric',
		messytables.DecimalType: 'numeric',
		messytables.DateType: 'text',
	},
	'sqlserver': {
		messytables.StringType: 'varchar(255)', # this is temp, return to above when working
		messytables.IntegerType: 'int',
		messytables.FloatType: 'float',
		messytables.DecimalType: 'decimal',
		messytables.DateType: 'date',
	},
	'access': {
		messytables.StringType: 'memo', # this is temp, return to above when working
		messytables.IntegerType: 'Integer',
		messytables.FloatType: 'Double',
		messytables.DecimalType: 'Double',
		messytables.DateType: 'Date/Time',
	}
}

class Messy2SQL:
	"""
	Should be able to input any kind of MessyTables data and output SQL create/insert
	"""

	messy_file = ""
	messytables_to_sql_mapping = {}
	headers = []
	header_types = []

	def __init__(self, messy_file, db_type='postgres', table_name=None):
		"""
		Takes the messy file in and reads it, designate a db_type to get your sql in that dialect
		"""

		self.messy_file = messy_file

		# if table name is not specified, use the name of the file
		if not table_name:
			self.table_name = os.path.splitext(messy_file)[0]
		else:
			self.table_name = table_name

		# Select the correct dialect to translate into using the mapping dictionary
		self.messytables_to_sql_mapping = MESSYTABLES_TO_SQL_DIALECT_MAPPING[db_type]

	def celltype_as_string(self, celltype):
		"""
		Basic conversion of MessyTables type to SQL typing
		"""
		# PLACEHOLDER: may need a conditional here to check whether this is a text field, if so, how long?
		return self.messytables_to_sql_mapping[celltype.__class__]


	def create_sql_table(self, rowset, sql_table_name=None, headers=None, types=None):
		"""
		Create a SQL table schema from a MessyTables RowSet
		"""

		# if a different name isn't specified, use the primary root name
		if not sql_table_name:
			sql_table_name = self.table_name
		
		# we don't care about the offset returned, so just throw it away, get headers
		_, headers = messytables.headers_guess(rowset.sample)
		types = map(self.celltype_as_string, messytables.type_guess(rowset.sample, strict=False))
		
		self.headers = headers
		self.header_types = types

		return self.headers_and_typed_as_sql(sql_table_name, headers, types)

	def headers_and_typed_as_sql(self, table_name, headers, types):
		"""
		Returns the actual SQL assembly of the headers & types combined
		"""

		# TODO: NEED TO MODIFY TO MAKE SURE THAT THERE AREN'T DUPLICATE LABELS

		# Need to iterate over the types found and create a string that will go into the SQL query 
		# in the form of 'column_name column_type, column_name column_type'
		insert_string = ""
		for i in range(len(headers)):
			insert_string += "%s %s, " % (headers[i], types[i])
		
		# return create table sql query, make sure to remove trailing commas
		return "CREATE TABLE %s (%s);" % (table_name, insert_string.strip().rstrip(', '))


	def create_sql_db(self, db_name=None):
		"""
		Create a SQL db based on name of file.
		Need to make this more flexible to handle more complicated db create queries
		"""

		if not db_name:
			db_name = os.path.splitext(messy_file)[0]

		return "CREATE DATABASE %s;" % db_name

	def create_sql_insert(self, rowset, headers=None, table_name=None):
		"""
		Use the actual data to create a SQL insert statement for every line...
		"""

		# use table name only if it is specified
		if not table_name:
			table_name = self.table_name

		# I THINK I ONLY WANT TO TAKE STREAM, FORCE INTO DETECTED TYPES
		# YEP, JUST NEED TO GET DETECTED TYPES FROM THE HEADERS!
		offset, _ = messytables.headers_guess(rowset.sample)

		# if headers aren't specified by user
		if not headers:
			headers = messytables.type_guess(rowset.sample, strict=False) # grab header types if not specified

		# # for each row in the table, make an insert row.
		# # add one to begin with content, not the header:
		rowset.register_processor(messytables.offset_processor(offset + 1))

		# # guess column types:
		# types = messytables.type_guess(rowset.sample, strict=True)		
		
		# # and tell the row set to apply these types to
		# # each row when traversing the iterator:
		# rowset.register_processor(messytables.types_processor(types))

		# now run some operation on the data:
		value_rows = ""

		for row in rowset:
			
			# need to start a SQL row
			value_rows += "("

			i = 0
			for cell in row:
				# need to convert to a string no matter what...
				# need to add quotes around StringTypes specifically
				
				# gotta reset this so we don't get weird behavior
				value = ""

				# need to force types to headers
				if str(headers[i]) == "String":
					if cell.value:
						value = str(cell.value.strip())

						Q = '"'
						re_quoted_items = re.compile(r'" \s* [^"\s] [^"]* \"')

						woqi = re_quoted_items.sub('', value)

						if len(value) == 0:
							value = Q + value + Q
						elif len(woqi) > 0 and not (woqi[0] == Q and woqi[-1] == Q):
							value = Q + value + Q
						
						# check start and end of string for quotes
						# if not (cell.value[0] == '"' or cell.value[0]) == "'":
						# 	value = '"' + value
						# if not (cell.value[-1] == '"' or cell.value[-1]) == "'":
						# 	value = value + '"'
						
				elif "Date" in str(headers[i]):
					# Use dateutil parse to turn into a python date time regardless of what it comes in as
					if cell.value:
						try:
							value = parser.parse(cell.value)
						except ValueError:
							value = None

						# Don't add a bunch of null-ness for times if unnecessary
						if value and value.time():
							value = value.strftime('%Y-%m-%d %H:%M:%S')
						elif value:
							value = value.strftime('%Y-%m-%d')
						else:
							value = cell.value
					else:
						value = cell.value
				elif cell.value == None:
					value = ""
				else:
					value = str(cell.value)

				value_rows += value.strip() + ", "

				i += 1

				# ALSO NEED TO DO SOMETHING TO FIX THE DARN DATE INSERTS!

			# remove trailing commas
			value_rows = value_rows.strip()[:-1]
			
			# and end row...
			value_rows += "), "

		return "INSERT INTO %s VALUES %s;" % (table_name, value_rows.strip().rstrip(', '))
