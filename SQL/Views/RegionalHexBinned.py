"""
=========================================================
Create views for each year of data in Hex Bin format
Specify the resolution so ensure that the right bin
format is used.
=========================================================
"""
print(__doc__)

import numpy as np
import psycopg2
#import pydot
import csv
import re
import os

# See also need to configure region/region_nm
# below for the queries to run
resolution = 750

# The default string to connect to 
# Postgres database
#sys.path.append('/Library/PostgreSQL/9.3/bin/')
psql_path = '/Library/PostgreSQL/9.3/bin/'

# Configure the application with the appropriate
# details
try:
    root = os.path.dirname(os.path.abspath(__file__))
except NameError:  # We are the main py2exe script, not a module
    import sys
    root = os.path.dirname(os.path.abspath(sys.argv[0]))

os.chdir(root.replace('/Code/ETL',''))
approot = os.chdir(root.replace('/Code/ETL',''))
etlroot = os.path.join(approot,'Code','ETL')
datroot = os.path.join(approot,'Data')

def multiple_replace(dict, text): 

  """ Replace in 'text' all occurences of any key in the given
  dictionary by its corresponding value.  Returns the new tring.""" 

  # Create a regular expression  from the dictionary keys
  regex = re.compile("(%s)" % "|".join(map(re.escape, dict.keys())))

  # For each match, look-up corresponding value in dictionary
  return regex.sub(lambda mo: dict[mo.string[mo.start():mo.end()]], text) 

# Grab the SQL scripts that need to run
subs = {'{resolution}': '{}m'.format(resolution)}

conn = psycopg2.connect(cn)

# conn.cursor will return a cursor object, you can use this cursor to perform queries
cursor = conn.cursor()

# execute our Query
cursor.execute("SELECT DISTINCT(initcap(region_nm)) FROM inflation.hh_income_fct WHERE region_nm NOT IN ('All households4','Northern Ireland','Scotland')")
#cursor.execute("SELECT distinct(region_nm) FROM inflation.hh_income_fct WHERE region_nm LIKE 'Yorkshire%'")
rows = cursor.fetchall()
	
conn.close()

for r in range(0, len(rows)): 
    
	region = rows[r][0]
  print(region)

  for y in range(1996, 2016, 4): 

		# get a connection, if a connect cannot be made an exception will be raised here
		conn = psycopg2.connect(cn)

		# conn.cursor will return a cursor object, you can use this cursor to perform queries
		cursor = conn.cursor()
		
		q = utils.get_sql(os.path.join(approot,"SQL","Views","RegionalHexBinned.sql"), {'resolution': resolution, 'year': y, 'region_nm': region, 'region': region.replace(" ","_").replace("-","_")})

		# execute our Query
		print q
		#cursor.execute(q)
	
		conn.commit()
	
		conn.close()


