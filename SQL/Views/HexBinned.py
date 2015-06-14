"""
=========================================================
Create views for each year of data in Hex Bin format
Specify the resolution so ensure that the right bin
format is used.
=========================================================
"""
print(__doc__)

import psycopg2
import csv
import re
import os

# What size grid
resolution = 750

#sys.path.append('/Library/PostgreSQL/9.3/bin/')
psql_path = '/Library/PostgreSQL/9.3/bin/'

# Configure the application with the appropriate
# details
try:
    root = os.path.dirname(os.path.abspath(__file__))
except NameError:  # We are the main py2exe script, not a module
    import sys
    root = os.path.dirname(os.path.abspath(sys.argv[0]))

approot = root.split('/Code')[0]
etlroot = os.path.join(approot,'Code','ETL')
datroot = os.path.join(approot,'Data')
os.chdir(approot)

sys.path.append(etlroot)
import utils

# Grab the SQL scripts that need to run
subs = {'{resolution}': '{}m'.format(resolution)}

q = ""
with open(, 'r') as fh: 
	q = fh.read()


for y in range(1996, 2016): 

	print("Year {}".format(y))
	subs['{year}'] = str(y)

	# get a connection, if a connect cannot be made an exception will be raised here
	conn = psycopg2.connect(cn)

	# conn.cursor will return a cursor object, you can use this cursor to perform queries
	cursor = conn.cursor()
	
	utils.get_sql(os.path.join(approot,"SQL","Views","HexBinned.sql"), {'year': y, 'resolution': resolution })
	
	# execute our Query
	print q
	#cursor.execute(q))
	
	conn.commit()
	
	conn.close()


