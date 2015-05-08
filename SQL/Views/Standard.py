"""
=========================================================
Create views for each year of data
=========================================================
"""
print(__doc__)

import numpy as np
import psycopg2
#import pydot
import csv

# The default string to connect to 
# Postgres database
host = 'localhost'
db   = 'LandReg'
user = ''
pwd  = ''
port = ''
cn   = "host='{0}' dbname='{1}' user='{2}' password='{3}' port={4}".format(host, db, user, pwd, port)

# Grab the SQL scripts that need to run
q = ""
with open("Standard.sql", 'r') as fh: 
	q = fh.read()


for y in range(1996, 2013): 

	print("Year {}".format(y))

	# get a connection, if a connect cannot be made an exception will be raised here
	conn = psycopg2.connect(cn)

	# conn.cursor will return a cursor object, you can use this cursor to perform queries
	cursor = conn.cursor()

	# execute our Query
	cursor.execute(q.replace("{}",str(y)).replace("\\n","\n"))
	
	conn.commit()
	
conn.close()
