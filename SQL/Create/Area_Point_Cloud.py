"""
=========================================================
Append price paid data to randomly-generated data points
within the OS Vector Map building outlines
=========================================================
"""
print(__doc__)

import psycopg2
import csv
import os
from os import getcwd

area = 'manchester'

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

# Load the Postgres conf file
config = {}
with open(os.path.join(approot,'Code','.dbconfig')) as myfile:
    for line in myfile:
        name, var = line.partition("=")[::2]
        config[name.strip()] = var.strip().replace("'","")

cn   = "host='{0}' dbname='{1}' user='{2}' password='{3}' port={4}".format(config['host'], config['db'], config['user'], config['pwd'], config['port'])

conn = psycopg2.connect(cn)

queries = utils.get_sql_iterator(os.path.join(approot,'SQL','Create','Area_Point_Cloud.sql'))

# conn.cursor will return a cursor object, you can use this cursor to perform queries
cursor = conn.cursor()

for q in queries: 
	
	# execute our Query
	print("Executing query: " + q)
	#cursor.execute(q)
	print("     Done.")
	

conn.commit()

conn.close()
