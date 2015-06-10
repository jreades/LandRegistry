"""
=========================================================
Create views for each year of data
=========================================================
"""
print(__doc__)

import psycopg2
import csv

region = 'London'
reg    = 'ldn'

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

# Load the Postgres conf file
config = {}
with open(os.path.join(approot,'Code','.dbconfig')) as myfile:
    for line in myfile:
        name, var = line.partition("=")[::2]
        config[name.strip()] = var.strip().replace("'","")

cn   = "host='{0}' dbname='{1}' user='{2}' password='{3}' port={4}".format(config['host'], config['db'], config['user'], config['pwd'], config['port'])

# Grab the SQL scripts that need to run
q = utils.get_sql('Standard.sql', {'region': region, 'reg', reg} )

for y in range(1996, 2013): 

	print("Year {}".format(y))

	# get a connection, if a connect cannot be made an exception will be raised here
	conn = psycopg2.connect(cn)

	# conn.cursor will return a cursor object, you can use this cursor to perform queries
	cursor = conn.cursor()

	# execute our Query
	cursor.execute(q.replace("{year}",str(y)))
	
	conn.commit()
	
conn.close()
