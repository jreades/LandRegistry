"""
=========================================================
Create views for each year of data
=========================================================
"""
print(__doc__)

region = 'London'
reg    = 'ldn'
region = 'North West'
reg    = 'manchester'

import psycopg2
import os

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

for y in range(1997, 2015, 5): 

    print("Year: {}".format(y))

    # get a connection, if a connect cannot be made an exception will be raised here
    conn = psycopg2.connect(cn)

    # conn.cursor will return a cursor object, you can use this cursor to perform queries
    cursor = conn.cursor()

    # Grab the SQL scripts that need to run
    q = utils.get_sql(os.path.join(approot, 'Code','SQL','Views','InflationAdjustedTransactions.sql'), {'region': region, 'reg': reg, 'year': y} )

    # execute our Query
    #print(q)
    cursor.execute(q)
    
    conn.commit()
	
    conn.close()

print("Done")