"""
=========================================================
Create views for each year of data in Hex Bin format
Specify the resolution so ensure that the right bin
format is used.
=========================================================
"""
print(__doc__)

import psycopg2
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
