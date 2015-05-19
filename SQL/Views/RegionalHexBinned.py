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

# The default string to connect to 
# Postgres database
try:
    root = os.path.dirname(os.path.abspath(__file__))
except NameError:  # We are the main py2exe script, not a module
    import sys
    root = os.path.dirname(os.path.abspath(sys.argv[0]))

print(root)

os.chdir('../../')
approot = '.'
approot 
config = {}
with open(os.path.join(approot,'.dbconfig')) as myfile:
    for line in myfile:
        name, var = line.partition("=")[::2]
        config[name.strip()] = var.strip().replace("'","")

cn   = "host='{0}' dbname='{1}' user='{2}' password='{3}' port={4}".format(config['host'], config['db'], config['user'], config['pwd'], config['port'])

print(cn)

resolution = 750

def multiple_replace(dict, text): 

  """ Replace in 'text' all occurences of any key in the given
  dictionary by its corresponding value.  Returns the new tring.""" 

  # Create a regular expression  from the dictionary keys
  regex = re.compile("(%s)" % "|".join(map(re.escape, dict.keys())))

  # For each match, look-up corresponding value in dictionary
  return regex.sub(lambda mo: dict[mo.string[mo.start():mo.end()]], text) 

# Grab the SQL scripts that need to run
subs = {'{resolution}': '{}m'.format(resolution)}

q = ""
with open(os.path.join(approot,"SQL","Views","RegionalHexBinned.sql"), 'r') as fh: 
	q = fh.read()

conn = psycopg2.connect(cn)

# conn.cursor will return a cursor object, you can use this cursor to perform queries
cursor = conn.cursor()
	
#print multiple_replace(subs, q)

# execute our Query
#print multiple_replace(subs, q).replace("\\n","\n")
#cursor.execute("SELECT DISTINCT(initcap(region_nm)) FROM inflation.hh_income_fct WHERE region_nm NOT IN ('All households4','Northern Ireland','Scotland')")
cursor.execute("SELECT distinct(region_nm) FROM inflation.hh_income_fct WHERE region_nm LIKE 'Yorkshire%'")
rows = cursor.fetchall()
	
conn.close()

for r in range(0, len(rows)): 
    
    region = rows[r][0]
    print(region)
    subs['{region_nm}'] = region
    subs['{region}'] = region.replace(" ","_").replace("-","_")

    for y in range(1996, 2016, 4): 

	print("Year {}".format(y))
	subs['{year}'] = str(y)

	# get a connection, if a connect cannot be made an exception will be raised here
	conn = psycopg2.connect(cn)

	# conn.cursor will return a cursor object, you can use this cursor to perform queries
	cursor = conn.cursor()

	# execute our Query
	#print multiple_replace(subs, q).replace("\\n","\n")
	cursor.execute(multiple_replace(subs, q).replace("\\n","\n"))
	
	conn.commit()
	
	conn.close()


