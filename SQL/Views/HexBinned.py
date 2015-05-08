"""
=========================================================
Create views for each year of data in Hex Bin format
=========================================================
"""
print(__doc__)

import numpy as np
import psycopg2
#import pydot
import csv
import re

# The default string to connect to 
# Postgres database
host = 'localhost'
db   = 'LandReg'
user = ''
pwd  = ''
port = ''
cn   = "host='{0}' dbname='{1}' user='{2}' password='{3}' port={4}".format(host, db, user, pwd, port)

resolution = 750

def multiple_replace(dict, text): 

  """ Replace in 'text' all occurences of any key in the given
  dictionary by its corresponding value.  Returns the new tring.""" 

  # Create a regular expression  from the dictionary keys
  regex = re.compile("(%s)" % "|".join(map(re.escape, dict.keys())))

  # For each match, look-up corresponding value in dictionary
  return regex.sub(lambda mo: dict[mo.string[mo.start():mo.end()]], text) 

# Grab the SQL scripts that need to run
subs = {'{resolution}': '{}m'.format{resolution}}

q = ""
with open("HexBinned.sql", 'r') as fh: 
	q = fh.read()


for y in range(1996, 2013): 

	print("Year {}".format(y))
	subs['{year}'] = str(y)

	# get a connection, if a connect cannot be made an exception will be raised here
	conn = psycopg2.connect(cn)

	# conn.cursor will return a cursor object, you can use this cursor to perform queries
	cursor = conn.cursor()
	
	#print multiple_replace(subs, q)

	# execute our Query
	#print multiple_replace(subs, q).replace("\\n","\n")
	cursor.execute(multiple_replace(subs, q).replace("\\n","\n"))
	
	conn.commit()
	

conn.close()