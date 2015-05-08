"""
=========================================================
Create hex map and dump it to the Desktop
=========================================================
"""
print(__doc__)

import numpy as np
import psycopg2
#import pydot
import csv
from subprocess import call
import os

resolution = 750

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
with open("HexBinning.sql", 'r') as fh: 
	q = fh.read()


# get a connection, if a connect cannot be made an exception will be raised here
conn = psycopg2.connect(cn)

# conn.cursor will return a cursor object, you can use this cursor to perform queries
cursor = conn.cursor()

# execute our Query
cursor.execute(q.replace("\\n","\n"))
	
conn.commit()

conn.close()

os.system(' '.join(["pg_dump","-h {0} -d {1} -p {2} -U {3} -W {4} -t public.hex_{5}m", "|", "/usr/bin/gzip","-c > hex_{5}m.sql.gz"]).format(host, db, port, user, pwd, resolution)
