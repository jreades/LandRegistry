"""
=========================================================
Delete the erroneous data in the warehouse using the
one-time update issued by the Land Registry
=========================================================
"""
print(__doc__)

import psycopg2
import gzip
import csv
import os

# Set the default filename that
# we'll be working with until it's
# all in the database
fn   = '_'.join(["LR","Price","Paid","Corrections","2003","4","5"])

# The default string to connect to 
# Postgres database
host = 'localhost'
db   = 'LandReg'
user = ''
pwd  = ''
port = ''
cn   = "host='{0}' dbname='{1}' user='{2}' password='{3}' port={4}".format(host, db, user, pwd, port)

############################
############################
# Get a connection, if a connect cannot be made an exception will be raised here
conn = psycopg2.connect(cn)

# conn.cursor will return a cursor object, you can use this cursor to perform queries
cursor = conn.cursor()

############################
############################
# Next we have to get the data
# from the Land Registry and clean
# it up so that it's DB-friendly.

# Retrieve file from Land Registry
# Web site. But skip if it looks 
# like we've already processed the
# data by creating a compressed 
# archive.
path = '../Data'
if not os.path.exists('/'.join([path,'.'.join([fn,'csv','gz'])])): 
	print("Can't find ??? to process!".replace("???",'.'.join([fn,'csv','gz'])))

with gzip.open('/'.join([path,'.'.join([fn,'csv','gz'])]), 'rb') as fh_in:
	csv_in = csv.reader(fh_in, delimiter=',', quotechar='"')
	# While reading...
	for row in csv_in:
		tid = row[0].replace("{","").replace("}","")
		dt  = row[2].split(" ")[0]
		#print("; ".join([tid,dt]))
		# And find and delete the row
		cursor.execute("DELETE FROM landreg.price_paid_fct WHERE completion_dt=%s AND transaction_id=%s", (dt, tid))

cursor.execute("COMMIT")
fh_in.close()

# And close the connection so that
# it doesn't go stale
conn.close()

# Update user
print("Done removing extraneous data.")
