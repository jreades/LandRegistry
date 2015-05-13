"""
=========================================================
NOT WORKING YET. STILL IN PROCESS OF BEING MIGRATED.
WILL BE WORKING ON FULL REFRESH FIRST.
Extract a Land Registry data from the web site and 
update the core and aggregate tables in the 'warehouse'
=========================================================
"""
print(__doc__)

import numpy as np
import pylab as pl
import psycopg2
import pydot
import datetime
import urllib
import gzip
import csv
import os

from dateutil.relativedelta import relativedelta

# The key! We work by year so that
# we don't have to try to grab bits
# and pieces incrementally.
year = 2014

# Set the default filename that
# we'll be working with until it's
# all in the database
fn   = '-'.join(["pp",str(year)])

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
# First we have to get the maximum
# date that is already *in* the db

# Get a connection, if a connect cannot be made an exception will be raised here
conn = psycopg2.connect(cn)

# conn.cursor will return a cursor object, you can use this cursor to perform queries
cursor = conn.cursor()

# Find the max date in the main 
# table of schema
cursor.execute("SELECT max(completion_dt) FROM landreg.price_paid_fct")

# retrieve it from the database
rs = cursor.fetchall()
ts = rs[0][0]
print("Maximum loaded date is ?.".replace("?",ts.strftime("%Y-%m-%d")))

# And close the connection so that
# it doesn't go stale
conn.close()

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
if not os.path.exists('.'.join([fn,'csv','gz'])): 
	rf = urllib.URLopener()
	rf.retrieve("http://publicdata.landregistry.gov.uk/market-trend-data/price-paid-data/b/pp-2014.csv", '.'.join([fn,'csv']))
	# After downloading we can
	# compress the raw file
	gz_out = gzip.open('.'.join([fn,'csv','gz']), 'wb')
	fh_in  = open('.'.join([fn,'csv']), 'rb')
	gz_out.writelines(fh_in)
	fh_in.close()
	gz_out.close()
else:
	print("Skipping download as ??? already exists.".replace("???",'.'.join([fn,'csv','gz'])))


# Now clean the data by removing 
# useless artefacts and checking for
# simple problems. Also use our max 
# date from above to strip out anything
# that precedes our cut-off by more than
# two months (because you can have 
# transactions filter through for some 
# time!).
td = ts + relativedelta(months=-2)

afh = open('.'.join([fn,'added','csv'])
cfh = open('.'.join([fn,'changed','csv'])
dfh = open('.'.join([fn,'deleted','csv'])

added   = csv.writer(afh, 'wb'), delimiter="|", quotechar='"', quoting=csv.QUOTE_MINIMAL)
changed = csv.writer(cfh, 'wb'), delimiter="|", quotechar='"', quoting=csv.QUOTE_MINIMAL)
deleted = csv.writer(dfh, 'wb'), delimiter="|", quotechar='"', quoting=csv.QUOTE_MINIMAL)

with gzip.open('.'.join([fn,'csv','gz']), 'rb') as fh_in:
	csv_in = csv.reader(fh_in, delimiter=',', quotechar='"')
	# While reading...
	for row in csv_in:
		# Strip off { and } from identifier
		row[0] = row[0].replace("{","").replace("}","")
		# What type or record is it?
		r_type = row[-1]
		# Skip record if not greater
		if r_type == 'A':
			# Added
			# Note: there's no point filtering out
			# duplicates based on the date of the 
			# last transaction because old records
			# show up all the time.
			added.writerow(row)
		elif r_type == 'C':
			# Changed
			changed.writerow(row)
		elif r_type == 'D':
			# Deleted
			deleted.writerow(row[0])

afh.flush()
cfh.flush()
dfh.flush()
afh.close()
cfh.close()
dfh.close()

# Remove weird 2-byte chars before loading into PostgreSQL
os.system(' '.join(["iconv","-f","UTF-8","-c","-t","ascii//TRANSLIT","<",'.'.join([fn,'added','csv']),'|','/usr/bin/sort','-t','"|"','-k 3,3','-o','.'.join([fn,'added','formatted','csv']) ]))
os.system(' '.join(["iconv","-f","UTF-8","-c","-t","ascii//TRANSLIT","<",'.'.join([fn,'changed','csv']),'|','/usr/bin/sort','-t','"|"','-k 3,3','-o','.'.join([fn,'changed','formatted','csv']) ]))

# Tidy up
os.remove('.'.join([fn,'added','csv']))
os.remove('.'.join([fn,'changed','csv']))
os.remove('.'.join([fn,'added','csv']))
os.remove('.'.join([fn,'csv']))

# Update user
print("Having finished pre-processing data. Ready to load into Postgres DB.")

############################
############################
# Now we have to run the command to
# bulk-load data into the database.
# We'll take an ETL-ish approach of 
# loading everything into a temporary
# table and then copying it over from
# there.

# Get a connection, if a connect cannot be made an exception will be raised here
conn = psycopg2.connect(cn)

# conn.cursor will return a cursor object, you can use this cursor to perform queries
cursor = conn.cursor()

# Find the max date in the main 
# table of schema
cursor.execute("SELECT max(completion_dt) FROM landreg.price_paid_fct")

# retrieve it from the database
rs = cursor.fetchall()
ts = rs[0][0].strftime("%Y-%m-%d")

# And close the connection so that
# it doesn't go stale
conn.close()

