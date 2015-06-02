"""
=========================================================
Extract full Land Registry Price Paid data set from the web site  
to update the core and aggregate tables in the data warehouse.
In the long run, this will be able to create schemas and 
tables as well as update them, but this is a start.
=========================================================
"""
print(__doc__)

# With environments like Entought Canopy you will probably
# need to 'break out' of the environment briefly to run
#
# $ pip install 'psycopg2'
#
# from the command line. This works if you've allowed Canopy
# to be the default, otherwise you need to specify the path 
# something like the following: 
#
# $ /Users/XXX/Library/Enthought/Canopy_64bit/User/bin/pip install 'psycopg2' (XXX is your username)
# $ /Users/XXX/Library/Enthought/Canopy_64bit/User/bin/pip install pyparsing==1.5.7
#
# On a Mac you may need to do the following to get psycopg2
# to install:
#
# 1. Install libssl >= 1.0.0 (see 'Requirements' for script that seems to work)
# 2. $ sudo mv /usr/lib/libpq.5.dylib /usr/lib/libpq.5.dylib.old  
# 3. $ sudo ln -s /Library/PostgreSQL/9.3/lib/libpq.5.dylib /usr/lib

import numpy as np
import pylab as pl
import psycopg2
import subprocess
import pydot
import datetime
import urllib
import gzip
import csv
import os
import re
import utils

from distutils import spawn
from dateutil.relativedelta import relativedelta

#sys.path.append('/Library/PostgreSQL/9.3/bin/')
psql_path = '/Library/PostgreSQL/9.3/bin/'

def which(program):
    import os
    def is_exe(fpath):
        return os.path.isfile(fpath) and os.access(fpath, os.X_OK)

    fpath, fname = os.path.split(program)
    if fpath:
        if is_exe(program):
            return program
    else:
        for path in os.environ["PATH"].split(os.pathsep):
            path = path.strip('"')
            exe_file = os.path.join(path, program)
            if is_exe(exe_file):
                return exe_file

    return None

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

# Where is the data stored remotely?
lrURL = "http://publicdata.landregistry.gov.uk/market-trend-data/price-paid-data/b/pp-complete.csv"

# Set the default filename that
# we'll be working with until it's
# all in the database
fn    = '-'.join(["pp","complete"])

############################
############################
# First we have to get the maximum
# date that is already *in* the db

try :
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
    print("Last transaction date in database is ?.".replace("?",ts.strftime("%Y-%m-%d")))

    # And close the connection so that
    # it doesn't go stale
    conn.close()
    
    ############################
    # Check to see if the user really wants to 
    # delete everything *if* we found a date.
    # If no, exit.
    print('It seems that you already have data in your Land Registry database, do you really want to me to reload everything?')
    proceed = raw_input('Should I proceed [y/n]: ')
    if proceed=='y': 
        print("OK, will reload everything.")
    else:
        print("OK, stopping.")
        exit()
    
except:
    # catch *all* exceptions
    e = sys.exc_info()[0]
    print("Unable to connect to database and obtain a maximum date so you probably have nothing that I can overwrite.")
    proceed = raw_input('Should I definitely proceed [y/n]: ')
    if proceed=='y': 
        print("OK, will load everything.")
    else:
        print("OK, stopping.")
        exit()


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
if not os.path.isdir(datroot):
    print "Creating data directory..."
    os.mkdir(datroot)
    
if not os.path.exists(os.path.join(datroot, '.'.join([fn,'csv','gz']))):
    print("Can't find local copy of " + '.'.join([fn,'csv','gz']) + " so will go ahead and download...")
    rf = urllib.URLopener()
    rf.retrieve(lrURL, os.path.join(datroot, '.'.join([fn,'csv'])))
    print("Downloaded, starting on compression...")
    
    # After downloading we can
    # compress the raw file	
    gz_out = gzip.open(os.path.join(datroot, '.'.join([fn,'csv','gz'])), 'wb')
    fh_in  = open(os.path.join(datroot, '.'.join([fn,'csv'])), 'rb')
    gz_out.writelines(fh_in)
    fh_in.close()
    gz_out.close()
    print("Compressed via gzip.")
    
else:
    print("Skipping download as ??? already exists.".replace("???",'.'.join([fn,'csv','gz'])))


if not os.path.exists(os.path.join(datroot, '.'.join([fn,'formatted','csv']))):
    afh = open(os.path.join(datroot, '.'.join([fn,'added','csv'])), 'w+')
    added = csv.writer(afh, delimiter="|", quotechar='"', quoting=csv.QUOTE_MINIMAL)

    with gzip.open(os.path.join(datroot, '.'.join([fn,'csv','gz'])), 'rb') as fh_in:
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
	       print("Found changed record ('C'), which shouldn't be in full Price Paid file: " + row[0])
	       exit()
	   elif r_type == 'D':
	       # Deleted
	       print("Found deleted record ('D'), which shouldn't be in full Price Paid file: " + row[0])
	       exit()
	   else:
	       print("Unexpected record type ('" + r_type + "') in full Price Paid file: " + row[0])
	       exit();

    afh.flush()
    afh.close()
    
    # Remove weird 2-byte chars before loading into PostgreSQL
#    os.system(' '.join(["iconv","-f","UTF-8","-c","-t","ascii//TRANSLIT","<",os.path.join(localPath, '.'.join([fn,'added','csv'])),'|','/usr/bin/sort','-t','"|"','-k 3,3','-o',os.path.join(localPath, '.'.join([fn,'formatted','csv'])) ]))
    try:
        os.system(' '.join(["iconv","-f","UTF-8","-c","-t","ascii//TRANSLIT","<",os.path.join(datroot, '.'.join([fn,'added','csv'])),'|','/usr/bin/sort','-t','"|"','-k 3,3','|','/usr/bin/uniq','>',os.path.join(datroot, '.'.join([fn,'formatted','csv'])) ]))
    
        # Tidy up
        os.remove(os.path.join(datroot, '.'.join([fn,'added','csv'])))
        os.remove(os.path.join(datroot, '.'.join([fn,'csv'])))
    except: 
        e = sys.exc_info()[0]
        print("Possible problem fixing 2-byte characters or tidying up")
        print e
    
else:
    print("Found existing copy of ???".replace("???",'.'.join([fn,'formatted','csv'])))
    proceed = raw_input('Do you want me to overwrite this? [y/n]: ')
    if proceed=='y':
        print("For safety, you need to delete this manually before I can do anything")
        exit()
    else:
        print("Great, will go ahead using the existing file")

# Update user
print("Have finished pre-processing data. Ready to load into Postgres DB.")

############################
############################
# Now we have to run the command to
# bulk-load data into the database.
# We'll take an ETL-ish approach of
# loading everything into a temporary
# table and then copying it over from
# there.

# Execute the query using psql instead
# of SQL -- this is needed to get around
# a permissions issue (COPY runs as the 
# server, while the psql \copy runs as
# the local user).
q = " ".join(["\copy","landreg.loader_fct","FROM","".join(["'",os.path.join(datroot,'.'.join([fn,'formatted','csv'])),"'"]),"WITH","DELIMITER '|'"])
subprocess.call([''.join([psql_path,'psql']),'-h',config['host'],'-d',config['db'],'-U',config['user'],'-w','-p',config['port'],'-c',q])

# And now we can tidy up the last large files
os.remove(os.path.join(datroot, '.'.join([fn,'formatted','csv'])))
os.remove(os.path.join('tmp','table.csv'))

#############################
#############################
# Now we can bulk copy into the 
# price_paid_fct -- but this is a 
# little trickier since we will need
# to rebuild the indexes afterwards
# (as this is much faster than inserting
# while the indexes are active)

# Update the Price Paid Fact -- this is the 
# big one to do, so try to avoid re-running
# this unless absolutely necessary.
conn = psycopg2.connect(cn)
update_ppf = get_sql(os.path.join(approot,'Code','SQL','Load','Full.sql'))
cursor = conn.cursor()
proceed = raw_input('Do you want me to overwrite the entire price paid fact? [y/n]: ')
if proceed=='y':
    cursor.execute(update_ppf)
conn.close()

# And now run the scripts to create the
# relevant materialised views and subsidiary
# tables.
conn = psycopg2.connect(cn)
cursor = conn.cursor()
proceed = raw_input('Do you want me to rebuild the aggregate views? [y/n]: ')
if proceed=='y':
    print "Rebuilding annual views..."
    annual_views = utils.get_sql(os.path.join(approot,'Code','SQL','Load','Annual.sql'))
    #print annual_views
    cursor.execute(annual_views)
    print "Rebuilding monthly views..."
    quaterly_views = utils.get_sql(os.path.join(approot,'Code','SQL','Load','Quarterly.sql'))
    #print quarterly_views
    cursor.execute(quaterly_views)
conn.close()