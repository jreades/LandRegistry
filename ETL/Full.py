"""
=========================================================
Extract the full Land Registry Price Paid data set from the web site  
to populate the core and aggregate tables in the data warehouse.
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

import psycopg2
import subprocess
import datetime
import os
import petl as etl
import utils

import sys

reload(sys)  
sys.setdefaultencoding('utf8')

from petl import look

from distutils import spawn
from dateutil.relativedelta import relativedelta

# Place to store the data locally
fn   = '-'.join(["pp","complete"]) #,str(today.year),str(today.month).zfill(2)])

#sys.path.append('/Library/PostgreSQL/9.3/bin/')
psql_path = '/Library/PostgreSQL/9.3/bin/'

# Configure the application with the appropriate
# details
try:
    root = os.path.dirname(os.path.abspath(__file__))
except NameError:  # We are the main py2exe script, not a module
    import sys
    root = os.path.dirname(os.path.abspath(sys.argv[0]))

approot = root.replace('/Code/ETL','')
etlroot = os.path.join(approot,'Code','ETL')
datroot = os.path.join(approot,'Data')
os.chdir(approot)

# Load the Postgres conf file
config = {}
with open(os.path.join(approot,'Code','.dbconfig')) as myfile:
    for line in myfile:
        name, var = line.partition("=")[::2]
        config[name.strip()] = var.strip().replace("'","")

cn   = "host='{0}' dbname='{1}' user='{2}' password='{3}' port={4}".format(config['host'], config['db'], config['user'], config['pwd'], config['port'])

# Where is the data stored remotely?
lrURL = "http://publicdata.landregistry.gov.uk/market-trend-data/price-paid-data/b/pp-complete.csv"

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
    # Create the source and give it headers since Land Registry don't
    src  = etl.pushheader(etl.io.fromcsv(lrURL,'utf-8'), ['transaction_id','price_int','completion_dt','pc','property_type_cd','new_build_cd','tenure_cd','paon','saon','street_nm','locality_nm','town_nm','authority_nm','county_nm','status_cd'])
    # Write to a local file
    etl.csv.tocsv(src, source=os.path.join(datroot,'.'.join([fn,'tmp','csv','gz'])), encoding='utf8', write_header=True)
    print "File saved locally to avoid too much in memory..."
    
    del(src)
    
    # Tidy up some of the fields so that they're db-friendly
    tidy = etl.io.fromcsv(os.path.join(datroot,'.'.join([fn,'tmp','csv','gz']))).convert('transaction_id','replace','{','').convert('transaction_id','replace','}','').convert('price_int',int).convert('completion_dt',lambda v: datetime.datetime.strptime(v, "%Y-%m-%d 00:00").date()).sort('completion_dt')

    # Summarise what's there (helpful for tracking
    # changes to the format, especially the status codes).
    print "There are {} rows of data.".format(etl.util.counting.nrows(tidy))
    counts = etl.util.counting.valuecounts(tidy, 'status_cd')
    print "I found the following record types and counts:"
    print counts
    confs  = etl.conflicts(tidy, key='transaction_id')
    if confs.nrows() > 0:
        print "I found the following conflicts:"
        print confs
    else:
        print "I found no conflicting Transaction IDs"  

    proceed = raw_input('Given these stats should I proceed with the processing [y/n]: ')
    if proceed=='y': 
        print("OK, will load the data.")
    else:
        print("OK, stopping.")
        exit()

    etl.csv.totsv(tidy, source=os.path.join(datroot,'.'.join([fn,'.csv'])), encoding='utf-8', write_header=True)
    print "Foo!"
    
    os.remove(os.path.join(datroot,'.'.join([fn,'tmp','.csv.gz'])))
    
else:
    print("Skipping download as ??? already exists.".replace("???",'.'.join([fn,'csv','gz'])))
    
# Remove weird 2-byte chars before loading into PostgreSQL
# os.system(' '.join(["iconv","-f","UTF-8","-c","-t","ascii//TRANSLIT","<",os.path.join(localPath, '.'.join([fn,'added','csv'])),'|','/usr/bin/sort','-t','"|"','-k 3,3','-o',os.path.join(localPath, '.'.join([fn,'formatted','csv'])) ]))
try:
    #os.system(' '.join(["iconv","-f","UTF-8","-c","-t","ascii//TRANSLIT","<",os.path.join(datroot, '.'.join([fn,'added','csv'])),'|','/usr/bin/sort','-t','"|"','-k 3,3','|','/usr/bin/uniq','>',os.path.join(datroot, '.'.join([fn,'formatted','csv'])) ]))
    
    # Tidy up
    #os.remove(os.path.join(datroot, '.'.join([fn,'added','csv'])))
    #os.remove(os.path.join(datroot, '.'.join([fn,'csv'])))
    pass
except: 
    e = sys.exc_info()[0]
    print("Possible problem fixing 2-byte characters or tidying up")
    print e
    

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
q = " ".join(["\copy","landreg.loader_fct","FROM","".join(["'",os.path.join(datroot,'.'.join([fn,'csv'])),"'"]),"WITH","DELIMITER '\t'"])
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
update_ppf = utils.get_sql(os.path.join(approot,'Code','SQL','Load','Full.sql'))
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
    # Shouldn't be necessary, but may be
    conn.commit() 
    print "Rebuilding monthly views..."
    quaterly_views = utils.get_sql(os.path.join(approot,'Code','SQL','Load','Quarterly.sql'))
    #print quarterly_views
    cursor.execute(quaterly_views)
    # Shouldn't be necessary, but may be
    conn.commit() 
conn.close()