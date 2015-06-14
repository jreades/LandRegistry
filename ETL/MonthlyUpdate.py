"""
=========================================================
Extract a Land Registry monthly update file from the web site  
to update the core and aggregate tables in the 'warehouse'
=========================================================
"""
print(__doc__)

import psycopg2
import datetime
import gzip
import csv
import os
import petl as etl
import utils

from petl import look

# Set the default filename that
# we'll be working with until it's
# all in the database
today = datetime.date.today() - datetime.timedelta(weeks=8)

# Place to store the data locally
fn   = '-'.join(["pp",str(today.year),str(today.month).zfill(2)])

# Path to Postgres functions
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
lrURL = "http://publicdata.landregistry.gov.uk/market-trend-data/price-paid-data/a/pp-monthly-update-new-version.csv"

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

if os.path.exists(os.path.join(datroot,'-'.join([fn,'Added.csv.gz']))):
    prompt  = 'Found file {} should I proceed [y/n]: '.format('-'.join([fn,'Added.csv.gz']))
    proceed = raw_input(prompt)
    
    if proceed=='y': 
        print("OK, will continue to load the data.")
    else:
        print("OK, stopping.")
        exit()

########################
########################
# Use the PETL syntax to accomplish the following:
# 1. Create a new source table linked to the remote CSV file and give it a header
# 2. Apply some simple transforms to fields in the CSV file
# 3. Do some basic checks and counts to help the user understand what's likely to happen
# 4. Prompt for the process to continue or stop

# Create the source and give it headers since Land Registry don't
src  = etl.pushheader(etl.io.fromcsv(lrURL), ['transaction_id','price_int','completion_dt','pc','property_type_cd','new_build_cd','tenure_cd','paon','saon','street_nm','locality_nm','town_nm','authority_nm','county_nm','status_cd'])
# Tidy up some of the fields so that they're db-friendly
#tidy = convert( convert( convert( convert( src, 'transaction_id','replace','{',''), 'transaction_id','replace','}',''), 'completion_dt','replace',' 00:00'), 'price_int',int)
tidy = src.convert('transaction_id','replace','{','').convert('transaction_id','replace','}','').convert('price_int',int).convert('completion_dt',lambda v: datetime.datetime.strptime(v, "%Y-%m-%d 00:00").date()).sort('completion_dt')

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
    print("OK, will load the latest data.")
else:
    print("OK, stopping.")
    exit()

etl.csv.totsv(tidy.selecteq('status_cd','A'), source=os.path.join(datroot,'-'.join([fn,'Added.csv.gz'])), encoding='UTF-8', write_header=True)
etl.csv.totsv(tidy.selecteq('status_cd','D'), source=os.path.join(datroot,'-'.join([fn,'Deleted.csv.gz'])), encoding='UTF-8', write_header=True)
etl.csv.totsv(tidy.selecteq('status_cd','C'), source=os.path.join(datroot,'-'.join([fn,'Changed.csv.gz'])), encoding='UTF-8', write_header=True)

# Things can get a little slow if we don't tidy up
# the PETL tables when we're done...
del(src)
del(tidy) 

# Remove weird 2-byte chars before loading into PostgreSQL
#os.system(' '.join(["iconv","-f","UTF-8","-c","-t","ascii//TRANSLIT","<",'.'.join([fn,'added','csv']),'|','/usr/bin/sort','-t','"|"','-k 3,3','-o','.'.join([fn,'added','formatted','csv']) ]))

# Update user
print "Having finished pre-processing data. Ready to load into Postgres DB."

############################
############################
# Now we have to run the command to
# bulk-load data into the database.
# We'll take an ETL-ish approach of 
# loading everything into a temporary
# table and then copying it over from
# there.

# It seems that we need to delete records
# first -- Land Registry isn't actually clear
# on whether Changes or Deletes need to run
# first. I assume that Additions come last.
proceed = raw_input('Do you want me to delete records from the price paid fact? [y/n]: ')
if proceed=='y': 
    
    conn = psycopg2.connect(cn)
    cur  = conn.cursor()
    
    update_ppf = utils.get_sql(os.path.join(approot,'Code','SQL','Update','Delete.sql'))
    
    #cur.prepare(update_ppf.replace('{tid}','%s'))
    q = update_ppf.replace("'{tid}'", '%s')
    
    with gzip.open(os.path.join(datroot, '-'.join([fn,'Deleted.csv.gz'])), 'r') as f:
        csv_in = csv.reader(f, delimiter='\t', quotechar='"')
        # Skip header
        next(csv_in)
        # While reading...
        for row in csv_in:
            # Grab tid
            #print update_ppf.replace('{tid}',row[0])
            cur.execute(q,[row[0]])
            print cur.statusmessage
    
    conn.commit() 
    
    conn.close()

print "Deletions complete..."

# Now let's do the changes.
proceed = raw_input('Do you want me to update records in the price paid fact? [y/n]: ')
if proceed=='y': 
    
    conn = psycopg2.connect(cn)
    cur  = conn.cursor()
    
    update_ppf = utils.get_sql(os.path.join(approot,'Code','SQL','Update','Change.sql'))
    
    #cur.prepare(update_ppf.replace('{tid}','%s'))
    q = update_ppf.replace("'{tid}'", '%s')
    
    with gzip.open(os.path.join(datroot, '-'.join([fn,'Changed.csv.gz'])), 'r') as f:
        csv_in = csv.reader(f, delimiter='\t', quotechar='"')
        # Skip header
        next(csv_in)
        # While reading...
        for row in csv_in:
            # Grab tid
            tid = row[0]
            #print update_ppf.replace('{tid}',record[0])
            cur.execute(q,append(row[2:len(row)], [int(row[1]),tid]))
            print tid + ': ' + cur.statusmessage
    
    conn.commit() 
    
    conn.close()

print "Changes complete..."

# And, finally, the bulk load
# of new records...
proceed = raw_input('Do you want me to add new records in the price paid fact? [y/n]: ')
if proceed=='y': 
    
    conn = psycopg2.connect(cn)
    cur  = conn.cursor()
    
    q = utils.get_sql(os.path.join(approot,'Code','SQL','Update','Add.sql'))
    
    with gzip.open(os.path.join(datroot, '-'.join([fn,'Added.csv.gz'])), 'r') as f:
        csv_in = csv.reader(f, delimiter='\t', quotechar='"')
        # Skip header
        next(csv_in)
        # While reading...
        for row in csv_in:
            # Grab tid
            tid = row[0]
            #print update_ppf.replace('{tid}',record[0])
            cur.execute(q,row)
            print tid + ': ' + cur.statusmessage
    
    conn.commit() 
    
    conn.close()

print "Additions complete..."

# Before we're done, we should vaccum
# and analyze (vacuum full rebuilds the 
# table from scratch)
conn = psycopg2.connect(cn)
utils.vacuum(conn, 'landreg.price_paid_fct')
conn.close()

print "Vacuuming complete..."