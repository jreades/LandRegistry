"""
=========================================================
Append price paid data to randomly-generated data points
within the OS Vector Map building outlines
=========================================================
"""
print(__doc__)

import csv
from os import getcwd
from os import chdir

import psycopg2

#sys.path.append('/Library/PostgreSQL/9.3/bin/')
psql_path = '/Library/PostgreSQL/9.3/bin/'

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

dataset = []

for y in range(1995, 2014): 
	# get a connection, if a connect cannot be made an exception will be raised here
	conn = psycopg2.connect(cn)

	# conn.cursor will return a cursor object, you can use this cursor to perform queries
	cursor = conn.cursor()

	# execute our Query
	cursor.execute("select pc, id from viz.pc_transaction_spa where yr={}".format(y))

	# retrieve the records from the database
	locations = cursor.fetchall()

	#cursor.execute("select transaction_id, price_int, ppf.pc as pc from landreg.price_paid_fct as ppf, os.pc_mapping_dim as pmd where extract(year from ppf.completion_dt)={} and ppf.pc=pmd.pc and pmd.region='London'".format(y))

	transactions = cursor.fetchall()

	lookup  = dict()

	for i in range(0, len(locations)):
		#print locations[i][0]
		pc = locations[i][0]
	
		if pc not in lookup:
			lookup[pc] = list()
		
		lookup[pc].append(locations[i][1])

	errors  = 0

	for i in range(0, len(transactions)): 
	
		pc    = transactions[i][2]
		price = transactions[i][1]
		tid   = transactions[i][0]
		year  = y
	
		try: 
			dataset.append( [tid, year, price, lookup[pc].pop(), pc] )
		except KeyError:
			pass
		except IndexError:
			print 'Unable to find id for TID  {} in postcode {}'.format(tid,pc)
			errors = errors + 1
			pass 

	print 'There were {} errors in year {}'.format(errors, y)

with open(os.path.join(datroot,'.'join(['pp_transaction_fct','csv'])), 'w') as fp: 
	c = csv.writer(fp, delimiter=',')
	c.writerows(dataset)

q = " ".join(["\copy","{area}.pp_transaction_fct","FROM","".join(["'",os.path.join(datroot,'.'.join(['pp_transaction_fct','csv'])),"'"]),"WITH","DELIMITER ','"])
subprocess.call([''.join([psql_path,'psql']),'-h',config['host'],'-d',config['db'],'-U',config['user'],'-w','-p',config['port'],'-c',q])

# And now we can tidy up the last large files
os.remove(os.path.join(datroot, '.'.join(['pp_transaction_fct','csv'])))