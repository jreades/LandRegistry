"""
=========================================================
Append price paid data to randomly-generated data points
within the OS Vector Map building outlines
=========================================================
"""
print(__doc__)

import numpy as np
import psycopg2
import pydot
import csv

# The default string to connect to 
# Postgres database
host = 'localhost'
db   = 'LandReg'
user = ''
pwd  = ''
port = ''
cn   = "host='{0}' dbname='{1}' user='{2}' password='{3}' port={4}".format(host, db, user, pwd, port)

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

	cursor.execute("select transaction_id, price_int, ppf.pc as pc from landreg.price_paid_fct as ppf, os.pc_mapping_dim as pmd where extract(year from ppf.completion_dt)={} and ppf.pc=pmd.pc and pmd.region='London'".format(y))

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

with open("pp_transaction_fct.csv", 'w') as fp: 
	c = csv.writer(fp, delimiter=',')
	c.writerows(dataset)
