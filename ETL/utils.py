# -*- coding: utf-8 -*-
import os.path
import re

def multiple_replace(dict, text): 
    """ Replace in 'text' all occurences of any key in the given
    dictionary by its corresponding value.  Returns the new tring.""" 

    # Create a regular expression  from the dictionary keys
    regex = re.compile("(%s)" % "|".join(map(re.escape, dict.keys())))

    # For each match, look-up corresponding value in dictionary
    return regex.sub(lambda mo: dict[mo.string[mo.start():mo.end()]], text) 

def read_sql(path):
    """Accepts the path to a SQL-compatible source file.
    Not usually called direclty, as get_sql will use this
    internally"""
    
    sql = ''
    f = open(path, 'r')
    for s in f.read().splitlines():
        if not "--" in s and s:
           sql += s + " "
            
    f.close()
    return sql
    

#########################
# Interpolate SQL queries 
# into a single massive query.
def get_sql(path, subs={'foobarbaz':None}, openc='{', closec='}'):
    """Interpolates parameterised SQL using a dictionary expecting to find '{var}' 
    in the SQL query and to have a value for each substitution. Returns one string 
    no matter how many queries contained in the source file."""
    
    replacements = dict()
    
    for k in subs:
        replacements[''.join([openc,k,closec])] = str(subs[k])
    
    sql   = read_sql(path)
    
    query = multiple_replace(replacements, sql).replace("\n"," ").replace("\t"," ")
    return query

#########################
# Interpolate SQL queries 
# into a list of queries.
def get_sql_iterator(path, subs={'foobarbaz':None}, openc='{', closec='}'):
    """Interpolates parameterised SQL using a dictionary expecting to find '{var}' 
    in the SQL query and to have a value for each substitution. Returns a list of 
    SQL queries that can be run iteratively (which may make debugging easier or 
    avoid some issues relating to VACUUMing."""
    
    replacements = dict()
    
    for k in subs:
        replacements[''.join([openc,k,closec])] = str(subs[k])
    
    sql   = read_sql(path)
    query = multiple_replace(replacements, sql).replace("\n"," ").replace("\t"," ").split(';')
    return query

#########################
# Needed to enable Vacuuming 
# within Psycopg2
def vacuum(conn, table='', do='ANALYZE'):
    old_isolation_level = conn.isolation_level
    conn.set_isolation_level(0)
    query = 'VACUUM ' + do
    if table is not '':
        query += ' ' + table
        
    conn.cursor().execute(query)
    msg = conn.notices
    conn.set_isolation_level(old_isolation_level)
    
    return msg
    