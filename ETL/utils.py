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
#
# Would be good if you had the option to return a list 
# of SQL queries so that you could execute each
# in turn. That would make debugging easier.
def get_sql(path, subs={'foobarbaz':None}, openc='{', closec='}'):
    """Interpolates parameterised SQL using a dictionary expecting to find '{{var}}' 
    in the SQL query and to have a value for each substitution."""
    
    for k in subs:
        subs[''.join([openc,k,closec])] = str(subs[k])
        del subs[k]
    
    sql   = read_sql(path)
    query = multiple_replace(subs, sql).replace("\n"," ").replace("\t"," ")
    return query

