#!/usr/bin/env python

import os,sys

#by charlie_sun@03/26/2014

"""
a script for running cgquery in batch mode
"""

query_dic = {'1':'filename','2':'aliquot_id','3':'legacy_sample_id','4':'sample_id','5':'participant_id'}

if ( len(sys.argv[:]) < 3  or len(sys.argv[:]) > 3 )  and __name__ == '__main__':
     print 'USAGE cgquery.py file_name query_by(int value 1-5) \n'
     print '1 for filename, 2 for aliquot_id, 3 for legacy_sample_id, 4 for sample_id, 5 for participant_id\n'
     print 'None for Everything\n' 
elif  __name__ == '__main__':

    file_name = sys.argv[1]
    query_by = query_dic[sys.argv[2]]
    file_in = open(file_name, 'r')
    all_lines = file_in.readlines()
    file_in.close()

    for line in all_lines:
        my_query = """cgquery '%s=%s' >> %s_out"""%(query_by,line.strip(),file_name)
        process = os.popen(my_query)
        processed = process.read()
        process.close()
