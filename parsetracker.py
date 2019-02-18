# -*- coding: utf-8 -*-
"""
Created on Tue Jan 22 17:07:02 2019

@author: Tony Foo Hee Haw
"""

import pandas as pd
import argparse
import sys

parser = argparse.ArgumentParser(description='Evaluate tracker and determine the stream and assignments of individual reports')
parser.add_argument('tracker', metavar=r'file', type=str,
                    help='full file path including the file extension of the tracker. Only supports .xlsx files')
args = parser.parse_args()

def main(tracker):
    data_columns=('Result Name','Source','Stream','Assigned to')
    try:
        df = pd.read_excel(tracker, sheet_name='Result Summary', engine = 'xlrd')
        df = df.reindex(columns=list(data_columns))
        df.to_csv('tracker_data.csv','~',index=False)
        print('tracker_data.csv')
    except:
        sys.exit("Unable to parse tracker " + tracker + ", file may be invalid or worksheet missings")
#        exit(1)
        
    

if __name__ == '__main__':
    main(args.tracker)

