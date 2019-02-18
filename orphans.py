# -*- coding: utf-8 -*-
"""
Created on Fri Nov  2 10:02:47 2018

@author: (Tony) Foo Hee Haw

Written using 3.6.5 Anaconda python distribution
Dependencies: pandas, xlrd, numpy

Orphans.py
Tool to facilitate missing data (Single File)
    - Can introduce file loop using a parent ps1 script

There are 2 formats where this is ran:
    1. With Lookup
    2. Analysing based on Unique Key Array (First Key considered higher importance match)

Algorithm summary:    
Consolidate SRC and TGT orphans into separate pd Dataframes

Src Key Lookup:
    Remove Lookup col from ukey array
    Read lookup file into array or hash variable, match to corresponding lookup if available
    
    if available, perform ukey array matching

Unique Key Array:
For each record in SRC, attempt to match records in TGT that have the same
    first unique key filtered into a small temp dataframe.
    
- From the smaller dataframe, trim the matching data down as it iterates through each matching
  unique key column.
- Trim down each record to closest match (1 or 2 mismatches between unique keys) [Tentative]
- Display a max of 5 records [Tentative]
- Include serial no, but not considered in match

Print output into a seperate excel file.
    - How to handle large amounts of orphans?

"""


import pandas as pd
#import numpy as np
import xlrd
import csv

import logging
import sys
import argparse
import os

# ------------- Script Preparations ---------
# Parse arguments
parser = argparse.ArgumentParser(description='Check close matching orphaned records in SRC/TGT and generate an output Excel file detailing findings')
parser.add_argument('file', metavar=r'file', type=str,
                    help='full file path including the file extension of the file which orphans should be analysed. Only supports .xlsx files')
parser.add_argument('-l', '-lookup', '--lookup', metavar='file', type=str, default=None,
                    help='specify full file path of the lookup file, lookup file should be a "~" dsv or csv file (default: None)')

args = parser.parse_args()

# Generate the output file name from file argument
basename, ext = os.path.splitext(args.file)
result_name = basename + "_orphans" + ext

# ------------ End of Preparations -----------
    
def main(infile, outfile, lookupfile=None):

    logger = setup_custom_logger('orphans.py') #setup logger object
    
    #load file
    logger.info('Reading Excel file ' + infile + ' into memory')
    try: #Put all these stuff in try, to account for file opening, missing worksheets etc errors
        book = xlrd.open_workbook(infile)
        
        logger.info('Reading and Configuring parameters')
        
        # Extract Src Key Lookup and UKey array from result file first
        sheet = book.sheet_by_name('Header Information')
        lookup_col = sheet.cell_value(1, 1)
        ukey_arr = list(filter(None,[sheet.cell_value(r, 0) for r in range(4,sheet.nrows)])) #Extract ukey values and remove '' elements
        # if lookup_col is there, shift it to the front of the list
        if ( lookup_col != None and lookup_col in ukey_arr):
            ukey_arr.insert(0, ukey_arr.pop(ukey_arr.index(lookup_col)))
        print_arr = ['Approx Match','Indicator','Serial No']
        logger.info('Finished configuring parameters')
        
        # Read Orphan Records into dataframes
        logger.info('Retrieving SRC Data')
        df_src = get_data(book, 'SRC', print_arr, ukey_arr, logger)
        logger.info('SRC Data retrieved, retrieving TGT Data')
        df_tgt = get_data(book, 'TGT', print_arr, ukey_arr, logger)
        logger.info('TGT Data retrieved')
    except:
        logger.error("Unable to parse file " + infile + ", invalid excel file or some orphans data may be missing.")
        exit(2)
    
    if df_src.empty or df_tgt.empty:
        logger.info('No mutual Orphan Records found, hence nothing to compare')
        exit(1)
    
    main_write_df = pd.DataFrame()
    lookup_dict = None
    
    if ( lookup_col != 'None' ):
        
        if ( lookupfile == None ):
            logger.error("Result " + infile + " uses lookup but lookup file was not provided")
            exit(3)
        
        ukey_arr.remove(lookup_col)
    
        #Read lookup file into dictionary variable
        lookup_dict={}
        
        try:
            with open(lookupfile, 'r') as f:
                reader = csv.reader(f, delimiter='~')
                next(reader, None) #Skip the 'src~tgt' header
                for key, value in reader:
                    lookup_dict[key] = value
        except:
            logger.error("Unable to parse lookup file " + str(lookupfile))
            exit(3)
            
#        lookup_dict = dict((int(k),int(v)) for k,v in lookup_dict.items())
#       print (lookup_dict)
        lookup_dict_inv = {v: k for k, v in lookup_dict.items()} #inverted dictionary for substitution
        
        df_tgt[lookup_col].replace(lookup_dict_inv, inplace=True) #awk substitue to src lookup
        
        # Main loop to build main styling DF
        for entry_index, entry in df_src.iterrows():
            df_search = search_tgt_orphans(entry, ukey_arr, df_tgt, lookup_dict = lookup_dict, lookup_key = lookup_col)
            main_write_df = pd.concat([main_write_df, df_search]).reset_index(drop=True) #Consolidate the data from most recent iteration onto main writing DF

    else:
        if ( lookupfile != None):
            logger.info("As File does not use any lookup column, lookupfile is not required")
        
        for entry_index, entry in df_src.iterrows():
            df_search = search_tgt_orphans(entry, ukey_arr, df_tgt)
            main_write_df = pd.concat([main_write_df, df_search]).reset_index(drop=True) #Consolidate the data from most recent iteration onto main writing DF

    
    logger.info('Checking orphans complete')
    
    logger.info("Printing results to output Excel File")
    #Generate output file
    writer = pd.ExcelWriter(outfile, engine='xlsxwriter')
    workbook = writer.book
    
    # Write the summary part
    worksheet = workbook.add_worksheet('Summary')
    sum_format = workbook.add_format({'font_name': 'Arial', 'bold': True, 'font_size': 10})
    sum_format2 = workbook.add_format({'font_name': 'Arial', 'bold': True, 'font_size': 10, 'font_color': 'red'})
    worksheet.set_column(5,5,45)
    worksheet.set_column(6,6,25)
    worksheet.set_column(7,7,20)
    
    a_match_count = len(main_write_df.loc[ ( main_write_df['Indicator'] == 'SRC' ) & ( main_write_df['Approx Match'] == 'FOUND' ) ])
    true_orph_count = len(main_write_df.loc[ main_write_df['Indicator'].isnull() ])
    
    worksheet.write(0,6,'Analysis Summary',sum_format)
    worksheet.write(1,7,'Percentage',sum_format)
    worksheet.write(2,5,'Total Orphan Records in Source SRC', sum_format)
    worksheet.write(2,6,len(df_src), sum_format)
    worksheet.write(3,5,'Total Orphan Records in Target TGT', sum_format)
    worksheet.write(3,6,len(df_tgt), sum_format)
    worksheet.write(4,5,'No of Orphans with Approx Matches', sum_format)
    worksheet.write(4,6, a_match_count, sum_format)
    worksheet.write(4,7, a_match_count/len(df_src)*100, sum_format)
    worksheet.write(5,5,'No of True Orphans', sum_format)
    worksheet.write(5,6, true_orph_count, sum_format2)
    worksheet.write(5,7, true_orph_count/len(df_src)*100, sum_format2)

    #Write the Dataframe style object to output Excel file (another sheet)
    output_styling(main_write_df,ukey_arr).to_excel(writer, sheet_name='Orphan Analysis', index = False)
    
    # Rewrite df headers as I like (also to follow the standard format for eCompare result)
    worksheet = writer.sheets['Orphan Analysis']
    n_format = workbook.add_format({'bold': True, 'font_size': 10})
    ukey_format = workbook.add_format({'bold': True, 'font_size': 10, 'font_color': 'blue'})
    lkey_format = workbook.add_format({'bold': True, 'font_size': 10, 'font_color': 'purple'})
    
    result_col = 0 #iterator column variable
    for column in main_write_df:
        if column in ukey_arr:
            worksheet.write(0, result_col, column, ukey_format)
        elif ( column == lookup_col ):
            worksheet.write(0, result_col, column, lkey_format)
        else:
            worksheet.write(0, result_col, column, n_format)
#        worksheet.set_column(result_col,result_col,len(column))
        result_col += 1
        
    worksheet.freeze_panes(1, 0)
    
#    worksheet_sum = workbook.add_worksheet('Summary')

    
    try:
        writer.save()
    except:
        logger.error("Result file " + outfile + " not saved as it is in use by another process")
        exit(1)    

    # End of main()
    
# ----------- Supplementary functions to main() --------------
        
def get_data(excel, report_indicator, info_arr, ukey_list, logger):
    df = pd.read_excel(excel, sheet_name=report_indicator, engine = 'xlrd')
    #Select only data from df containing orphans
    df = df.loc[df['Is Orphan Record'].astype(str) == 'Yes' ] # (I define astype str because null by default is 'float' and cannot be compared to str)
    df = df.reindex(columns=list(info_arr + ukey_list)).fillna('') #Filter only columns in print_arr and ukey_arr, adding columns as needed
    df['Indicator'] = report_indicator
    return df
 
# Primary function for searching related orphans       
def search_tgt_orphans(row, ukey_list, df_tgt, lookup_dict = None, lookup_key = None):
    
    # Give priority to lookup search
    # Second priority goes individual ukeys
    
    # ------ Start Support Functions ----------
    
    def search_column (row, column, df_search):
        df_search = df_search.copy()
#        print(",column is "+ column)
        return df_search.loc[(df_search[column] == row[column] )]
#        df_found = df_search.loc[(df_search[column] == row[column] )]
#        return df_found
    
    # Loop through all the unique keys and search entries that match the column
    def loop_through_ukeys (row, ukey_list, df_search, lookup = False):
        # Consider 3 situations:
        # 1. New found df <= 4, New found df = 0 (use previous), Reached the end and still >= 4
        # Zero_checking is a boolean used for non-lookup formats, where returning 0 results would trigger a reverse ukey switch
        
        df_found = df_search # Reference for original DF in case first iteration returns empty DF
        for column in ukey_list:
            df_found_sub = search_column(row, column, df_search) #Returns a DF of matched columns
#            print("sub length is " + str(len(df_found_sub)))
            if ( len(df_found_sub) == 0 ):
                if (lookup):
                    return pd.concat([df_found.head(4),pd.DataFrame(['... Only first 4 matches shown'],columns=['Serial No'])],sort=False) #Print first 4 and notification
                else:
                    #we return original array here cause any similar match of >5 entries are likely to not very specific we may as well consider not matched
                    df_found = df_search
                    return df_found #if false, return original array
            elif ( len(df_found_sub) <= 4 ):
                df_found = df_found_sub
                return df_found
            df_search=df_found_sub #Reduce the search criteria based on found from previous iteration
        # executes when loop reaches the end with > 4 rows
        df_found = df_found_sub
        return df_found
    
    # ---------- End Support Functions -----------
    
    #Designates row as a copy of the entry Series passed in, clearing the SettingWithCopy Warnings
    row = row.copy()
    
    if ( lookup_key != None ):
        replace_bool = True #boolean to trigger replacing tgt lookup values back to original values
        df_found = search_column(row, lookup_key, df_tgt)
        if ( len(df_found) == 0 ):
            # Generate empty DF that clearly states the not found lookup key field
            replace_bool = False
            if ( row[lookup_key] not in lookup_dict):
                # Add a "notice" dataframe to denote failure to find matching orphans
                df_found = pd.DataFrame(data=['Did not find any TGT orphans matching src unique key ' + lookup_key + ': ' + str(row[lookup_key])],columns=['Serial No'])
            else:    
                df_found = pd.DataFrame(data=['Did not find any TGT orphans matching src unique key ' + lookup_key + ': ' + str(lookup_dict[row[lookup_key]])],columns=['Serial No'])
        elif ( len(df_found) <= 4 ):
           row['Approx Match'] = 'FOUND' #Indicate presence of close matching TGT orphans
           df_found['Approx Match'] = 'FOUND'
        else:
            row['Approx Match'] = 'FOUND'
            df_found['Approx Match'] = 'FOUND'
            df_found = loop_through_ukeys(row, ukey_list, df_found, lookup = True)
        
        if (row[lookup_key] in lookup_dict and replace_bool):
            df_found[lookup_key].replace(lookup_dict, inplace=True) #Restore lookup field values back to original values
        
    #End of if lookup column is specified
    else: #Else when lookup column not specified
#        print("search ukey")
        df_found = loop_through_ukeys(row, ukey_list, df_tgt)
#        print("len of df_found " + str(len(df_found)) + ", len of df_tgt " + str(len(df_tgt)))
        
        if ( df_found.equals(df_tgt) ): #if both dataframes are the same
            #Try reversing the ukey_list
#            print("search ukey_inverse")
            df_found = loop_through_ukeys(row, list(reversed(ukey_list)), df_tgt)
            # If STILL can't find any approx matches
            if ( df_found.equals(df_tgt) ):
#                print("can't find at all")
                df_found = pd.DataFrame(data=['Did not find any close matching TGT orphans for src unique keys ' + ukey_list[0] + ' or ' + ukey_list[-1]],columns=['Serial No'])
            else: #Else found using reverse ukey_list
                row['Approx Match'] = 'FOUND'
                df_found['Approx Match'] = 'FOUND'
        else: #Else found using original order ukey_list
            row['Approx Match'] = 'FOUND'
            df_found['Approx Match'] = 'FOUND'
    
    df_search=pd.concat([row.to_frame().T,df_found], sort=False)
    return df_search

# Used for styling, colour all SRC indicator rows cyan
def output_styling(out_df,ukey_arr):
    def src_bg(x,column):
        is_max = pd.Series(data=False, index=x.index)
        is_max[column] = x.loc[column] == 'SRC'
        return ['background-color: #ccffff' if is_max.any() else '' for v in is_max]
    
    # Used for styling, compare each tgt column with that of src, marking red if !=
    def check_value(x):
        # Any way to implement on specific source - tgt range?
        return ['color: red' if v != x.iloc[0] else 'color: black' for v in x]
    
    def font_size(v):
        return 'font-size: 10pt'
    
    mwd_style = out_df.style.apply(src_bg, column=['Indicator'], axis = 1) #apply style for src columns (cyan bg)
    mwd_style.applymap(font_size) #Set font size to 10 instead of 11, consistent with result

    mwd_sidx = out_df[out_df['Indicator'].isin({'SRC'})].index.tolist() #Get Array of all the rows containing src
    mwd_sidx.append(out_df.tail(1).index.item() + 1 )
#    print("mwd_sidx is ",mwd_sidx, "; len mwd_sidxc is ",len(mwd_sidx))
    
    # Tablute all the range of src-tgt entries then apply mismatch check styles across them in a loop
    for i in range(1, len(mwd_sidx)):
        if ( mwd_sidx[i] - mwd_sidx[i-1] <= 1 ):
#            print ("execute if less than 1")
            next #Skip if no corresponding tgt entries are found
        else:
            #Otherwise, get the sub interval of one src record to its approx match orphans then perform comparison on each column based on src values
            mwd_style.apply(check_value, subset=pd.IndexSlice[mwd_sidx[i-1]:mwd_sidx[i]-1, list(ukey_arr) ] )
#            print ("execute check value ", mwd_sidx[i-1], " and ", mwd_sidx[i]-1)

    return mwd_style

# Setup logger object
def setup_custom_logger(name):
    formatter = logging.Formatter(fmt='%(name)s %(asctime)s %(levelname)-8s %(message)s',
                                  datefmt='%d-%m-%Y %H:%M:%S')
#    handler = logging.FileHandler('D:\log.txt', mode='w')
#    handler.setFormatter(formatter)
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    
    stdout_handler = logging.StreamHandler(stream=sys.stdout) #Setup stdout stream
    stdout_handler.setFormatter(formatter)
    stdout_handler.setLevel(logging.INFO)
    stderr_handler = logging.StreamHandler(stream=sys.stderr) # and stderr stream
    stderr_handler.setFormatter(formatter)
    stderr_handler.setLevel(logging.ERROR) #I think logging.ERROR is still also going to stdout but dunno how to fix

    if logger.handlers: #Clear existing handlers otherwise will duplicate
        logger.handlers = []
#    logger.addHandler(handler)
    logger.addHandler(stdout_handler)
    logger.addHandler(stderr_handler)
    
    return logger
    
if __name__ == '__main__':
    if (args.lookup is not None): 
        #if lookup is passed from Powershell, pass it into main too
        main(args.file, result_name, lookupfile=args.lookup)
    else:
        main(args.file, result_name)