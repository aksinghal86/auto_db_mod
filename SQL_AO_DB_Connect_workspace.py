"""
# coding: utf-8

# # Initial code for using Python to interact with the PGE_SIP database
#
# Written in Python 3 by Exponent for various PGE projects
#
# ###### Revision History
#
#    | Action             | Date         | Programmers                |
#    |--------------------|--------------|----------------------------|
#    | Creation           | July 2018    | L. Drew Hill, Ankur Singhal|
#    | Handoff to Ankur   | Aug 1, 2018  | L. Drew Hill, Ankur Singhal|
#
#
# This document provides a walkthrough of the code required to connect via
# Python to the PGE_SIP database.
#
# Notes:
#     - PGE_SIP must be established as an ODBC Data Source (DSN) within
#       Windows' ODBC Data Source Administrator.
#     - Tdot vs.order number is important to keep
#     - drop P dots
#     - refereshed not just monthly, but weekly and possibly even daily
#
# Other things to look out for:
#     - Check that all desired rows and columns are properly transferred from
#       source file -> SQL database by manual (or automated) observation.
#       Nothing missing, and no weird conversions (e.g. 0's to NULLS, etc.)
#     - Confirm column types are appropriately preserved from
#       Excel -> python -> SQL database
#     - Update "master" / "combined" files to be smart enough to know if column
#       placements change (so columns in the month-specific file are properly
#       appended to columns in the master (or combined) table
"""

###############################################################################
# Load libraries and modules #
###############################################################################
import datetime
import os
import re
import random
import tkinter
import tkinter.filedialog
import sqlalchemy
import pandas as pd
import numpy as np
import pyodbc


###############################################################################
# Functions #
###############################################################################
def connect_sql():
    """
    SQL server details. Returns connection to the server

    Because we have defined our server as DSN, we use "trusted source"
    credentialing, but there are other ways to do this, if necessary.
    """

    # Define SQL server details
    SERVER = 'SFSVMSQL3'
    DATABASE = 'PGE_SIP'
    DSN = 'PGE_SIP'

    # Connect to servers
    return pyodbc.connect('DSN='+DSN+';SERVER='+SERVER+';DATABASE='+DATABASE+';')


def get_AO_file():
    """
    Opens up a dialog box to ask the user to select the appropriate AO file.
    All sheets are imported from the AO source file.

    Note: User input is required
    """
    root = tkinter.Tk()

    # Ask the user to select the appropriate AO file
    print("Please choose the AO source file that you'd like to import")
    srcfile = tkinter.filedialog.askopenfile(parent=root, mode='rb', title="")
    root.destroy()

    print("\nRegistering AO source file. May take a few minutes...\n")

    # Convert to a panda dataframe
    XL_AO = pd.ExcelFile(srcfile)
    sheetnames = list(XL_AO.sheet_names)
    print("Available sheets {}".format(sheetnames))

    # Remove hidden sheets
    for name in sheetnames:
        if 'hidden' in name:
            sheetnames.remove(name)

    # Unlike D1000 file, need all sheets here, except for the hidden sheet,
    # so all sheets will be imported
    # User is not provided with an option to limit introducing any errors
    print("\nImporting the following sheets {}".format(sheetnames))

    # Load file into a pandas frame for all relevant AO sheets
    # df = pd.read_excel(io=AO_sourcefile, sheet_name=AO_sheetnames)

    # Get date of the data file assuming that date is always in MMDDYYYY
    # format at the end of the file name
    dbdate_s = re.sub('.xlsx'r'\'>|.xls'r'\'>', '', str(srcfile))
    dbdate_s = dbdate_s[len(dbdate_s)-8: len(dbdate_s)]

    return(srcfile, sheetnames, dbdate_s)


def del_blank_cols(df):
    """
    Drop any blank columns defined as a column with nunique() parameter == 0
    """

    blank_cols = []
    for i in range(0, len(df.columns)):
        if df.iloc[:, i].nunique() == 0:
            blank_cols.append(i)

    # Drop all the blank cols
    return df.drop(df.columns[blank_cols], axis=1)


def get_headers(df):
    """
    """
    # Get the row num of very first non-null item (where the headers start)
    check_col = 0
    hrow = 0
    for item in df[df.columns[check_col]]:
        hrow += 1
        if not pd.isnull(item) and item != (check_col+1):
            # Some sheets have a row of column indices. Avoid falsely
            # identifying this as the first data row by excluding any values
            # that are == column number
            break

    # ASSUMPTION: the header of the first column demonstrates
    # where the 'primary' header is (just a fake moniker I'm giving it).
    prim_head = []
    tot_head = []
    for name in df.iloc[hrow-1]:
        if pd.isnull(name):
            # Programmatically determine leading rows that are blank
            prim_head.append('noname')
            tot_head.append('noname')
        elif '$' not in str(name) and '%' not in str(name):
            # ASSUMPTION: the only unit rows all include either "$" or "%"
            prim_head.append(name)
            tot_head.append(name)
        else:
            # Secondary header often contains units like "$" or "%"
            tot_head.append('unit')

    return(hrow, prim_head, tot_head)


def rename_cols(df_THISFILE):
    """
    Renames the columns after addressing the following issues:
        --> Drop blank columns
        --> Several blank cols and rows in the sheets
        --> Column names are in multiple rows
        --> Some columns are unnnamed
        --> Make SQL friendly

    Returns (hopefully) a dataframe that has "clean" column names
    """

    # Drop blank columns
    df_THISFILE = del_blank_cols(df_THISFILE)

    # ###################### Get column headers, etc. #########################
    # Get the: first named row of the sheet (header_row);
    # list of 'primary' column names (primaryheader_list);
    # list of 'secondary' column names (totalheader_list)
    header_row, primaryheader_list, totalheader_list = get_headers(df_THISFILE)

    # ########################### Issue 1 #####################################
    # ##### Unnamed col in some sheets that describes the Cost Element col ####
    # Find it and name this unnamed col
    # ASSUMPTION: the second cost eLement column is never the last column.
    # isolate index of the second cost element column, if it exists
    second_cost_element_col = -1
    for value in df_THISFILE.iloc[header_row]:
        second_cost_element_col += 1
        if 'Order' in str(value) and 'Cost' in str(value):
            break

    if second_cost_element_col == (len(df_THISFILE.iloc[header_row])-1):
        second_cost_element_col = np.nan

    # ONLY IF the second_cost_Element column does exist, name it
    if not pd.isnull(second_cost_element_col):
        totalheader_list[second_cost_element_col] = "Cost_Element_2"

    # ############################ Issue 2 ####################################
    # ############ Unnamed col that describes the Project #####################
    # Find it and name this unnamed col
    # Done by counting characters b/c this should be a long description
    # containing >12 chars. Other cols shouldn't fit this criterion
    # Produce random list of rows to check (does not sow primary header row)
    row_check_list = random.sample(range(header_row, df_THISFILE.shape[0]), 5)

    # The following criteria need to be met in order to be a Proj Desc col:
    #   --> >12 alphabet characters,
    #   --> Not yet named in primary header list
    #   --> Not a Cost Order col
    proj_desc_alphabet_criteria_n = 12
    crit_test_list = []
    for i in range(0, len(df_THISFILE.columns)):
        for j in row_check_list:
            if df_THISFILE.iloc[header_row-1, i] not in primaryheader_list:
                # print(j, i)
                cell_value = df_THISFILE.iloc[j, i]
                cv_alphabeta = re.sub('[0-9]', '', str(cell_value))

                if (len(cv_alphabeta) > proj_desc_alphabet_criteria_n) and (i != second_cost_element_col):
                    # print(cv_alphabeta)
                    if i not in crit_test_list:
                        crit_test_list.append(i)

    # Now check all those that passed... hopefully only a single i
    if len(set(crit_test_list)) == 1:
        proj_desc_col_element = int(crit_test_list[0])
        totalheader_list[proj_desc_col_element] = 'project_description'
    elif len(set(crit_test_list)) > 1:
        print('Unable to distinguish Project Description column.')
    elif len(set(crit_test_list)) < 1:
        print('Note: This sheet does not have a Project Description column '
              '(as far as this program can tell)')

    # #################### Final clean up #####################################
    # Remove non-SQL-friendly characters
    new_headers = []
    for name in totalheader_list:
        new_headers.append(re.sub(' ', '_', name))

    # Drop 'unit' col
    new_headers = list(filter(('unit').__ne__, new_headers))
    new_headers = list(filter(('noname').__ne__, new_headers))

    # Now work on the non-'primary' headers
    # header names for primary rows begin one row above "header_row"
    supraheader_list = []
    for name in df_THISFILE.iloc[header_row-2]:
        if not pd.isnull(name):
            name = re.sub('\n| - |\(|\)|\\$|\\.', '', name)
            name = re.sub('\\%', 'pcnt', name)
            name = re.sub(' ', '_', name)
            name = re.sub('\+', 'plus', name)
            name = re.sub("/", "_divby_", name)
            name = re.sub("^_", "", name)
            name = re.sub("_$", "", name)
            supraheader_list.append(name)

    # ############ Put it all together ########################################
    # Create final header list by merging these lists
    header_THISDF = new_headers + supraheader_list

    # Reassign column names
    df_THISFILE.columns = header_THISDF

    # Drop extraneous rows (defined in very beginning)
    df_THISFILE.drop(df_THISFILE.index[0:header_row], inplace=True)

    return df_THISFILE


def save_db(dict_of_AO_db, sheetnames, date_of_db):
    """
    Saves output file(s) on local drive to QA/QC, verify code works and if
    the SQL db already contains the db. In this case, instead of appending
    a duplicate copy, copy is stored on local drive.
    """

    root = tkinter.Tk()

    print("\n\n********************************************************\n"
          "Please choose the folder to store the output file in\n")
    os.chdir(tkinter.filedialog.askdirectory(parent=root))
    root.destroy()

    print("Okay! Output AO files will be stored in {}\n".format(os.getcwd()))

    for sheet in sheetnames:
        print(sheet)
        dfname = 'df_' + sheet

        # create dataframe wherein column types are classified automatically
        df = dict_of_AO_db[dfname].infer_objects()
        name_of_db = re.sub(" ", "", dfname + '_' + str(date_of_db) + '.csv')

        print("Output file {} being created...\n".format(name_of_db))
        df.to_csv(name_of_db, index=False)


def upload_ao_sheets(dict_of_AO_db, sheetnames, date_of_db):
    """
    Upload cleaned AO financial sheets to SQL database

    Note: pyodbc does not interface directly with Microsoft SQL for uploads, so
    "sqlalchemy" is used here to bridge the gap.

    The upload currently takes 200 - 400 seconds, but can be made much faster
    with some tweaking or use of "turbodc" package
    """

    # Create MSSQL engine using Windows authentication and DSN as defined above
    # This will serve as our connection for "df.to_sql"

    try:
        DSN = 'PGE_SIP'
        mssql_engine = sqlalchemy.create_engine('mssql+pyodbc://'+DSN,
                                                encoding="latin1", echo=False)

        for sheet in sheetnames:
            # send it
            print(sheet)
            dfname = 'df_' + sheet
            # create dataframe wherein column types are classified automatically
            df = dict_sheetdfs[dfname].infer_objects()
            name_of_db = re.sub(" ", "", dfname + '_' + str(date_of_db) + '_DEVEXAMPLE')

            print("Uploading {} to SQL server. Please wait...".format(name_of_db))
            df.to_sql(name=name_of_db, con=mssql_engine, if_exists='replace')

    except:
        print("\n\n*************************************\n"
              "Could not connect/write to SQL server. \n"
              "Saving to local machine for now..."
              "\n\n*************************************\n")
        save_db(dict_of_AO_db, sheetnames, date_of_db)

    """
    THIS WHOLE THING NEEDS TO BE FIXED 


    # Append this month's data to master database
    name_of_masterdb_in_SQL = dfname + '_MASTER_DELETEME'

    # Note: Can probably be made faster with some tweaking or turbodbc" package
    df.to_sql(name=name_of_db_in_SQL, con=mssql_engine,
              if_exists='replace', chunksize=10**3)

    # print out how long the process took
    end = datetime.datetime.now()
    print(end - start)

    # Add current month to master list, while being wary of duplication

    # CHECK into column order during master append.
    # Does this month's column order match or allign with the master?
    # If a new column needs to be added, will it do so automatically?

    # reconnect to server
    conn = connect_sql()

    # read the full set of sourcefile dates that have already been entered
    ao_master_sourcefiledate_excerpt = pd.read_sql('Select sourcefile_date from dbo.df_DB1000_MASTER_DELETEME', conn)
    conn.close()

    # ### Appending data to master datatable
    # only if this months sourcefile date does not already exist
    # ASSUMPTION: only one sourcefile per sourcefile date
    if dbdate not in list(set(ao_master_sourcefiledate_excerpt.iloc[:, 0])):
        # Time this process -- takes ~ 200 - 400 seconds
        start = datetime.datetime.now()

        # ## Append this month's data to master database
        name_of_masterdb_in_SQL = 'df_DB1000_MASTER_DELETEME'
        # ## This can be made much faster with some tweaking
        df.to_sql(name=name_of_masterdb_in_SQL,
                  con=mssql_engine,
                  # change to 'replace' if starting from scratch
                  if_exists='append')
        end = datetime.datetime.now()
        print(end - start)
    else:
        print("NOTE: It looks like " + name_of_db_in_SQL + " has already been "
              "appended to its master.\n"
              "A copy of the file will be stored locally for your review.")
        save_db(df, dbdate)
    """

###############################################################################
# Main #
###############################################################################

# Connect to servers
# connection = connect_sql()
# connection.crsr.fast_executemany = True

# Produce a data table with information regarding all tables in the databases
# Send pandas command to collect SQL data
# dt_tables = pd.read_sql('SELECT * from Information_schema.tables', connection)
# connection.close()

# Examine tables -- excluding views (i.e. only BASE TABLEs)
# dt_tables[dt_tables['TABLE_TYPE'] == 'BASE TABLE']

# Isolate all 'master' tables
# PGE_SIP_master_tables = dt_tables['TABLE_NAME'][dt_tables['TABLE_NAME'].str.contains('master|Master')]
# PGE_SIP_master_tables

# Example of what we are trying to achieve
# example()

# Produce AO sourcefile and AO sheet names
AO_sourcefile, AO_sheets, dbdate = get_AO_file()

# note, this takes ~6-7 minutes to run
start = datetime.datetime.now()

# Loop through all sheets, create a well-formatted dataframe, then append
# to a dictionary with an appropriate name
dict_sheetdfs = {}
sheetnumba = 0
for sht in AO_sheets:
    print('\nWorking on ' + sht + '...')
    sheetnumba += 1

    df_AO = pd.read_excel(io=AO_sourcefile, sheet_name=sht, header=None)
    df_AO = rename_cols(df_AO)

    # add to dictionary
    dict_sheetdfs['df_' + sht] = df_AO.copy()
    print(sht + ' completed!')

# temporary: ultimately move it to upload SQL function as an error exception
# Save as csv to reduce future time in dev work
save_db(dict_sheetdfs, AO_sheets, dbdate)

# print out how long the process took
end = datetime.datetime.now()
print('Process length: ' + str(end-start) + '\n' +
      str(sheetnumba) + ' sheets completed.')

# Use date in the source file name as a suffix.
# Can be tailored to fit based on user input, today's date, etc. 


"""                
###############################################################################
###############################################################################
# Upload cleaned AO sheets to SQL database
DSN = 'PGE_SIP'
mssql_engine = sqlalchemy.create_engine('mssql+pyodbc://'+DSN,
                                        encoding="latin1", echo=False)

# Time this process -- takes ~4 mins
start = datetime.datetime.now()

for sht in AO_sheets:
    # send it
    print(sht)
    dfname = 'df_' + sht
    # create dataframe wherein column types are classified automatically
    dfff = dict_sheetdfs[dfname].infer_objects()
    name_of_db_in_SQL = re.sub(" ","",dfname + '_' + str(dbdate) +'_DEVEXAMPLE')
    dfff.to_sql(name = name_of_db_in_SQL, con = mssql_engine, if_exists = 'replace')
    print(dfname)

end = datetime.datetime.now()
print(end - start)

#upload_ao_sheets(df_AO, AO_sheets, dbdate)

"""