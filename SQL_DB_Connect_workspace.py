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
import pyodbc
import pandas as pd


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


def example():
    """
    Download examples of current "Schedule" summary tables from PGE_SIP db.
    These are just examples to demonstrate the end goal of this script

    Note: Under the former PGE_SIP SQL scripts, the primary deliverable files
    were 'VW_Sch_T001' where milestone is not NULL and ordered by
    Program, Order_number, and Project_name and 'TBL_Status_eval'
    """

    conn = connect_sql()

    # Read in the final Schedule deliverables example
    VW_Sch_T001 = pd.read_sql('Select * from VW_Sch_T001', conn)
    TBL_Status_Eval = pd.read_sql('Select * from TBL_Status_eval', conn)

    print(VW_Sch_T001, TBL_Status_Eval)

    conn.close()


def get_D1000_file():
    """
    Opens up a dialog box to ask the user to select the appropriate D1000 file
    Afterwards, function prompts the user to select the relevant sheet,
    e.g. Milestone, which is likely to be the default sheet to be imported.

    Note: User input is required
    """
    root = tkinter.Tk()

    # Ask the user to select the appropriate D1000 file
    print("Please choose the D1000 source file that you'd like to import")
    DB1000_sourcefile = tkinter.filedialog.askopenfile(parent=root, mode='rb',
                                                       title="")
    root.destroy()

    print("\nRegistering D1000 file. May take a few minutes...\n")

    # Convert to a panda dataframe
    XL_DB1000 = pd.ExcelFile(DB1000_sourcefile)
    DB1000_sheetnames = list(XL_DB1000.sheet_names)
    print("Available sheets {}".format(DB1000_sheetnames))

    # 'Milestones' is used as default, unless user specifies otherwise
    response = input("Would you like to import data from 'Milestones'?\n"
                     "(1 = confirm, 0 = reject) ")

    DB1000_sheet = ''
    if response == '1':
        DB1000_sheet = "Milestones"
        print("\nGreat! Importing data from 'Milestones'...\n")
    else:
        while DB1000_sheet not in DB1000_sheetnames:
            DB1000_sheet = input("Please enter the desired sheet name exactly "
                                 "as it appears above, minus the quotes: ")
            if DB1000_sheet in DB1000_sheetnames:
                print("\nNote that this program is currently configured to "
                      "only modify the 'Milestones' worksheet.  It may or "
                      "may not work with other sheets, depending on the "
                      "similarity of structure with the 'Milestones' sheet. "
                      "Please check the output to file ensure it meets your "
                      "needs or let the developer now!"
                      "\n\nFor now Importing from {}...".format(DB1000_sheet))
            else:
                print("\nError! Sheet does not exist. Double check and "
                      "enter the correct sheet name again.")

    # Load file into a pandas frame for the DB1000 sheet provided by user
    df = pd.read_excel(io=DB1000_sourcefile, sheet_name=DB1000_sheet)

    # Get date of the data file assuming that date is always in MMDDYYYY
    # format at the end of the file name
    dbdate_s = re.sub('.xlsx'r'\'>|.xls'r'\'>', '', str(DB1000_sourcefile))
    dbdate_s = dbdate_s[len(dbdate_s)-8: len(dbdate_s)]

    # Close the source file
    DB1000_sourcefile.close()

    return(df, dbdate_s)


def rename_cols(col_list, var):
    """
    Autoname repeating group columns to resolve the "Forecast" vs. "Actual"
    issue between spreadsheets downloaded from PGE at different time points

    Programmatically renames repeating columns, i.e.,
    Forecast, Act, or Baseline
    """

    # Find var ("forecast" or "act." with proper milestone name in some months
    # (e.g. "PKICK"), then check for milestone name by removing var and "."
    # and look for any other characters.

    # Criteria for determining if a var (str) includes a milestone name ==
    # each variable string, in addition to the letters in "Forecast" or
    # "Act", has 4 or more alphabet characters in it
    milestone_names = []
    milestone_list = []
    has_fewer_than_4_alphachar = []
    for i in list(range(0, len(col_list))):
        if var in str.lower(col_list[i]):
            var_name = str.lower(col_list[i])
            possible_milestone = re.sub(" "+var, '', var_name)
            if len(re.sub('[0-9]| ', '', possible_milestone)) >= 4:
                milestone_list.append(possible_milestone)
                milestone_names.append(i)
            else:
                has_fewer_than_4_alphachar.append(int(i))

    return(milestone_names, milestone_list, has_fewer_than_4_alphachar)


def autoname_check(milestone_list):
    """
    Confirm with user that the column names make sense.

    Essentially, a failsafe to have the user confirm the result from a random
    set of  potentially identified milestone names!
    """
    # create a list of random milestone names from the milestone_list
    rand = random.sample(range(len(milestone_list)), 5)
    randlist = [milestone_list[rand[0]], milestone_list[rand[1]],
                milestone_list[rand[2]], milestone_list[rand[3]]]
    user_input = int(input("Do these look like actual milestones "
                           "(yes = 1, no = 0)?\n %s" % randlist))

    return user_input


def apply_milestone_names(var, milestone_names, milestone_list, col_list):
    """
    Loop through every column name and apply Milestone names where appropriate
    """
    for i in list(range(len(milestone_names))):
        column_position = milestone_names[i]
        name = milestone_list[i]
        next_col_name = col_list[column_position+1]
        next_next_col_name = col_list[column_position+1+1]

        new = name + "_" + var

        # programmatically ensure you're catching actual and baseline,
        # rather than blindly assigning them/assuming
        if "Act." in next_col_name:
            new_next = name + '_Actual'
        elif "Base" in next_col_name:
            new_next = name + '_Baseline'

        if "Act." in next_next_col_name:
            new_next_next = name + '_Actual'
        elif "Base" in next_next_col_name:
            new_next_next = name + '_Baseline'

        # assign all relevant columns new SQL-appropriate names
        col_list[column_position] = new
        col_list[column_position+1] = new_next
        col_list[column_position+1+1] = new_next_next

    # re-assign actual column names in the schedule database
    return col_list


def del_blank_cols(df):
    """
    Drop any blank columns defined as:
    a column with nunique() parameter == 0 and will be unnamed
    """
    blank_cols = []

    for i in range(0, len(df.columns)):
        if df.iloc[:, i].nunique() == 0 and 'Unnamed' in df.columns[i]:
            blank_cols.append(i)

    return df.drop(df.columns[blank_cols], axis=1)


def save_db(df, dbdate):
    """
    Saves output file(s) on local drive to QA/QC, verify code works and if
    the SQL db already contains the db. In this case, instead of appending
    a duplicate copy, copy is stored on local drive.
    """
    name_of_db = 'df_DB1000_' + dbdate + '.csv'
    root = tkinter.Tk()

    print("Please choose the folder to store the output file in\n")
    os.chdir(tkinter.filedialog.askdirectory(parent=root))
    root.destroy()

    print("Output file {} being stored in {}".format(name_of_db, os.getcwd()))
    df.to_csv(name_of_db, index=False)


def upload_sched(df, dbdate):
    """
    Upload cleaned "Schedule" source file (from pandas dataframe) to database

    Note: pyodbc does not interface directly with Microsoft SQL for uploads, so
    "sqlalchemy" is used here to bridge the gap.

    The code first uploads the database as an stand-alone data table to SQL;
    and then appends it to the master table.

    The upload currently takes upwords of 4 minutes, but can be made faster
    with some tweaking or use of "turbodc" package
    """

    # Time this process -- takes ~ 200 - 400 seconds
    start = datetime.datetime.now()

    # Create MSSQL engine using Windows authentication and DSN as defined above
    # This will serve as our connection for "df.to_sql"
    try:
        print("\n\nUploading to SQL server. Please wait...\n")

        DSN = 'PGE_SIP'
        mssql_engine = sqlalchemy.create_engine('mssql+pyodbc://'+DSN)

        # desired database parameters
        name_of_db_in_SQL = 'df_DB1000_' + dbdate + '_DELETEME'

        # Upload dataframe as stand-alone table to the SQL db
        # Note: Can probably be made faster with turbodbc" package
        df.to_sql(name=name_of_db_in_SQL, con=mssql_engine,
                  if_exists='replace', chunksize=10**3)

    except:
        print("\n\n*************************************\n"
              "Could not connect/write to SQL server. \n"
              "Do you have read/write access to the SQL server?\n"
              "Saving to local machine for now..."
              "\n\n*************************************\n")
        save_db(df, dbdate)

    # print out how long the process took
    end = datetime.datetime.now()
    print("Process length: ", str(end - start))

    """
    THE WHOLE THING NEEDS TO BE FIXED!!!!!!!!!!!!
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

# Produce column name list from the D1000 sheet imported above
df_DB1000, db_date = get_D1000_file()
col_list_DB1000 = list(df_DB1000.columns)

# Auto rename columns
forecast_milestone_names, milestone_list_f, has_fewer_than_4_alphachar_f = rename_cols(col_list_DB1000, "forecast")
act_milestone_names, milestone_list_a, has_fewer_than_4_alphachar_a = rename_cols(col_list_DB1000, "act.")

# Confirm which variable has column names
milestone_name_bucket = '**error, fix this block of code**'
fyes = 0
ayes = 0

if (len(forecast_milestone_names) > 0) and (has_fewer_than_4_alphachar_f == []):
    fyes = 1
    milestone_name_bucket = 'Forecast'
    print("This month's naming column is Forecast")
if (len(act_milestone_names) > 0) and (has_fewer_than_4_alphachar_a == []):
    ayes = 1
    milestone_name_bucket = "Act."
    print("This month's naming column is Actual")
if fyes == 0 and ayes == 0:
    print("Error. Algorithm could not determine which column includes "
          "milestone name")
if fyes == 1 and ayes == 1:
    print("Something is amiss in the data. Both Forecast and Actual met the "
          "milestone name inclusion test criteria.")

# Confirm with user that the column names make sense
if fyes:
    user_milestone_input = autoname_check(milestone_list_f)
if ayes:
    user_milestone_input = autoname_check(milestone_list_a)

if user_milestone_input:
    # the winner is!
    print("\nThe %s columns hold the milestone name." % milestone_name_bucket)
else:
    milestone_name_bucket = 'error'
    print("\n\n********************************************************\n"
          "Uh oh. The Python code for naming milestone columns may need "
          "some work!\nContact your programmer support folks."
          "\n\n********************************************************\n")

# Create a set of all unique milestones for posterity
if fyes:
    unique_milestones = set(milestone_list_f)
if ayes:
    unique_milestones = set(milestone_list_a)


# Loop through every column name and apply Milestone names where appropriate

# If Forecast is named column
if fyes:
    df_DB1000.columns = apply_milestone_names("Forecast",
                                              forecast_milestone_names,
                                              milestone_list_f,
                                              col_list_DB1000)

# If actual is named column
if ayes:
    df_DB1000.columns = apply_milestone_names("Actual",
                                              act_milestone_names,
                                              milestone_list_a,
                                              col_list_DB1000)

# Add Date column
# ASSUMPTION: date AlWAYS included in MMDDYYYY format at end of file name
df_DB1000['sourcefile_date'] = db_date

# Delete blank columns
df_DB1000 = del_blank_cols(df_DB1000)

# Upload cleaned "Schedule" source file to SQL database
upload_sched(df_DB1000, db_date)

# ########################################################################### #
