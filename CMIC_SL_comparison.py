# -*- coding: utf-8 -*-
"""
Created on Wed Oct  9 19:39:18 2019

@author: aksin
"""

import pandas as pd
import argparse
import glob
# import os

#os.chdir('/Users/aksin/Projects/ShimmickDataVal/data')


def convert_to_str(val):
    return val.astype(int).astype(str)

def combine_cols(df, cols):
    if len(cols) == 1:
        return df[cols[0]]
    else:
        return df[cols[0]] + '_' + combine_cols(df, cols[1:])


###############################################################################
#.........................Clean up CMIC.......................................#
###############################################################################
def cmic_cont_or_co(val):
    if val == 0:
        return 'Contract'
    else:
        return 'CO'

def clean_cmic(df):
    df.rename(columns={'VLS_JOBVEN1_CODE': 'job_no', 'VLS_JOBVEN1_NAME': 
        'job_name', 'VLS_CONT_CODE': 'subcontract_no',
        'VLS_JOBVEN2_CODE': 'vendor_no', 'VLS_JOBVEN2_NAME': 'vendor_name', 
        'VLS_SCH_TASK_CODE': 'item_no', 'VLS_SCH_TASK_NAME': 'item_name',
        'VLS_SCH_CAT_CODE': 'category', 
        'VLS_SCH_PHS_CODE': 'phase_no', 'VLS_SCH_JOB_CODE': 'job_cost_no', 
        'VLS_CHG_CODE': 'co_code', 'VLS_MST_DATE': 'co_date', 
        'VLS_SCH_UNIT': 'qty', 'VLS_SCH_WM_CODE': 'qty_type', 
        'VLS_SCH_AMT': 'dollar_amount',
        'VLS_CONT_AMT': 'cont_total', 'CS_JV2_CONT_AMT': 'vendor_total'
        },
    inplace=True)
    
        
    df = df[df['job_no'].notnull()].copy()
    df['co_code'] = pd.to_numeric(df['co_code'], errors="coerce")
    df['cont_or_co'] = df['co_code'].apply(lambda x: cmic_cont_or_co(x))
    df['category'] = df['category']/100
    df['co_date'] = pd.to_datetime(df['co_date'], infer_datetime_format=True)
    df['item_name'] = df['item_name'].astype(str).apply(lambda x: x[0:30].strip().lower())
    df['vendor_name'] = df['vendor_name'].astype(str).apply(lambda x: x[0:15].strip().lower())
    df['phase_no'] = df['phase_no'].astype(str)
    df['phase_no'] = df['phase_no'].apply(lambda x: x+'0' if len(x)==6 else x)
    
    cols_to_convert = ['job_no', 'vendor_no', 'item_no', 'job_cost_no', 'category']
    
    for col in cols_to_convert:
        df[col] = convert_to_str(df[col])
    
    cols_to_combine = ['job_no', 'subcontract_no', 'item_no', 'phase_no', 
                       'category', 'cont_or_co']
    
    df['ID'] = combine_cols(df, cols_to_combine)
    
    return df
###############################################################################
#.........................Clean up SL.........................................#
###############################################################################
def sl_cont_or_co(val1, val2):
    if val1 == val2:
        return 'Contract'
    else:
        return 'CO'

def gather_co_rel_data(cont_type, cont_val, co_val):
    if cont_type== 'CO':
        return co_val
    else:
        return cont_val

def clean_sl(df):

    #sl = pd.read_csv('sl.csv', header=None)
    #sl.dropna(how='all', axis=1, inplace=True)
    #sl.dropna(how='all', axis=0, inplace=True)
    
    # Get Change Order date
    df['cont_or_co'] = df.apply((lambda x: sl_cont_or_co(x[47], x[48])), axis=1)
    df['co_date'] = df[102]
    df['co_date'] = pd.to_datetime(df['co_date'], infer_datetime_format=True)
    df.drop_duplicates([41, 42, 'cont_or_co', 'co_date'], inplace=True)
    df = df[df['cont_or_co']=='Contract'].append(df[~(df['co_date'].isnull())]).sort_index()
    
    df['job_no'] = df[26].map(lambda x: x.split(' ')[4].split('-')[0].strip())
    df['job_name'] = df[26].map(lambda x: x.split('- ')[1].strip())
    df['subcontract_no'] = df[20].map(lambda x: x.split(' ')[3])
    df['vendor_no'] = df[20].map(lambda x: x.split(': ')[2].split(' ')[2].strip())
    df['vendor_name'] = df[20].map(lambda x: x.split(': ')[2].split('  ')[-1].strip())
    df['vendor_name'] = df['vendor_name'].astype(str).apply(lambda x: x[0:15].strip().lower())
    df['item_no'] = df[41].map(lambda x: x.split(': ')[-1].strip())
    df['item_name'] = df[42].map(lambda x: x.split('  ')[0].strip())
    df['item_name'] = df['item_name'].astype(str).apply(lambda x: x[0:30].strip().lower())
    df['category'] = df[42].map(lambda x: x.split(': ')[-1].strip())
    df['phase_no'] = df[42].astype(str).map(lambda x: x.split('Phase: ')[-1].split('  ')[0].strip().strip('.'))
    df['job_cost_no'] = df[42].map(lambda x: x.split('- ')[0].split(' ')[-1].strip())
    df['co_code'] = None
      
    df['qty'] = df.apply((lambda x: gather_co_rel_data(x['cont_or_co'], x[57], x[106])), axis=1)
    df['qty'] = df['qty'].fillna('0').str.replace(',', '').astype(float)
    
    df['qty_type'] = df.apply((lambda x: gather_co_rel_data(x['cont_or_co'], x[56], x[105])), axis=1)
    df['qty_type'] = df['qty_type'].fillna('LS')
    
    df['dollar_amount'] = df.apply((lambda x: gather_co_rel_data(x['cont_or_co'], x[47], x[108])), axis=1)
    df['dollar_amount'] = df['dollar_amount'].str.replace(',', '').astype(float)
    
    df['cont_total'] = df[128].str.replace(',', '').astype(float)
    df['vendor_total'] = df[129].str.replace(',', '').astype(float)
   
    cols_to_combine = ['job_no', 'subcontract_no', 'item_no', 'phase_no', 
                       'category', 'cont_or_co']
    
    df['ID'] = combine_cols(df, cols_to_combine)
    
    return df
###############################################################################
#.........................Combine and Compare.................................#
###############################################################################

def compare_cols(df, cols):
    for col in cols:
        df[col+'_zcomparison'] = df[col+'_cmic'] == df[col+'_sl']
    
    return df

def compare_dfs(cmic, sl):
    
    # Merge databases with the relevant columns
    cols_to_keep = ['job_no', 'job_name', 'subcontract_no', 'vendor_no', 'vendor_name',
                    'item_no', 'item_name', 'category', 'phase_no', 'job_cost_no', 
                    'cont_or_co', 'co_date', 'qty', 'qty_type', 'dollar_amount', 
                    'cont_total', 'vendor_total', 'ID']
    combined = pd.merge(cmic[cols_to_keep], sl[cols_to_keep], 
                        how='outer', on='ID', suffixes=('_cmic', '_sl'))
    
    # Compare the databases
    cols_to_compare = ['job_no', 'job_name', 'subcontract_no', 'vendor_name',
                    'item_no', 'item_name', 'category', 'phase_no', 'job_cost_no', 
                    'cont_or_co', 'co_date', 'qty', 'qty_type', 'dollar_amount', 
                    'cont_total', 'vendor_total']

    
    combined = compare_cols(combined, cols_to_compare)
    combined.loc[(combined['cont_or_co_sl'] == 'Contract') & (combined['cont_or_co_cmic']  == 'Contract'), 'co_date_zcomparison'] = True
    
    # Sort by column name
    combined = combined.reindex(sorted(combined.columns), axis=1)
    
    return combined


###############################################################################
#.........................Load files..........................................#
###############################################################################

def file_loader(loc):
    cmic_files = glob.glob(loc+'cmic'+'*.*')
    sl_files = glob.glob(loc+'sl'+'*.*')
    
    for cmic_file in cmic_files:
        job_no = cmic_file.split()[-1].split('.')[0]
        
        # Get the relevant SL file
        sl_file = [s for s in sl_files if job_no in s][0]
        
        # Load files
        cmic_df = pd.read_table(cmic_file, encoding="ISO-8859-1")
        sl_df = pd.read_csv(sl_file, header=None)
        
        print("---------------------\n",
             "Comparing files\n'{0}' and\n'{1}'".format(cmic_file, sl_file))
    
        # Clean up dfs for comparison
        cmic_df = clean_cmic(cmic_df)
        sl_df = clean_sl(sl_df)
        
        # Combine and compare
        out_df = compare_dfs(cmic_df, sl_df)
        
        # Save file
        out_df.to_csv(loc+'comparison_'+job_no+'.csv', index=False)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description = 'location of CMIC and SL files')
    parser.add_argument('location', help='enter the location')
    args = parser.parse_args()
    
    loc = args.location
    file_loader(loc)
    print("===================================================\n",
          "All done! Comparison files stored in: '{}'".format(loc))

