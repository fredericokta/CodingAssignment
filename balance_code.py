# Databricks notebook source
'''
Assignment 2 Definition
The task is to calculate the balance of each account number after each transaction using the provided dataset.
The dataset contains the details of all the credits and debits made to accounts daily. 
The objective is to read the given dataset and perform transformation using PySpark code to fulfill this requirement.

For this, I'm using in a databricks workspace databricks a small cluster with this libraries pandas==1.5.2 and pyspark==3.3.1 installed.
'''


# COMMAND ----------

import pyspark.pandas as ps
import pandas as pd

# COMMAND ----------


excel_path = "/mnt/curated/ProblemDefinition.xlsm" #xlsm path
ps.set_option('compute.ops_on_diff_frames', True)


# COMMAND ----------

df = ps.read_excel(excel_path)

df = df.loc[:, ~df.columns.str.contains('^Unnamed')] #remove Unnamed columns

df = df.dropna(subset=['AccountNumber']) #remove lines without AccountNumber
df['AccountNumber'] = df['AccountNumber'].astype(int) #convert AccountNumber to int

df['TransactionType'] = df['TransactionType'].str.strip() #removes any leading or trailing whitespace
df = df.sort_values(by=['TransactionDate', 'AccountNumber'])

def calculate_current_balance(group): #The calculate_current_balance function is applied to each group of transactions for each AccountNumber.
    current_balance = 0
    balances = []
    for row in group.itertuples(index=False): #for every row,  the value of current_balance is stored in a list with index aligned with the original dataframe.
        if row.TransactionType == 'Credit':
            current_balance += row.Amount
        elif row.TransactionType == 'Debit':
            current_balance -= row.Amount
        balances.append(current_balance)
    return pd.Series(balances, index=group.index)


df['CurrentBalance'] = df.groupby('AccountNumber').apply(calculate_current_balance).reset_index(level=0, drop=True) #the list of current_balance is merged with the original dataframe as a new column following the correct index. 

display(df)
