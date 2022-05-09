import pandas as pd
import numpy as np

def income_districtID_lookup(row, df_lookup):
    '''
    '''
    # Get district from opendatabcn-income
    districte = row['Nom_Districte']
    # Get id from income lookup
    try:
        district_id = df_lookup.loc[df_lookup['district'] == districte, 'district_id'].iloc[0]
    except:
        district_id = 'Not Found'
    # Return id
    return district_id
    
def income_neighborhoodID_lookup(row):
    '''
    '''
    # Get neighborhood from opendatabcn-income
    barri = row['Nom_Barri']
    # Get id from income lookup
    try:
        neighborhood_id = df_lookup.loc[df_lookup['neighborhood'] == barri, 'neighborhood_id'].iloc[0]
    except:
        neighborhood_id = 'Not Found'
    # Return id
    return neighborhood_id

def accidents_districtID_lookup(row):
    '''
    '''
    # Get district from opendatabcn-accidents
    districte = row['Nom districte']
    # Get id from income lookup
    try:
        district_id = df_lookup.loc[df_lookup['district'] == districte, 'district_id'].iloc[0]
    except:
        district_id = 'Not Found'
    # Return id
    return district_id
    
def accidents_neighborhoodID_lookup(row):
    '''
    '''
    # Get neighborhood from opendatabcn-accidents
    barri = row['Nom barri']
    # Get id from income lookup
    try:
        neighborhood_id = df_lookup.loc[df_lookup['neighborhood'] == barri, 'neighborhood_id'].iloc[0]
    except:
        neighborhood_id = 'Not Found'
    # Return id
    return neighborhood_id