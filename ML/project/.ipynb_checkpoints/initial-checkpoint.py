import pandas as pd
import numpy as np


# importing datasets
df_hyper = pd.read_csv('landing/temporal/raw/allhyper.data', sep=',', header=None)
df_hyper_test = pd.read_csv('landing/temporal/raw/allhyper.test', sep=',', header=None)

df_hypo = pd.read_csv('landing/temporal/raw/allhypo.data', sep=',', header=None)
df_hypo_test = pd.read_csv('landing/temporal/raw/allhypo.test', sep=',', header=None)

df_bp = pd.read_csv('landing/temporal/raw/allbp.data', sep=',', header=None)
df_bp_test = pd.read_csv('landing/temporal/raw/allbp.test', sep=',', header=None)

df_rep = pd.read_csv('landing/temporal/raw/allrep.data', sep=',', header=None)
df_rep_test = pd.read_csv('landing/temporal/raw/allrep.test', sep=',', header=None)

df_sick = pd.read_csv('landing/temporal/raw/sick.data', sep=',', header=None)
df_sick_test = pd.read_csv('landing/temporal/raw/sick.test', sep=',', header=None)


# defining dataset columns
initial_columns = ['age', 'sex', 'on_thyroxine', 'query_on_thyroxine', 'on_antithyroid_meds', 'sick',
                    'pregnant', 'thyroid_surgery', 'I131_treatment', 'query_hypothyroid', 'query_hyperthyroid', 'lithium',
                    'goitre', 'tumor', 'hypopituitary', 'psych', 'TSH_measured', 'TSH', 
                    'T3_measured', 'T3', 'TT4_measured', 'TT4', 'T4U_measured', 'T4U', 
                    'FTI_measured', 'FTI', 'TBG_measured', 'TBG', 'referral_source', 'target']

df_hyper.columns = initial_columns
df_hyper_test.columns = initial_columns

df_hypo.columns = initial_columns
df_hypo_test.columns = initial_columns

df_bp.columns = initial_columns
df_bp_test.columns = initial_columns

df_rep.columns = initial_columns
df_rep_test.columns = initial_columns

df_sick.columns = initial_columns
df_sick_test.columns = initial_columns


# replacing '?' values with NaN
df_hyper.replace('?', np.nan, inplace=True)
df_hyper_test.replace('?', np.nan, inplace=True)

df_hypo.replace('?', np.nan, inplace=True)
df_hypo_test.replace('?', np.nan, inplace=True)

df_bp.replace('?', np.nan, inplace=True)
df_bp_test.replace('?', np.nan, inplace=True)

df_rep.replace('?', np.nan, inplace=True)
df_rep_test.replace('?', np.nan, inplace=True)

df_sick.replace('?', np.nan, inplace=True)
df_sick_test.replace('?', np.nan, inplace=True)


# split 'target' and extract 'patient_id'
df_hyper[['target', 'patient_id']] = df_hyper['target'].str.split('|', expand=True)
df_hyper_test[['target', 'patient_id']] = df_hyper_test['target'].str.split('|', expand=True)

df_hypo[['target', 'patient_id']] = df_hypo['target'].str.split('|', expand=True)
df_hypo_test[['target', 'patient_id']] = df_hypo_test['target'].str.split('|', expand=True)

df_bp[['target', 'patient_id']] = df_bp['target'].str.split('|', expand=True)
df_bp_test[['target', 'patient_id']] = df_bp_test['target'].str.split('|', expand=True)

df_rep[['target', 'patient_id']] = df_rep['target'].str.split('|', expand=True)
df_rep_test[['target', 'patient_id']] = df_rep_test['target'].str.split('|', expand=True)

df_sick[['target', 'patient_id']] = df_sick['target'].str.split('|', expand=True)
df_sick_test[['target', 'patient_id']] = df_sick_test['target'].str.split('|', expand=True)


# clean-up 'target' **gives weird warning**
df_hyper['target_hyper'] = df_hyper['target'].str.replace('.', '')
df_hyper_test['target_hyper'] = df_hyper_test['target'].str.replace('.', '')
df_hyper.drop('target', axis=1, inplace=True)
df_hyper_test.drop('target', axis=1, inplace=True)

df_hypo['target_hypo'] = df_hypo['target'].str.replace('.', '')
df_hypo_test['target_hypo'] = df_hypo_test['target'].str.replace('.', '')
df_hypo.drop('target', axis=1, inplace=True)
df_hypo_test.drop('target', axis=1, inplace=True)

df_bp['target_bp'] = df_bp['target'].str.replace('.', '')
df_bp_test['target_bp'] = df_bp_test['target'].str.replace('.', '')
df_bp.drop('target', axis=1, inplace=True)
df_bp_test.drop('target', axis=1, inplace=True)

df_rep['target_rep'] = df_rep['target'].str.replace('.', '')
df_rep_test['target_rep'] = df_rep_test['target'].str.replace('.', '')
df_rep.drop('target', axis=1, inplace=True)
df_rep_test.drop('target', axis=1, inplace=True)

df_sick['target_sick'] = df_sick['target'].str.replace('.', '')
df_sick_test['target_sick'] = df_sick_test['target'].str.replace('.', '')
df_sick.drop('target', axis=1, inplace=True)
df_sick_test.drop('target', axis=1, inplace=True)


# back-up clean datasets to .csv separately
df_hyper.to_csv('landing/temporal/csv_checkpoint/all_hyper.csv', index=False)
df_hyper_test.to_csv('landing/temporal/csv_checkpoint/all_hyper_test.csv', index=False)

df_hypo.to_csv('landing/temporal/csv_checkpoint/all_hypo.csv', index=False)
df_hypo_test.to_csv('landing/temporal/csv_checkpoint/all_hypo_test.csv', index=False)

df_bp.to_csv('landing/temporal/csv_checkpoint/all_bp.csv', index=False)
df_bp_test.to_csv('landing/temporal/csv_checkpoint/all_bp_test.csv', index=False)

df_rep.to_csv('landing/temporal/csv_checkpoint/all_rep.csv', index=False)
df_rep_test.to_csv('landing/temporal/csv_checkpoint/all_rep_test.csv', index=False)

df_sick.to_csv('landing/temporal/csv_checkpoint/all_sick.csv', index=False)
df_sick_test.to_csv('landing/temporal/csv_checkpoint/all_sick_test.csv', index=False)


# merge targets of all dataframes on 'patient_id'
df_thyroid = pd.merge(df_hyper, df_hypo[['patient_id','target_hypo']], on='patient_id', how='left')
df_thyroid_test = pd.merge(df_hyper_test, df_hypo_test[['patient_id','target_hypo']], on='patient_id', how='left')

df_bp_rep = pd.merge(df_bp, df_rep[['patient_id','target_rep']], on='patient_id', how='left')
df_bp_rep_test = pd.merge(df_bp_test, df_rep_test[['patient_id','target_rep']], on='patient_id', how='left')

df_bp_rep_sick = pd.merge(df_bp_rep, df_sick[['patient_id','target_sick']], on='patient_id', how='left')
df_bp_rep_sick_test = pd.merge(df_bp_rep_test, df_sick_test[['patient_id','target_sick']], on='patient_id', how='left')

df_all = pd.merge(df_thyroid, df_bp_rep_sick[['patient_id','target_bp','target_rep','target_sick']], on='patient_id', how='left')
df_all_test = pd.merge(df_thyroid_test, df_bp_rep_sick_test[['patient_id','target_bp','target_rep','target_sick']], on='patient_id', how='left')


# export to csv
df_thyroid.to_csv('landing/persistent/all_thyroid.csv', index=False)
df_thyroid_test.to_csv('landing/persistent/all_thyroid_test.csv', index=False)

df_all.to_csv('landing/persistent/all_conditions.csv', index=False)
df_all_test.to_csv('landing/persistent/all_conditions_test.csv', index=False)

