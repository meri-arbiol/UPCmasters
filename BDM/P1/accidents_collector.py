import pandas as pd

available_years = ['2010', '2011', '2012', '2013', '2014', '2015', '2016', '2017', '2018', '2019', '2020', '2021']
urls_accidents_bcn = []

for year in available_years:
    df = pd.read_csv('https://opendata-ajuntament.barcelona.cat/data/dataset/e769eb9d-d778-4cd7-9e3a-5858bba49b20/resource/bcfc0866-7e2a-4054-9a93-fb3f371fc5eb/download/{}_accidents_gu_bcn.csv'.format(year))
    df.to_csv('landing/opendatabcn-accidents/{}_accidents_bcn.csv'.format(year), encoding='utf-8')