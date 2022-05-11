import pandas as pd
from datetime import date
import os


def get_filepaths(directory):
    '''
    This function will generate the file names in a directory 
    tree by walking the tree either top-down or bottom-up. For each 
    directory in the tree rooted at directory top (including top itself), 
    it yields a 3-tuple (dirpath, dirnames, filenames).
    '''
    file_names = []  # List which will store all of the full filepaths.

    # Walk the tree.
    for root, directories, files in os.walk(directory):
        for filename in files:
            file_names.append(filename)  # Add it to the list.

    return file_names  # Output


def check_update_years(files):
    '''
    
    '''
    current_year = date.today().year # get current year
    update_years = [str(year) for year in range(2010, current_year)] # opendatabcn-accidents available years
    
    # Check years present in topic's current landing zone
    present_years = [element[:4] for element in files] # returns a list of present years
    
    #
    for element in present_years:
        if element in update_years:
            update_years.remove(element)
    
    return update_years


def collect_data(update_years):
    '''
    
    '''
    for year in update_years:
        try:
            df = pd.read_csv('https://opendata-ajuntament.barcelona.cat/data/dataset/e769eb9d-d778-4cd7-9e3a-5858bba49b20/resource/bcfc0866-7e2a-4054-9a93-fb3f371fc5eb/download/{}_accidents_gu_bcn.csv'.format(year))
            df.to_csv('landing/temporal/opendatabcn-accidents/{}_accidents_bcn.csv'.format(year), encoding='utf-8')
            print('accidents data for {} extracted'.format(year))
        except:
            print('unable to extract accidents data for {}'.format(year))

            
if __name__ == '__main__':
    #
    opendatabcn_filenames = get_filepaths("landing/temporal/opendatabcn-accidents")
    update_years = check_update_years(opendatabcn_filenames)
    
    # opendatabcn data collection
    if update_years:
        collect_data(update_years)
        print('update complete!')
    else:
        print('accidents data is already up to date!')