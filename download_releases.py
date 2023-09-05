# %%
# #!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
    Purpose
        Read in URLs of organogram data releases and downloads files
    Inputs
        - yaml: 'download_parameters.yaml'
    Outputs
        - CSV: Source/ - various
    Parameters
        None
    Notes
        None
'''

import os

import pandas as pd
import yaml

import download_data as dl
import format_datetime as dt

# %%
# READ IN PARAMETERS FROM YAML FILE
with open('download_parameters.yaml', 'r') as f:
    params = yaml.safe_load(f)

# %%
# READ IN FILE OF LINKS TO DATA
os.chdir(
    'C:/Users/' + os.getlogin() + '/Institute for Government/' +
    params['root_folder_path']
)

df_links = pd.read_excel(
    params['links_file_name'],
    sheet_name=params['links_sheet_name'],
)

# %%
# CHECK DATA
# Check that Coverage is one of 'Senior' and 'Junior'
assert set(df_links['Coverage'].unique()) == {'Senior', 'Junior'}, \
    'Unexpected value in Coverage column'

# %%
# RESTRICT DATASET OF LINKS
# Restrict to data releases from March and September
df_links = df_links.loc[
    (df_links['Month'] == 'March') |
    (df_links['Month'] == 'September')
]

# Restrict to data releases for which we have a URL
df_links = df_links.loc[
    (pd.notnull(df_links['URL'])) &
    (df_links['URL'] != 'x')
]

# %%
# DOWNLOAD DATA
# Check whether folders exist for each department, and create if not
for department in df_links['Department'].unique():
    if not os.path.exists(
        'C:/Users/' + os.getlogin() + '/Institute for Government/' +
        params['root_folder_path'] + '/' + params['source_data_folder_path'] + '/' +
        department
    ):
        os.makedirs(
            'C:/Users/' + os.getlogin() + '/Institute for Government/' +
            params['root_folder_path'] + '/' + params['source_data_folder_path'] + '/' +
            department
        )

# %%
# Download data
df_links.apply(
    lambda row: dl.download_file(
        url=row['URL'],
        data_folder_path=(
            'C:/Users/' + os.getlogin() + '/Institute for Government/' +
            params['root_folder_path'] + '/' + params['source_data_folder_path'] + '/' +
            row['Department']
        ),
        rename_data_file=True,
        new_filename=(
            row['Department'].lower() + '-' +
            str(row['Year']) + '-' +
            str(dt.map_month_to_number(row['Month'], padded=True)) + '-' +
            row['Coverage'].lower() +
            '-organogram.csv'
        ),
        overwrite_existing=False,
        log_details=True,
        logs_folder_path=(
            'C:/Users/' + os.getlogin() + '/Institute for Government/' +
            params['root_folder_path'] + '/' + params['logs_folder_path']
        )
    ),
    axis='columns'
)

# %%
# CHANGE DIRECTORY BACK TO ROOT
# NB: Done, so that reruns don't break if we forget to reset the kernel
os.chdir(
    'C:/Users/' + os.getlogin() + '/Institute for Government/' +
    params['root_folder_path']
)
