import pandas as pd
import censusdata
import re
import sys
import ohio.ext.pandas
from sqlalchemy import create_engine

# New Jersey, can be anything
state = '34'
variables = ['B01001_001E', 'B01002_001E', 'B19013_001E']
variable_names = ['pop', 'med_age', 'med_income']

# Pulls a geo object containing all counties for the 'state' variable
counties_geo = censusdata.geographies(censusdata.censusgeo([('state', state), ('county', '*')]), 'acs5', 2018, key = '3d7e71138248eccfa1f372adc09ca6240ab16dd1')

# Loops through the geo object and appends the actual county # to a list
countyList = list()

for county in counties_geo:
    geo_tuple = counties_geo[county].params()
    geo_tuple = geo_tuple[1][1]
    countyList.append(geo_tuple)

print(countyList)


# Working code to get info on each of those tables for all counties & Block Groups

block_groups = pd.DataFrame()

'''
for county in countyList:
    test = censusdata.download('acs5', 2018,
                               censusdata.censusgeo([('state', '34'), ('county', '{county}'.format(county = county)), ('block group', '*')]), variables,
                               key = '3d7e71138248eccfa1f372adc09ca6240ab16dd1')
    block_groups = block_groups.append(test)
    #print(block_groups)
'''

block_groups = censusdata.download('acs5', 2018,
                               censusdata.censusgeo([('state', '34'), ('county', '001'.format(county = county)), ('block group', '*')]), variables,
                               key = '3d7e71138248eccfa1f372adc09ca6240ab16dd1')

# Set the column names
block_groups.columns = variable_names

idx_pos = 0
# Make a new column out of our index
name_column = block_groups.index
# Add the new column
block_groups.insert(loc=idx_pos, column= 'name', value = name_column)
# Change it from a geo object to a str for splitting
block_groups['name'] = block_groups['name'].apply(str)
# Split it on ,s
block_groups = block_groups.join(block_groups.pop('name').str.split(",", expand=True))
# Give them better names
block_groups.rename(inplace= True, columns={0: "block_group", 1: "census_tract", 2: "county", 3: "state"})
# Remove the gross column
block_groups.drop(columns=[4], inplace=True)

# Reset and drop index column for easier viewing
block_groups.reset_index(inplace=True)
block_groups.drop(columns='index', inplace=True)

# Regex our columns into what we want

block_groups['block_group'] = block_groups['block_group'].str.extract('(\d+)', expand = False)
block_groups['census_tract'] = block_groups['census_tract'].str.extract('(\d+)', expand = False)
block_groups['state'] = block_groups['state'].str.extract('(^[^:]+\s*)', expand = False)

print(block_groups)

block_groups.to_csv('block_groups.csv', index=False)

db_string = sys.argv[1]

table_name = 'drobusto_etl'

engine = create_engine(db_string)
block_groups.pg_copy_to(table_name, engine, if_exists = 'replace', index = False, schema = 'acs')


# We want to split off the index into a column, split that column on ,s
# Make four columns "block group" "census tract" "county" "state"
# For the last two, just grabbing the value is fine
# For the first two, we just want the value to be the last character

# We have a nasty geo object but we can probably loop through, convert it to a string, then
# just cut off after from the : to the end

###########
# Need to get credentials from pgpass file to get into the database
# Either have the script read directly from that file
# OR make another credentials file that's formatted better

# psychopg2 allows you to run sql that pulls data into the database
# copy csv to postgress with psychopg2

# Commit file to github then pull it into the server
# or scp which is more lightweight

# Ohio pandas extension




