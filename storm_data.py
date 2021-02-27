import requests
import pandas as pd
import json
import os

# Prep the responses
base_url = 'http://api.eia.gov/series/?series_id='

# Dict holds desired col names and series id for response calls
series_dict = {
    0: ['nat_gas', 'EBA.TEX-ALL.NG.NG.HL'],
    1: ['wind', 'EBA.TEX-ALL.NG.WND.HL'],
    2: ['coal', 'EBA.TEX-ALL.NG.COL.HL'],
    3: ['solar', 'EBA.TEX-ALL.NG.SUN.HL'],
    4: ['hydro', 'EBA.TEX-ALL.NG.WAT.HL'],
    5: ['nuclear', 'EBA.TEX-ALL.NG.NUC.HL'],
}

# Personal key can be registerd at: https://www.eia.gov/opendata/register.php
api_key = os.getenv('EIA_API_KEY')

def produce_dataset(series_dict):
    """Preps energy source datasets then joins them all in one."""
    
    # Empty list for dataframe concatenation
    dfs = []
    
    # Loop through the responses and secure data for each energy source
    for i in range(len(series_dict)):
        response = requests.get('{}{}&api_key={}'.format(base_url, series_dict[i][1], api_key))
        data = response.json()
        data = data['series'][0]['data']
        dfs.append(pd.DataFrame(data, columns=['timestamp', series_dict[i][0]]).set_index('timestamp'))
    
    # Combine all the energy source datasets into one
    final_df = pd.concat(dfs, axis=1)
    final_df.reset_index(inplace=True)
    final_df.rename(columns={'index': 'timestamp'}, inplace=True)
    
    return final_df
    
data = produce_dataset(series_dict)

# Create separate date and time columns and drop timestamp
data['date'] = data.timestamp.apply(lambda x: f'{x[4:6]}-{x[6:8]}-{x[0:4]}')
data['time'] = data.timestamp.apply(lambda x: f'{x[9:11]}:{x[12:]}')
data['datetime'] = data.date + ' ' + data.time
data.drop('timestamp', axis=1, inplace=True)
data['date'] = pd.to_datetime(data.date)

# Filter data and export to csv file
tx_storm_data = data[(data.date >= '2021-01-01') & (data.date <= '2021-02-26')]
tx_storm_data.reset_index(drop=True, inplace=True)
tx_storm_data.to_csv('tx_storm_data.csv', index_label='index')

# Get some parallel weather data
weather_daily_url = 'https://www.ncei.noaa.gov/pub/data/uscrn/products/daily01/2021/CRND0103-2021-TX_Austin_33_NW.txt'
weather_daily_headers = 'https://www.ncei.noaa.gov/pub/data/uscrn/products/daily01/HEADERS.txt'

# Prep headers
headers = pd.read_table(weather_daily_headers, sep=' ')
headers = headers.loc[0, :].values.tolist()

# Parse and clean Austin temp data
austin_weather = pd.read_fwf(weather_daily_url, col_spec='infer', names=headers)
data = austin_weather.loc[:, ['LST_DATE', 'T_DAILY_AVG']]
data.columns = ['date', 'temp_daily_avg']
data['date'] = data.date.apply(lambda x: pd.to_datetime(str(x), format='%Y%m%d'))

# Convert from Celsius to Fahrenheit
data['temp_daily_avg'] = data.temp_daily_avg.apply(lambda x: x*9/5 + 32)

# Export to csv
data.to_csv('austin_daily_temp.csv', index=False)
