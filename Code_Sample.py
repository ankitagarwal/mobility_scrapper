import pandas as pd

from Scrapper import Scrapper
from datetime import date

countries_to_scrape = ['GB', 'US', 'BR', 'MX', 'CO', 'FR', 'BE', 'ZA', 'PE', 'AR', 'CA']

sc = Scrapper()
sc.start_clean()
countries = sc.get_county_list()
sc.store_output(countries, 'countries-list.csv')
sc.log(f'Output saved - countries-list.csv')
all_countries_data = None
today = date.today()

for idx, row in countries.iterrows():
    country = row['country']
    if country in countries_to_scrape:
        sc.log(f'scrapping country {country}')
        national_data = sc.get_national_data(row['url'])

        file = country + f'_national_data_{today}.csv'
        sc.store_output(national_data, file)
        sc.log(f'National data saved - {file}')

        sub_national_data = sc.get_sub_national_data(row['url'])
        file = country + f'_sub_national_data_{today}.csv'
        sc.store_output(sub_national_data, file)
        sc.log(f'Sub National data saved - {file}')

        all_data = national_data.append(sub_national_data)
        file = country + f'_all_data_{today}.csv'
        sc.store_output(all_data, file)
    else:
        sc.log(f'skipping country {country}')
        continue
    if all_countries_data is None:
        all_countries_data = all_data
    else:
        all_countries_data = all_countries_data.append(all_data)

file = f'all_countries_all_data_{today}.csv'
sc.store_output(all_countries_data, file)

# ------------------------Collecting Region Data ------------------------------------
region_list = sc.get_region_list()
sc.store_output(region_list, "region_list.csv")
regional_data = pd.DataFrame()
regional_sub_data = pd.DataFrame()
# region_list={'url':["https://www.gstatic.com/covid19/mobility/2020-03-29_US_Michigan_Mobility_Report_en.pdf"]}
for url in region_list['url']:
    region_data = sc.get_regional_data(url)
    # region_list["overall data"] = region_list.url.apply(sc.get_regional_data)
    regional_data = regional_data.append(region_data)
    sub_region_data = sc.get_sub_regional_data(url)
    # region_list["locality data"] = region_list.url.apply(sc.get_sub_regional_data)
    regional_sub_data = regional_sub_data.append(sub_region_data)
    all_data = region_data.append(sub_region_data)
    sc.store_output(all_data, f"{:qregion_data['region'][0]}_all_region_data_{today}.csv")

# sc.store_output(region_list,"all_data.csv")
sc.store_output(regional_data, f'all_region_all_data_{today}.csv')
sc.store_output(regional_sub_data, f'all_region_all_sub_data_{today}.csv')


