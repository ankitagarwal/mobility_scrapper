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
