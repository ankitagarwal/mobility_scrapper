from Scrapper import Scrapper

countries_to_scrape = ['GB']

sc = Scrapper()
# sc.start_clean()
countries = sc.get_county_list()
sc.store_output(countries, 'countries-list.csv')
sc.log(f'Output saved - countries-list.csv')
for idx, row in countries.iterrows():
    country = row['country']
    if country in countries_to_scrape:
        sc.log(f'scrapping country {country}')
        national_data = sc.get_national_data(row['url'])
        file = country + '_national_data.csv'
        sc.store_output(national_data, file)
        sc.log(f'National data saved - {file}')
    else:
        sc.log(f'skipping country {country}')
