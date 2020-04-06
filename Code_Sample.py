from Scrapper import Scrapper

countries_to_scrape = ['GB', 'US']

sc = Scrapper()
sc.start_clean()
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

        sub_national_data = sc.get_sub_national_data(row['url'])
        file = country + '_sub_national_data.csv'
        sc.store_output(sub_national_data, file)
        sc.log(f'Sub National data saved - {file}')

        all_data = national_data.append(sub_national_data)
        file = country + '_all_data.csv'
        sc.store_output(all_data, file)
    else:
        sc.log(f'skipping country {country}')
