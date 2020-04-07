import os
import pandas as pd
import numpy as np
from pycountry import countries as pyc
import logging
import re
import urllib.request
from bs4 import BeautifulSoup
import shutil

from pdfminer.layout import LAParams
from pdfminer.converter import PDFPageAggregator
from pdfminer.pdfinterp import PDFResourceManager
from pdfminer.pdfinterp import PDFPageInterpreter
from pdfminer.pdfpage import PDFPage
from pdfminer.layout import LTTextBoxHorizontal


class Scrapper:
    DEV_MODE = True
    logger = None
    path = 'dev-data'
    output = 'output'

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        logging.basicConfig()
        self.logger.setLevel(logging.DEBUG)
        self.logger.info("Logger created")

    def store_output(self, df, file):
        file = self.get_local_file(file, self.output)
        df.to_csv(file, index=False)

    def start_clean(self):
        try:
            if os.path.exists(self.path):
                shutil.rmtree(self.path)
                self.log(f'Removed folder {self.path}')
            os.mkdir(self.path)
            self.log(f'Created folder {self.path}')

            if os.path.exists(self.output):
                shutil.rmtree(self.output)
                self.log(f'Removed folder {self.output}')
            os.mkdir(self.output)
            self.log(f'Created folder {self.output}')
        except OSError as e:
            self.logger.exception("Error: %s : %s" % (self.path, e.strerror))

    def get_local_file(self, file, path=None):
        if path is None:
            path = self.path
        return os.path.join(path, file)

    def write_file(self, file, content):
        my_file = open(self.get_local_file(file), "wb")  # open file in write mode
        my_file.write(content)  # write to file
        my_file.close()  # close file

    def read_file(self, file):
        my_file = open(self.get_local_file(file), "r")  # open file in write mode
        content = my_file.read()  # read from a file
        my_file.close()  # close file
        return content

    def open_file(self, file, mode='rb'):
        my_file = open(self.get_local_file(file), mode)
        return my_file

    def log(self, msg):
        self.logger.info(msg)

    def file_exists(self, file):
        file = self.get_local_file(file)
        if os.path.exists(file) and os.path.isfile(file):
            self.logger.info(f'File exists {file}')
            return True
        else:
            self.logger.info(f'File doesn\'t exist {file}')
            return False

    def url_to_file(self, url):
        url_to_file = {
            'https://www.google.com/covid19/mobility/': 'mobility.html'
        }
        if url in url_to_file.keys():
            return url_to_file[url]
        else:
            return os.path.basename(url)

    def get_content(self, url):
        content = self.scrape_content(url)
        if content is None:
            return self.read_file(self.url_to_file(url))
        else:
            return content

    def scrape_content(self, url):
        file = self.url_to_file(url)
        if self.DEV_MODE and self.file_exists(file):
            self.logger.info(f'File present in cache {file}')
            return None
        else:
            self.logger.info(f'Scrapping url {url}')
            with urllib.request.urlopen(url) as response:
                html = response.read()
                self.write_file(file, html)
                return html

    def get_country_code_from_url(self, url):
        return re.split(". |_", os.path.basename(url))[1]

    def get_region_code_from_url(self, url):
        return re.split(". |_", os.path.basename(url))[2]

    def get_date_code_from_url(self, url):
        return re.split(". |_", os.path.basename(url))[0]

    def get_country_name_from_code(self, code):
        country = pyc.lookup(code)
        return country.name

    def get_date_country_cname(self, url):
        country = self.get_country_code_from_url(url)
        date = self.get_date_code_from_url(url)
        country_name = self.get_country_name_from_code(country)
        return [country, date, country_name]

    def get_date_region_cname(self, url):
        region = self.get_region_code_from_url(url)
        date = self.get_date_code_from_url(url)
        country_code = self.get_country_code_from_url(url)
        return [region, date, country_code]

    def get_county_list(self, url: str = 'https://www.google.com/covid19/mobility/'):
        html = self.get_content(url)
        page = BeautifulSoup(html, 'lxml')
        links = [tag['href'] for tag in page.select("div.country-data > a.download-link")]
        df = pd.DataFrame(data=links, columns=['url'])
        # No idea why this double casting is required.
        df[['country', 'date', 'country_name']] = pd.DataFrame(df.url.apply(self.get_date_country_cname).tolist())
        self.logger.info('Finished getting country list. Fount {} countries.'.format(len(df)))
        return df

    def parsedocument(self, document, start_page=0, end_page=0, return_page=False):
        # convert all horizontal text into a lines list (one entry per line)
        # document is a file stream
        lines = []
        dict = {}
        p = 0

        rsrcmgr = PDFResourceManager()
        laparams = LAParams()
        device = PDFPageAggregator(rsrcmgr, laparams=laparams)
        interpreter = PDFPageInterpreter(rsrcmgr, device)

        for page in PDFPage.get_pages(document):
            if p < start_page:
                self.logger.info(f'Skipping page number {p}')
                p += 1
                continue

            self.logger.info(f'Parsing page number {p}')
            interpreter.process_page(page)
            layout = device.get_result()
            l = []
            for element in layout:
                if isinstance(element, LTTextBoxHorizontal):
                    l.extend(element.get_text().splitlines())
            lines.extend(l)
            dict[p] = l

            if (end_page != 0) and (p == end_page):
                self.logger.info(f'Reached page number {p}, stopping')
                break
            p += 1

        if return_page:
            return dict
        else:
            return lines

    def get_clean_number(self, text: str):
        if str(text).startswith('Not enough data for this date'):
            return np.nan
        text = str(text).replace("compared to baseline", "")
        return float(text.replace('%', '').strip()) / 100

    def get_region_list(self, url="https://www.google.com/covid19/mobility/"):
        html = self.get_content(url)
        page = BeautifulSoup(html, 'lxml')
        links = [tag['href'] for tag in page.select("div.region-data > a.download-link")]
        df = pd.DataFrame(data=links, columns=['url'])
        df['country'] = df.url.apply(self.get_country_code_from_url)
        df['region'] = df.url.apply(self.get_region_code_from_url)
        df['date'] = df.url.apply(self.get_date_code_from_url)
        self.logger.info('Finished getting regions list. Found {} regions.'.format(len(df)))
        return df

    def get_national_data(self, url):
        self.logger.info(f'Getting national data for {url}')
        self.scrape_content(url)
        lines = self.parsedocument(self.open_file(self.url_to_file(url)), 0, 1)
        data = [
            ['retail_recr', self.get_clean_number(lines[13])],
            ['grocery_pharm', self.get_clean_number(lines[16])],
            ['parks', self.get_clean_number(lines[19])],
            ['transit', self.get_clean_number(lines[56])],
            ['workplace', self.get_clean_number(lines[59])],
            ['residential', self.get_clean_number(lines[62])]
        ]
        df = pd.DataFrame(data=data, columns=['entity', 'value'])
        df['country'], df['date'], df['country_name'] = self.get_date_country_cname(url)
        df['location'] = "COUNTRY OVERALL"
        self.logger.info(f'National data found of size - {len(df)}')
        return df

    def get_clean_location_name(self, location: str):
        location = location.replace("^c\\(\"", "")
        location = location.replace("\", \"", " ")
        location = location.replace("\"\\)", " ")
        location = location.replace("And", "and")
        return location

    def get_city_index(self, lines: [str]):
        idx = []
        three_ent_1 = []
        three_ent_2 = []
        for i in range(len(lines)):
            if lines[i].startswith('Retail'):
                idx.append(i - 1)
            if lines[i].startswith('Parks'):
                if lines[i + 1].startswith('*'):
                    three_ent_1.append([i + 2, i + 3, i + 4])
                else:
                    three_ent_1.append([i + 1, i + 2, i + 3])
            if lines[i].startswith('Residential'):
                if lines[i + 1].startswith('*'):
                    three_ent_2.append([i + 2, i + 3, i + 4])
                else:
                    three_ent_2.append([i + 1, i + 2, i + 3])
        return [idx, three_ent_1, three_ent_2]

    def get_sub_national_data(self, url):
        self.logger.info(f'Getting sub-natinal data for {url}')
        self.scrape_content(url)
        pages = self.parsedocument(self.open_file(self.url_to_file(url)), 2, 0, True)
        pages.popitem()  # Delete last page.
        nodes = []
        for n, lines in pages.items():
            [cities, three_ent_1, three_ent_2] = self.get_city_index(lines)
            if ((len(cities) != len(three_ent_1)) or
                    (len(three_ent_1) != len(three_ent_2)) or
                    (len(three_ent_2) > 2) or
                    (len(three_ent_2) < 1)):
                self.logger.warning(f'Page number {n} is corrupt, skipping..')
                continue
            try:
                node = []
                for i in range(len(cities)):
                    cur_node = [
                        ['retail_recr', self.get_clean_number(lines[three_ent_1[i][0]])],
                        ['grocery_pharm', self.get_clean_number(lines[three_ent_1[i][1]])],
                        ['parks', self.get_clean_number(lines[three_ent_1[i][2]])],
                        ['transit', self.get_clean_number(lines[three_ent_2[i][0]])],
                        ['workplace', self.get_clean_number(lines[three_ent_2[i][1]])],
                        ['residential', self.get_clean_number(lines[three_ent_2[i][2]])],
                    ]
                    for l in cur_node:
                        # TODO - find a better way to do this.
                        l.extend([self.get_clean_location_name(lines[cities[i]])])
                        node.extend([l])
                nodes.extend(node)
            except Exception as e:
                self.logger.warning(f'Page number {n} is corrupt, skipping..')
                self.logger.warning(e)
                print(len(lines), cities, three_ent_1, three_ent_2)
                print(lines)
                print(node)
            self.logger.info(f'Collected data from Page number {n}')
        df = pd.DataFrame(data=nodes, columns=['entity', 'value', 'location'])
        df['country'], df['date'], df['country_name'] = self.get_date_country_cname(url)
        self.logger.info(f'Sub National data found of size - {len(df)}')
        return df

    def get_regional_data(self, url):
        self.scrape_content(url)
        lines = self.parsedocument(self.open_file(self.url_to_file(url)), 0, 1)
        data = [
            ['retail_recr', self.get_clean_number(lines[13])],
            ['grocery_pharm', self.get_clean_number(lines[16])],
            ['parks', self.get_clean_number(lines[19])],
            ['transit', self.get_clean_number(lines[56])],
            ['workplace', self.get_clean_number(lines[59])],
            ['residential', self.get_clean_number(lines[62])]
        ]
        df = pd.DataFrame(data=data, columns=['entity', 'value'])
        df['region'], df['date'], df['country'] = self.get_date_region_cname(url)
        df['location'] = "REGION OVERALL"
        return df

    def remove_astric(self, data_set):
        result = []
        for data in data_set:
            if data.strip() != "*":
                result.append(data)
        return result

    def get_enity(self, entity):
        entity_dict = {"Retail & recreation": "retail_recr",
                       "Grocery & pharmacy": "grocery_pharm",
                       "Parks": "parks",
                       "Transit stations": "transit",
                       "Workplace": "workplace",
                       "Residential": "residential"}
        if entity.strip(" ") in entity_dict.keys():
            return entity_dict[entity.strip(" ")]

    def clean_data_list(self, data_list):
        result_list = []
        try:
            for data in data_list:
                data[1] = self.get_enity(data[1])
                data[2] = self.get_clean_number(data[2])
                result_list.append(data)
        except:
            self.logger.warning(f"Skipping location {data[0]} as the data is corrupt")
        return result_list

    def get_sub_regional_data(self, url):
        self.logger.info("getting sub_region;")
        self.scrape_content(url)
        lines = self.parsedocument(self.open_file(self.url_to_file(url)), 2, -1, True)
        main_df = pd.DataFrame()
        for line in lines.keys():
            try:
                data_list = []
                first_index = [index for index, value in enumerate(lines[line]) if value.strip() == "Retail & recreation"]
                second_index = [index for index, value in enumerate(lines[line]) if value.strip() == "Transit stations"]

                for i in range(0, len(first_index)):
                    location = lines[line][first_index[i] - 1]
                    first_set = lines[line][first_index[i]:first_index[i] + 9]
                    first_set = self.remove_astric(first_set)
                    for j in range(0, 3):
                        data_list.append([location, first_set[j], first_set[j + 3]])

                    second_set = lines[line][second_index[i]:second_index[i] + 9]
                    second_set = self.remove_astric(second_set)
                    for k in range(0, 3):
                        data_list.append([location, second_set[k], second_set[k + 3]])
                data_list = self.clean_data_list(data_list)
                df = pd.DataFrame(data=data_list, columns=['location', 'entity', 'value'])
                df['date'] = self.get_date_code_from_url(url)
                df['country'] = self.get_country_code_from_url(url)
                df['region'] = self.get_region_code_from_url(url)
                main_df = main_df.append(df, ignore_index=True)
            except Exception as e:
                self.logger.error(f"Skiping region { url } as the data is currupt")
        return main_df


#
# print(Scrapper().get_sub_regional_data("https://www.gstatic.com/covid19/mobility/2020-03-29_US_District_of_Columbia_Mobility_Report_en.pdf"))
# Scrapper().get_county_list()
# Scrapper().get_national_data('https://www.gstatic.com/covid19/mobility/2020-03-29_AF_Mobility_Report_en.pdf')
# Scrapper().get_sub_national_data('https://www.gstatic.com/covid19/mobility/2020-03-29_GB_Mobility_Report_en.pdf')
