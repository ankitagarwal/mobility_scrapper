import os
import pandas as pd
from PyPDF2 import PdfFileReader as PdfR
from pycountry import countries as pyc
import logging
import re
import urllib.request
from bs4 import BeautifulSoup
import types
import PyPDF2

from pdfminer.layout import LAParams
from pdfminer.converter import PDFPageAggregator
from pdfminer.pdfinterp import PDFResourceManager
from pdfminer.pdfinterp import PDFPageInterpreter
from pdfminer.pdfpage import PDFPage
from pdfminer.layout import LTTextBoxHorizontal


class Scrapper:
    DEV_MODE = True
    logger = None

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        logging.basicConfig()
        self.logger.setLevel(logging.DEBUG)
        self.logger.info("Logger created")

    def get_local_file(self, file):
        return os.path.join("dev-data", file)

    def write_file(self, file, content):
        my_file = open(self.get_local_file(file), "wb")  # open file in write mode
        my_file.write(content)  # write to file
        my_file.close()  # close file

    def read_file(self, file):
        my_file = open(self.get_local_file(file), "r")  # open file in write mode
        content = my_file.read()  # read from a file
        my_file.close()  # close file
        return content

    def open_file(self, file, mode= 'rb'):
        my_file = open(self.get_local_file(file), mode)
        return my_file

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
        content = self.scrape_content()
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

    def get_county_list(self, url: str = 'https://www.google.com/covid19/mobility/'):
        html = self.get_content(url)
        page = BeautifulSoup(html, 'lxml')
        links = [tag['href'] for tag in page.select("div.country-data > a.download-link")]
        df = pd.DataFrame(data=links, columns=['url'])
        df['country'], df['date'], df['country_name'] = df.url.apply(self.get_date_country_cname)
        self.logger.info('Finished getting country list. Fount {} countries.'.format(len(df)))
        return df

    def parsedocument(self, document):
        # convert all horizontal text into a lines list (one entry per line)
        # document is a file stream
        lines = []
        rsrcmgr = PDFResourceManager()
        laparams = LAParams()
        device = PDFPageAggregator(rsrcmgr, laparams=laparams)
        interpreter = PDFPageInterpreter(rsrcmgr, device)
        for page in PDFPage.get_pages(document):
            interpreter.process_page(page)
            layout = device.get_result()
            for element in layout:
                if isinstance(element, LTTextBoxHorizontal):
                    lines.extend(element.get_text().splitlines())
        return lines

    def get_clean_number(self, text: str):
        return int(text.replace('%', ''))

    def get_national_data(self, url):
        self.logger.info(f'Getting natinal data for {url}')
        self.scrape_content(url)
        lines = self.parsedocument(self.open_file(self.url_to_file(url)))
        data = [

            ['transit', self.get_clean_number(lines[56])],
            ['retail_recr', self.get_clean_number(lines[13])],
            ['grocery_pharm', self.get_clean_number(lines[16])],
            ['workplace', self.get_clean_number(lines[59])],
            ['residential', self.get_clean_number(lines[62])]
        ]
        self.logger.info(f'Data found {data}')
        df = pd.DataFrame(data=data, columns=['entity', 'value'])
        df['country'], df['date'], df['country_name'] = self.get_date_country_cname(url)
        df['location'] = "COUNTRY OVERALL"
        return df

# Scrapper().get_county_list()
Scrapper().get_national_data('https://www.gstatic.com/covid19/mobility/2020-03-29_AF_Mobility_Report_en.pdf')