import os
import logging
import urllib.request
from bs4 import BeautifulSoup


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
            'https://www.google.com/covid19/mobility/' : 'mobility'
        }
        return url_to_file[url]

    def get_content(self, url):
        file = self.url_to_file(url)
        if self.DEV_MODE and self.file_exists(file):
            self.logger.info(f'Reading file from cache {file}')
            return self.read_file(file)
        else:
            self.logger.info(f'Scrapping url {url}')
            with urllib.request.urlopen(url) as response:
                html = response.read()
                self.write_file(file, html)
                return html

    def get_county_list(self, url: str = 'https://www.google.com/covid19/mobility/'):
        html = self.get_content(url)
        page = BeautifulSoup(html, 'lxml')
        links = [tag['href'] for tag in page.select("div.country-data > a.download-link")]
        print(links)
Scrapper().get_county_list()

# get_country_list < - function(url="https://www.google.com/covid19/mobility/")
# {
#
#     # get webpage
#     page < - xml2:: read_html(url)
#
# # extract country urls
# country_urls < - rvest::html_nodes(page, "div.country-data > a.download-link") % > %
# rvest::html_attr("href")
#
# # create tibble from URL
# countries < - tibble(url=country_urls) % > %
# mutate(filename=basename(url),
#        date=map_chr(filename, ~strsplit(., "_")[[1]][1]),
# country = map_chr(filename, ~strsplit(., "_")[[1]][2]),
# country_name = countrycode::countrycode(country,
#                                         "iso2c",
#                                         "country.name")) % > %
# select(country, country_name, date, url)
#
# # return data
# return (countries)
#
# }