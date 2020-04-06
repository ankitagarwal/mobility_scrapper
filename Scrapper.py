import os
import urllib.request
from bs4 import BeautifulSoup


class Scrapper:
    DEV_MODE = True

    def write_file(self, content):
        my_file = open("file.html", "wb")  # open file in write mode
        my_file.write(content)  # write to file
        my_file.close()  # close file

    def read_file(self, file):
        my_file = open(file, "r")  # open file in write mode
        content = my_file.read()  # read from a file
        my_file.close()  # close file
        return content

    def file_exists(self, file):
        if os.path.exists(file) and os.path.isfile(file):
            return True
        else:
            return False

    def get_content(self, url):
        if self.DEV_MODE and self.file_exists()


    def get_county_list(self, url: str = 'https://www.google.com/covid19/mobility/'):
        with urllib.request.urlopen(url) as response:
            html = response.read()
            self.write_file(html)
            print(html)
            page = BeautifulSoup(html, 'html-parser')
            print(page)
            print(page.find_all("div.country-data > a.download-link"))

Scrapper().get_county_list("file.html")

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