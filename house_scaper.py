from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import requests
import sqlite3
import datetime

df = pd.DataFrame({"MLS": [],"Street": [], "City":[],"List Price":[],"Bedrooms":[],"Bathrooms":[],"Square Footage":[],"Date":[],  "Price/SqFt":[]})
months = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October",
              "November", "December"]
month_map = {key: int(val) for key, val in zip(months, range(1, 13))}


def sqlize_string(string):
    return "'" + string + "'"

def get_date(string):
    chunked = string.split()
    return sqlize_string(str(datetime.date(2016, month_map[chunked[0]], int(chunked[1]))))

def scraping(dataframe):
    page = requests.get("http://www.slocountyhomes.com/newlistex.php")
    data = BeautifulSoup(page.text, "html.parser")
    hdrs = ["Bathrooms", "Bedrooms", "City", "Date", "List Price", "MLS", "Price/SqFt", "Square Footage", "Street"]
    idx_map = { hdr:idx for hdr, idx in zip(hdrs, range(len(hdrs))) }
    row_entry = [0] * len(hdrs)


    table_rows = data.find("tbody").find_all("tr")

    for row in table_rows:
        listing_date = ""
        cells = row.find_all("td")
        if len(cells) == 1:
            listing_date = cells[0].text

        else:
            assert listing_date != ""
            assert len(cells) == 8
            ## CELL ORDER -->   MLS #	Street	City	List Price	Beds	Baths	Sq Footage
            row_entry[idx_map["MLS"]] = int(cells[0].text.strip())
            row_entry[idx_map["Street"]] = sqlize_string(cells[1].text.strip())
            row_entry[idx_map["City"]] = sqlize_string(cells[2].text.strip())
            row_entry[idx_map["List Price"]] = int(cells[3].text.strip()[1:].replace(",", ""))
            row_entry[idx_map["Bedrooms"]] = int(cells[4].text.strip())
            row_entry[idx_map["Bathrooms"]] = int(cells[5].text.strip())
            row_entry[idx_map["Square Footage"]] = int(cells[6].text.strip())
            row_entry[idx_map["Date"]] = get_date(listing_date)
            row_entry[idx_map["Price/SqFt"]] = row_entry[idx_map["List Price"]] / row_entry[idx_map["Square Footage"]]
            ## append this row to dataframe
            dataframe.loc[len(dataframe)] = row_entry

    dataframe = dataframe.drop_duplicates()
    return dataframe