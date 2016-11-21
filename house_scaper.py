from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import requests
import sqlite3
import datetime

df = pd.DataFrame({"MLS": [],"Street": [], "City":[],"ListPrice":[],"Bedrooms":[],"Bathrooms":[],"SqFt":[],"Date":[],  "Price/SqFt":[]})
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
    hdrs = ["Bathrooms", "Bedrooms", "City", "Date", "List Price", "MLS", "Price/SqFt", "SqFt", "Street"]
    idx_map = { hdr:idx for hdr, idx in zip(hdrs, range(len(hdrs))) }


    table_rows = data.find_all('tr')
    # print(table_rows)
    listing_date = ""
    for row in table_rows:
        row_entry = [0] * len(hdrs)
        cells = row.find_all("td", recursive=True)
        if len(cells) == 1:
            listing_date = cells[0].text.strip()
            assert listing_date != ""

        elif 0 < len(cells) <= 8 and len(cells) != 3:
            ## CELL ORDER -->   MLS #	Street	City	List Price	Beds	Baths	Sq Footage
            row_entry[idx_map["MLS"]] = int(cells[0].text.strip())
            row_entry[idx_map["Street"]] = sqlize_string(cells[1].text.strip())
            row_entry[idx_map["City"]] = sqlize_string(cells[2].text.strip())
            row_entry[idx_map["List Price"]] = int(cells[3].text.strip()[1:].replace(",", ""))
            row_entry[idx_map["Bedrooms"]] = int(cells[4].text.strip())
            row_entry[idx_map["Bathrooms"]] = int(cells[5].text.strip())
            try: # handle missing Sq footage
                row_entry[idx_map["SqFt"]] = int(cells[6].text.strip())
                row_entry[idx_map["Price/SqFt"]] = row_entry[idx_map["List Price"]] / row_entry[
                    idx_map["SqFt"]]
            except ValueError:
                row_entry[idx_map["SqFt"]] = -1
            row_entry[idx_map["Date"]] = get_date(listing_date)
            ## append this row to dataframe
            # print(row_entry)
            dataframe.loc[len(dataframe)] = row_entry
    ## data integrity
    dataframe.drop(dataframe[dataframe.SqFt == -1].index, inplace=True)
    dataframe.drop_duplicates(inplace=True)
    return dataframe

df = scraping(df)