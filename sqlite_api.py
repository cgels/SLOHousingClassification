import sqlite3
import datetime

class Database:

    def __init__(self):
        self.database = 'slo_housing.db'
        self.insertStmt = "INSERT INTO HOUSES (MLS_ID,BED,BATH,CITY, ADDRESS, LIST_PRICE, SQ_FOOTAGE, PRICE_PER_SQFT, LIST_DATE) \
          VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {})"
        self.deleteStmt = "DELETE FROM HOUSES WHERE "
        self.month_map = self._get_month_map()


    def _get_month_map(self):
        months = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October",
                  "November", "December"]
        return {key: int(val) for key, val in zip(months, range(1, 13))}

    def insert_row(self, row):
        conn = sqlite3.connect(self.database)
        conn.execute(self.insertStmt.format(row['MLS'], row['Bedrooms'], row['Bathrooms'], row['City'],
                                 row['Street'], row['List Price'], row['Square Footage'],
                                 int(row['List Price']) / int(row['Square Footage']),
                                 datetime.date(2016, self.month_map[row['Date'].split()[0]],
                                 int(row['Date'].split()[1]))))
