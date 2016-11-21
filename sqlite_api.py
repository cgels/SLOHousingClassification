import sqlite3
import datetime

class SLOHouseDatabase:

    def __init__(self):
        self.database = 'slo_housing.db'
        self.insertStmt = "INSERT OR REPLACE INTO HOUSES (MLS_ID,BED,BATH,CITY, ADDRESS, LIST_PRICE, SQ_FOOTAGE, PRICE_PER_SQFT, LIST_DATE) \
          VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {})"

    def _insert_row(self, conn, row):
        conn = sqlite3.connect(self.database)
        conn.execute(self.insertStmt.format(row['MLS'], row['Bedrooms'], row['Bathrooms'], row['City'],
                                 row['Street'], row['List Price'], row['Square Footage'],
                                 int(row['List Price']) / int(row['Square Footage']),
                                 datetime.date(2016, self.month_map[row['Date'].split()[0]],
                                 int(row['Date'].split()[1]))))

    def insert_dataframe(self, df):
        connect = sqlite3.connect(self.database)
        print("Connected to " + self.database)
        for idx, row in df.iterrows():
            self._insert_row(connect, row)
        connect.commit()
        print("Records created successfully")
        connect.close()
