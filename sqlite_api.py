import sqlite3
import datetime

class SLOHouseDatabase:

    def __init__(self):
        self.database = 'slo_housing.db'
        self.insertStmt = "INSERT OR REPLACE INTO HOUSES (MLS_ID, CITY, ADDRESS, BED, BATH, LIST_PRICE, SQ_FOOTAGE, PRICE_PER_SQFT, LIST_DATE) \
          VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {})"
        self.hdrs = ["Bathrooms", "Bedrooms", "City", "Date", "ListPrice", "MLS", "Price/SqFt", "SqFt", "Street"]
        self.idx_map = {hdr: idx for hdr, idx in zip(self.hdrs, range(len(self.hdrs)))}


    def _insert_row(self, conn, row):
        conn = sqlite3.connect(self.database)
        conn.execute(self.insertStmt.format(row[self.idx_map['MLS']], row[self.idx_map['City']], row[self.idx_map['Street']],
                                            row[self.idx_map['Bedrooms']], row[self.idx_map['Bathrooms']],
                                            row[self.idx_map['ListPrice']], row[self.idx_map['SqFt']],int(row[self.idx_map['Price/SqFt']]),
                                            row[self.idx_map['Date']]))


    def insert_dataframe(self, df):
        connect = sqlite3.connect(self.database)
        print("Connected to " + self.database)

        for idx, row in df.iterrows():
            self._insert_row(connect, row)

        connect.commit()
        print("Records created successfully")
        connect.close()
