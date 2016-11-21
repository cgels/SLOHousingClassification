import sqlite3
import datetime

class SLOHouseDatabase:

    def __init__(self):
        self.database = 'slo_housing.db'
        self.insertStmt = "INSERT OR REPLACE INTO HOUSES (MLS_ID, CITY, ADDRESS, BED, BATH, LIST_PRICE, SQ_FOOTAGE, PRICE_PER_SQFT, LIST_DATE) \
          VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {})"
        self.selectStmt = "SELECT * FROM HOUSES WHERE "
        self.hdrs = ["Bathrooms", "Bedrooms", "City", "Date", "ListPrice", "MLS", "Price/SqFt", "SqFt", "Street"]
        self.idx_map = {hdr: idx for hdr, idx in zip(self.hdrs, range(len(self.hdrs)))}


    def _insert_row(self, conn, row):
        mls = row[self.idx_map['MLS']]
        city = row[self.idx_map['City']]
        street = row[self.idx_map['Street']]
        bed = int(row[self.idx_map['Bedrooms']])
        bath = int(row[self.idx_map['Bathrooms']])
        price = int(row[self.idx_map['ListPrice']])
        sqft = int(row[self.idx_map['SqFt']])
        price_per = int(row[self.idx_map['Price/SqFt']])
        date = row[self.idx_map['Date']]
        conn.execute(self.insertStmt.format(mls, city, street, bed, bath, price, sqft, price_per, date))


    def insert_dataframe(self, df):
        connect = sqlite3.connect(self.database)
        print("Connected to " + self.database)

        for idx, row in df.iterrows():
            self._insert_row(connect, row)

        connect.commit()
        print("Records created successfully")
        connect.close()

    def _select_row(self, stmt):
        connect = sqlite3.connect(self.database)
        c = connect.cursor()
        result = c.execute(stmt, tuple(vals))
        connect.close()
        return result.fetchall()

    def select_row(self, stmt):
        connect = sqlite3.connect(self.database)
        c = connect.cursor()
        result = c.execute(stmt)
        connect.close()
        return result.fetchall()



