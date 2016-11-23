import sqlite3
import datetime
import pandas as pd

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


    def select_row(self, stmt):
        connect = sqlite3.connect(self.database)
        c = connect.cursor()
        result = c.execute(stmt)
        return result.fetchall()

    def get_dataframe_from_query(self, queryStmt=None):
        c = sqlite3.connect(self.database)
        if queryStmt:
            df = pd.read_sql(queryStmt, c)
        else:
            stmt = "SELECT * FROM HOUSES JOIN MLS_LISTINGS USING (MLS_ID)"
            df =  pd.read_sql(stmt, c)
        c.close()
        return df


class MLSDatabase:
    def __init__(self):
        self.database = 'slo_housing.db'
        self.insertStmt = "INSERT OR REPLACE INTO MLS_LISTINGS (MLS_ID, SUBTYPE, AREA, YR_BUILT, LOT_SQFT, VIEW, POOL, ARB_COMISSION) \
                  VALUES ({}, {}, {}, {}, {}, {}, {}, {})"
        self.hdrs = ["ListingID", "SubType", "MLSArea", "YrBuilt", "AcLSqft", "ViewYN", "PoolPrivateYN", "BAC"]
        self.idx_map = {hdr: idx for hdr, idx in zip(self.hdrs, range(len(self.hdrs)))}

    def create_db(self):
        conn = sqlite3.connect(self.database)
        print("Opened database successfully");
        try:
            conn.execute('''CREATE TABLE MLS_LISTINGS
                   (ID             INTEGER    PRIMARY KEY AUTOINCREMENT,
                    MLS_ID         INTEGER    NOT NULL,
                    SUBTYPE        TEXT       NOT NULL,
                    AREA           TEXT       NOT NULL,
                    YR_BUILT       INT        NOT NULL,
                    LOT_SQFT       INTEGER    NOT NULL,
                    VIEW           INT       NOT NULL,
                    POOL           INT       NOT NULL,
                    ARB_COMISSION  REAL       NOT NULL
                    );''')
        except Exception as err:
            print("NOTICE: ", err)

        print("Table created successfully");

        conn.close()

    def _insert_row(self, conn, row):
        mls = int(row[self.idx_map['ListingID']])
        subtype = self._sqlize_string(str(row[self.idx_map['SubType']]))
        area = self._sqlize_string(str(row[self.idx_map['MLSArea']]))
        year = int(row[self.idx_map['YrBuilt']])
        lotsize = int(row[self.idx_map['AcLSqft']])
        price = int(row[self.idx_map['ViewYN']])
        sqft = int(row[self.idx_map['PoolPrivateYN']])
        commission = float(row[self.idx_map['BAC']])

        try:
            conn.execute(self.insertStmt.format(mls, subtype, area, year, lotsize, price, sqft, commission))
        except Exception  as err:
            print(err)


    def insert_dataframe(self, df):
        connect = sqlite3.connect(self.database)
        print("Connected to " + self.database)

        for idx, row in df.iterrows():
            self._insert_row(connect, row)

        connect.commit()
        print("Records created successfully")
        connect.close()


    def select_row(self, stmt):
        connect = sqlite3.connect(self.database)
        c = connect.cursor()
        result = c.execute(stmt)
        connect.close()
        return result.fetchall()

    def _sqlize_string(self, string):
        return "'" + string + "'"


    def get_dataframe_from_query(self, queryStmt=None):
        c = sqlite3.connect(self.database)
        if queryStmt:
            df = pd.read_sql(queryStmt, c)
        else:
            stmt = "SELECT * FROM HOUSES JOIN MLS_LISTINGS USING (MLS_ID)"
            df =  pd.read_sql(stmt, c)
        c.close()
        return df
