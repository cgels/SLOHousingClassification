#!/usr/bin/python

import sqlite3

conn = sqlite3.connect('slo_housing.db')
print("Opened database successfully");

conn.execute('''CREATE TABLE HOUSES
       (ID             INTEGER    PRIMARY KEY AUTOINCREMENT,
        MLS_ID         INTEGER    UNIQUE,
        CITY           TEXT       NOT NULL,
        ADDRESS        TEXT       NOT NULL,
        BED            INT        NOT NULL,
        BATH           INT        NOT NULL,
        LIST_PRICE     REAL       NOT NULL,
        SQ_FOOTAGE     REAL       NOT NULL,
        PRICE_PER_SQFT REAL       NOT NULL,
        LIST_DATE      TEXT       NOT NULL
        );''')

print("Table created successfully");

conn.close()

