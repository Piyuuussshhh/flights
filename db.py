import psycopg2
import os
import csv
from dotenv import load_dotenv

from constants import FLIGHTS_COLUMN_NAMES, AIRLINES_COLUMN_NAMES, AIRPORT_COLUMN_NAMES, CC_COLUMN_NAMES

load_dotenv()

class Database:
    connection = None
    cursor = None

    def __init__(self):
        try:
            Database.connection = psycopg2.connect(
                dbname=os.getenv("DB_NAME"),
                user=os.getenv("DB_USER"),
                password=os.getenv("DB_PASS"),
                host=os.getenv("DB_HOST"),
                port=os.getenv("DB_PORT")
            )
            Database.cursor = Database.connection.cursor()
            print("Database connected successfully!")
        except psycopg2.Error as e:
            print("Error connecting to database:", e)

    def query(self, sql, params=None, fetch=False):
        try:
            Database.cursor.execute(sql, params or ())
            if fetch:
                return Database.cursor.fetchall()
            else:
                Database.connection.commit()
        except psycopg2.Error as e:
            print("Error executing query:", e)

    def close(self):
        """Closes the cursor and connection."""
        if Database.cursor:
            Database.cursor.close()
        if Database.connection:
            Database.connection.close()
            print("Database connection closed.")

    def _upload_csv(self, csv_file, table: str, columns):
        with open(csv_file, "r") as file:
            column_list = f"({', '.join(columns)})" if columns else ""
            self.cursor.copy_expert(f"COPY {table} {column_list} FROM STDIN WITH CSV HEADER NULL ''", file)

        self.connection.commit()

    def _create_table_flights(self):
        sql = """
            CREATE TABLE IF NOT EXISTS flights (
                id SERIAL PRIMARY KEY,
                YEAR VARCHAR(4) NOT NULL,
                MONTH VARCHAR(2) NOT NULL,
                DAY VARCHAR(2) NOT NULL,
                DAY_OF_WEEK VARCHAR(1) NOT NULL,
                AIRLINE VARCHAR(2) NOT NULL,
                FLIGHT_NUMBER VARCHAR(10) NOT NULL,
                TAIL_NUMBER VARCHAR(10),
                ORIGIN_AIRPORT VARCHAR(5) NOT NULL,
                DESTINATION_AIRPORT VARCHAR(5) NOT NULL,
                SCHEDULED_DEPARTURE INTEGER NOT NULL,
                DEPARTURE_TIME REAL,
                DEPARTURE_DELAY REAL,
                TAXI_OUT REAL,
                WHEELS_OFF REAL,
                SCHEDULED_TIME REAL,
                ELAPSED_TIME REAL,
                AIR_TIME REAL,
                DISTANCE INTEGER NOT NULL,
                WHEELS_ON REAL,
                TAXI_IN REAL,
                SCHEDULED_ARRIVAL INTEGER NOT NULL,
                ARRIVAL_TIME REAL,
                ARRIVAL_DELAY REAL,
                DIVERTED BOOLEAN NOT NULL,
                CANCELLED BOOLEAN NOT NULL,
                CANCELLATION_REASON CHAR(1),
                AIR_SYSTEM_DELAY REAL,
                SECURITY_DELAY REAL,
                AIRLINE_DELAY REAL,
                LATE_AIRCRAFT_DELAY REAL,
                WEATHER_DELAY REAL
            );
        """
        self.cursor.execute(sql)
        self.connection.commit()

        self._upload_csv("C:\\Users\\admin\\Desktop\\Programming\\data_science\\flights_pt2\\datasets\\flights_cleaned.csv", "flights", FLIGHTS_COLUMN_NAMES)

    def _create_table_airlines(self):
        sql = """
            CREATE TABLE IF NOT EXISTS airlines (
                id SERIAL PRIMARY KEY,
                IATA_CODE VARCHAR(5) NOT NULL,
                AIRLINE VARCHAR(50) NOT NULL
            );
        """
        self.cursor.execute(sql)
        self.connection.commit()

        self._upload_csv("C:\\Users\\admin\\Desktop\\Programming\\data_science\\flights_pt2\\datasets\\airlines.csv", "airlines", AIRLINES_COLUMN_NAMES)

    def _create_table_airports(self):
        sql = """
            CREATE TABLE IF NOT EXISTS airports (
                id SERIAL PRIMARY KEY,
                IATA_CODE VARCHAR(5) NOT NULL,
                AIRPORT VARCHAR(100) NOT NULL,
                CITY VARCHAR(50) NOT NULL,
                STATE VARCHAR(50) NOT NULL,
                COUNTRY VARCHAR(50) NOT NULL,
                LATITUDE REAL,
                LONGITUDE REAL
            );
        """
        self.cursor.execute(sql)
        self.connection.commit()

        self._upload_csv("C:\\Users\\admin\\Desktop\\Programming\\data_science\\flights_pt2\\datasets\\airports.csv", "airports", AIRPORT_COLUMN_NAMES)

    def _create_table_cancellation_codes(self):
        sql = """
            CREATE TABLE IF NOT EXISTS cancellation_codes (
                id SERIAL PRIMARY KEY,
                CANCELLATION_REASON CHAR(1) CHECK(CANCELLATION_REASON IN ('A','B','C','D')) NOT NULL,
                CANCELLATION_DESCRIPTION VARCHAR(50) NOT NULL
            );
        """
        self.cursor.execute(sql)
        self.connection.commit()

        self._upload_csv("C:\\Users\\admin\\Desktop\\Programming\\data_science\\flights_pt2\\datasets\\cancellation_codes.csv", "cancellation_codes", CC_COLUMN_NAMES)

    def _create_table_main(self):
        sql = """
            CREATE TABLE IF NOT EXISTS main AS
                SELECT
                    MAKE_DATE(f.YEAR::INTEGER, f.MONTH::INTEGER, f.DAY::INTEGER) AS FLIGHT_DATE,
                    f.DAY_OF_WEEK,
                    f.AIRLINE,
                    al.AIRLINE as AIRLINE_NAME,
                    f.FLIGHT_NUMBER,
                    f.TAIL_NUMBER,
                    f.ORIGIN_AIRPORT,
                    ap1.AIRPORT as ORIGIN_AIRPORT_NAME,
                    f.DESTINATION_AIRPORT,
                    ap2.AIRPORT AS DESTINATION_AIRPORT_NAME,
                    f.SCHEDULED_DEPARTURE,
                    f.DEPARTURE_TIME,
                    f.DEPARTURE_DELAY,
                    f.TAXI_OUT,
                    f.WHEELS_OFF,
                    f.SCHEDULED_TIME,
                    f.ELAPSED_TIME,
                    f.AIR_TIME,
                    f.DISTANCE,
                    f.WHEELS_ON,
                    f.TAXI_IN,
                    f.SCHEDULED_ARRIVAL,
                    f.ARRIVAL_TIME,
                    f.ARRIVAL_DELAY,
                    f.DIVERTED,
                    f.CANCELLED,
                    f.CANCELLATION_REASON,
                    cc.CANCELLATION_DESCRIPTION,
                    f.AIR_SYSTEM_DELAY,
                    f.SECURITY_DELAY,
                    f.AIRLINE_DELAY,
                    f.LATE_AIRCRAFT_DELAY,
                    f.WEATHER_DELAY
                FROM flights f
                    INNER JOIN airlines al
                        ON f.AIRLINE = al.IATA_CODE
                    INNER JOIN airports ap1
                        on f.ORIGIN_AIRPORT = ap1.IATA_CODE
                    INNER JOIN airports ap2
                        on f.DESTINATION_AIRPORT = ap2.IATA_CODE
                    LEFT JOIN cancellation_codes cc
                        on f.CANCELLATION_REASON = cc.CANCELLATION_REASON
        """
        self.cursor.execute(sql)
        self.connection.commit()

    def create_tables(self):
        self._create_table_flights()
        self._create_table_airlines()
        self._create_table_airports()
        self._create_table_cancellation_codes()
        self._create_table_main()



