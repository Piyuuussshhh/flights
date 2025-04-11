import psycopg2
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

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

    # def _create_table_flights(self):
    #     sql = """
    #         CREATE TABLE IF NOT EXISTS flights (
    #             id SERIAL PRIMARY KEY,
    #             YEAR VARCHAR(4) NOT NULL,
    #             MONTH VARCHAR(2) NOT NULL,
    #             DAY VARCHAR(2) NOT NULL,
    #             DAY_OF_WEEK VARCHAR(1) NOT NULL,
    #             AIRLINE VARCHAR(2) NOT NULL,
    #             FLIGHT_NUMBER VARCHAR(10) NOT NULL,
    #             TAIL_NUMBER VARCHAR(10),
    #             ORIGIN_AIRPORT VARCHAR(5) NOT NULL,
    #             DESTINATION_AIRPORT VARCHAR(5) NOT NULL,
    #             SCHEDULED_DEPARTURE INTEGER NOT NULL,
    #             DEPARTURE_TIME REAL,
    #             DEPARTURE_DELAY REAL,
    #             TAXI_OUT REAL,
    #             WHEELS_OFF REAL,
    #             SCHEDULED_TIME REAL,
    #             ELAPSED_TIME REAL,
    #             AIR_TIME REAL,
    #             DISTANCE INTEGER NOT NULL,
    #             WHEELS_ON REAL,
    #             TAXI_IN REAL,
    #             SCHEDULED_ARRIVAL INTEGER NOT NULL,
    #             ARRIVAL_TIME REAL,
    #             ARRIVAL_DELAY REAL,
    #             DIVERTED BOOLEAN NOT NULL,
    #             CANCELLED BOOLEAN NOT NULL,
    #             CANCELLATION_REASON CHAR(1),
    #             AIR_SYSTEM_DELAY REAL,
    #             SECURITY_DELAY REAL,
    #             AIRLINE_DELAY REAL,
    #             LATE_AIRCRAFT_DELAY REAL,
    #             WEATHER_DELAY REAL
    #         );
    #     """
    #     self.cursor.execute(sql)
    #     self.connection.commit()

    #     self._upload_csv("C:\\Users\\admin\\Desktop\\Programming\\data_science\\flights_pt2\\datasets\\flights_cleaned.csv", "flights", FLIGHTS_COLUMN_NAMES)

    def _create_table_dim_airlines(self):
        sql = """
            DROP TABLE IF EXISTS dim_airlines;
            CREATE TABLE IF NOT EXISTS dim_airlines (
                id SERIAL PRIMARY KEY,
                IATA_CODE VARCHAR(5) NOT NULL,
                AIRLINE VARCHAR(50) NOT NULL,
                UNIQUE(IATA_CODE)
            );
        """
        self.cursor.execute(sql)
        self.connection.commit()

        self._upload_csv("C:\\Users\\admin\\Desktop\\Programming\\data_science\\flights_pt2\\datasets\\airlines.csv", "dim_airlines", AIRLINES_COLUMN_NAMES)

    def _create_table_dim_airports(self):
        sql = """
            DROP TABLE IF EXISTS dim_airports;
            CREATE TABLE IF NOT EXISTS dim_airports (
                id SERIAL PRIMARY KEY,
                IATA_CODE VARCHAR(5) NOT NULL,
                AIRPORT VARCHAR(100) NOT NULL,
                CITY VARCHAR(50) NOT NULL,
                STATE VARCHAR(50) NOT NULL,
                COUNTRY VARCHAR(50) NOT NULL,
                LATITUDE REAL,
                LONGITUDE REAL,
                UNIQUE(IATA_CODE)
            );
        """
        self.cursor.execute(sql)
        self.connection.commit()

        self._upload_csv("C:\\Users\\admin\\Desktop\\Programming\\data_science\\flights_pt2\\datasets\\airports.csv", "dim_airports", AIRPORT_COLUMN_NAMES)

    def _create_table_dim_cancellation_codes(self):
        sql = """
            DROP TABLE IF EXISTS dim_cancellation_codes;
            CREATE TABLE IF NOT EXISTS dim_cancellation_codes (
                id SERIAL PRIMARY KEY,
                CANCELLATION_REASON CHAR(1) CHECK(CANCELLATION_REASON IN ('A','B','C','D')) NOT NULL,
                CANCELLATION_DESCRIPTION VARCHAR(50) NOT NULL,
                UNIQUE(CANCELLATION_REASON)
            );
        """
        self.cursor.execute(sql)
        self.connection.commit()

        self._upload_csv("C:\\Users\\admin\\Desktop\\Programming\\data_science\\flights_pt2\\datasets\\cancellation_codes.csv", "dim_cancellation_codes", CC_COLUMN_NAMES)

    def _create_table_dim_dates(self):
        start_date = datetime(2015, 1, 1)
        end_date = datetime(2015, 12, 31)

        self.cursor.execute("""
            DROP TABLE IF EXISTS dim_dates;
            CREATE TABLE dim_dates (
                date_id DATE PRIMARY KEY,
                day INT,
                month INT,
                year INT,
                weekday TEXT
            );
        """)

        current = start_date
        while current <= end_date:
            self.cursor.execute("""
                INSERT INTO dim_dates (date_id, day, month, year, weekday)
                VALUES (%s, %s, %s, %s, %s)
            """, (current, current.day, current.month, current.year, current.strftime("%A")))
            current += timedelta(days=1)

        self.connection.commit()

    def _create_table_fact_flights(self):
        def to_time(t):
            """Convert HHMM integer to time"""
            try:
                t = int(t)
                hour = t // 100
                minute = t % 100
                return datetime.strptime(f"{hour:02}:{minute:02}", "%H:%M").time()
            except:
                return None

        def safe(val):
            if pd.isna(val):
                return None
            if isinstance(val, (np.generic, np.bool_)):
                return val.item()  # Convert NumPy scalar to Python native type
            return val

        self.cursor.execute("""
            DROP TABLE IF EXISTS fact_flights;
            CREATE TABLE fact_flights (
                id SERIAL PRIMARY KEY,
                date_id DATE REFERENCES dim_dates(date_id),
                airline_id VARCHAR REFERENCES dim_airlines(IATA_CODE),
                flight_number TEXT,
                tail_number TEXT,
                origin_airport VARCHAR REFERENCES dim_airports(IATA_CODE),
                dest_airport VARCHAR REFERENCES dim_airports(IATA_CODE),
                scheduled_departure TIME,
                departure_time TIME,
                departure_delay INTEGER,
                scheduled_time INTEGER,
                elapsed_time INTEGER,
                air_time INTEGER,
                distance REAL,
                scheduled_arrival TIME,
                arrival_time TIME,
                arrival_delay INTEGER,
                overall_delay NUMERIC,
                diverted BOOLEAN,
                cancelled BOOLEAN,
                cancellation_code CHAR REFERENCES dim_cancellation_codes(CANCELLATION_REASON),
                air_system_delay REAL,
                security_delay REAL,
                airline_delay REAL,
                late_aircraft_delay REAL,
                weather_delay REAL
            );
        """)

        dtype_mapping = {
            'YEAR': 'Int32',
            'MONTH': 'Int32',
            'DAY': 'Int32',
            'DAY_OF_WEEK': 'Int32',
            'AIRLINE': 'string',
            'FLIGHT_NUMBER': 'string',
            'TAIL_NUMBER': 'string',
            'ORIGIN_AIRPORT': 'string',
            'DESTINATION_AIRPORT': 'string',
            'SCHEDULED_DEPARTURE': 'Int32',
            'DEPARTURE_TIME': 'Int32',
            'DEPARTURE_DELAY': 'Int32',
            'TAXI_OUT': 'Int32',
            'WHEELS_OFF': 'Int32',
            'SCHEDULED_TIME': 'Int32',
            'ELAPSED_TIME': 'Int32',
            'AIR_TIME': 'Int32',
            'DISTANCE': 'Int32',
            'WHEELS_ON': 'Int32',
            'TAXI_IN': 'Int32',
            'SCHEDULED_ARRIVAL': 'Int32',
            'ARRIVAL_TIME': 'Int32',
            'ARRIVAL_DELAY': 'Int32',
            'DIVERTED': 'boolean',
            'CANCELLED': 'boolean',
            'CANCELLATION_REASON': 'string',
            'AIR_SYSTEM_DELAY': 'Int32',
            'SECURITY_DELAY': 'Int32',
            'AIRLINE_DELAY': 'Int32',
            'LATE_AIRCRAFT_DELAY': 'Int32',
            'WEATHER_DELAY': 'Int32'
        }

        insert_query = """
            INSERT INTO fact_flights (
                date_id, airline_id, flight_number, tail_number, origin_airport, dest_airport,
                scheduled_departure, departure_time, departure_delay, scheduled_time, elapsed_time,
                air_time, distance, scheduled_arrival, arrival_time, arrival_delay, overall_delay,
                diverted, cancelled, cancellation_code, air_system_delay, security_delay,
                airline_delay, late_aircraft_delay, weather_delay
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        csv_path = "C:\\Users\\admin\\Desktop\\Programming\\data_science\\flights_pt2\\datasets\\flights_cleaned.csv"

        i = 0
        for chunk in pd.read_csv(csv_path, dtype=dtype_mapping, chunksize=100000):
            chunk['flight_date'] = pd.to_datetime(chunk[['YEAR', 'MONTH', 'DAY']])

            print(f"Inserted {(i + 1) * 100_000} records of fact_flights!")

            for row in chunk.itertuples(index=False):
                try:
                    cancel_code = None if pd.isna(row.CANCELLATION_REASON) or row.CANCELLATION_REASON == "NA" else row.CANCELLATION_REASON[:1]
                    dep_delay = 0 if pd.isna(row.DEPARTURE_DELAY) else row.DEPARTURE_DELAY
                    arr_delay = 0 if pd.isna(row.ARRIVAL_DELAY) else row.ARRIVAL_DELAY
                    overall_delay = dep_delay + arr_delay

                    self.cursor.execute(insert_query, (
                        safe(row.flight_date.date()),
                        safe(row.AIRLINE),
                        safe(row.FLIGHT_NUMBER),
                        safe(row.TAIL_NUMBER),
                        safe(row.ORIGIN_AIRPORT),
                        safe(row.DESTINATION_AIRPORT),
                        safe(to_time(row.SCHEDULED_DEPARTURE)),
                        safe(to_time(row.DEPARTURE_TIME)),
                        safe(row.DEPARTURE_DELAY),
                        safe(row.SCHEDULED_TIME),
                        safe(row.ELAPSED_TIME),
                        safe(row.AIR_TIME),
                        safe(row.DISTANCE),
                        safe(to_time(row.SCHEDULED_ARRIVAL)),
                        safe(to_time(row.ARRIVAL_TIME)),
                        safe(row.ARRIVAL_DELAY),
                        safe(overall_delay),
                        safe(bool(row.DIVERTED) if not pd.isna(row.DIVERTED) else None),
                        safe(bool(row.CANCELLED) if not pd.isna(row.CANCELLED) else None),
                        safe(cancel_code),
                        safe(row.AIR_SYSTEM_DELAY),
                        safe(row.SECURITY_DELAY),
                        safe(row.AIRLINE_DELAY),
                        safe(row.LATE_AIRCRAFT_DELAY),
                        safe(row.WEATHER_DELAY)
                    ))

                    self.connection.commit()

                except psycopg2.DataError as e:
                    print("⚠️ Insert failed on row:")
                    print(row)
                    print("🚨 Error:", e)
                    raise



    def create_tables(self):
        self._create_table_dim_airlines()
        self._create_table_dim_airports()
        self._create_table_dim_cancellation_codes()
        self._create_table_dim_dates()
        self._create_table_fact_flights()



