import psycopg2
import os
import csv
from dotenv import load_dotenv

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

    def _upload_csv(self, csv_file, table: str):
        with open(csv_file, 'r') as f:
            reader = csv.reader(f)
            headers = next(reader)  # Skip header row

            # Build insert query dynamically
            columns = ", ".join(headers)
            placeholders = ", ".join(["%s"] * len(headers))
            insert_query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders});"

            self.cursor.executemany(insert_query, reader)
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
                TAIL_NUMBER VARCHAR(10) NOT NULL,
                ORIGIN_AIRPORT VARCHAR(5) NOT NULL,
                DESTINATION_AIRPORT VARCHAR(5) NOT NULL,
                SCHEDULED_DEPARTURE INTEGER NOT NULL,
                DEPARTURE_TIME INTEGER NOT NULL,
                DEPARTURE_DELAY INTEGER NOT NULL,
                TAXI_OUT INTEGER NOT NULL,
                WHEELS_OFF INTEGER NOT NULL,
                SCHEDULED_TIME INTEGER NOT NULL,
                ELAPSED_TIME INTEGER NOT NULL,
                AIR_TIME INTEGER NOT NULL,
                DISTANCE INTEGER NOT NULL,
                WHEELS_ON INTEGER NOT NULL,
                TAXI_IN INTEGER NOT NULL,
                SCHEDULED_ARRIVAL INTEGER NOT NULL,
                ARRIVAL_TIME INTEGER NOT NULL,
                ARRIVAL_DELAY INTEGER NOT NULL,
                DIVERTED BOOLEAN NOT NULL,
                CANCELLED BOOLEAN NOT NULL,
                CANCELLATION_REASON CHAR(1) CHECK(CANCELLATION_REASON IN ('A','B','C','D') OR CANCELLATION_REASON = ' '),
                AIR_SYSTEM_DELAY VARCHAR,
                SECURITY_DELAY VARCHAR,
                AIRLINE_DELAY VARCHAR,
                LATE_AIRCRAFT_DELAY VARCHAR,
                WEATHER_DELAY VARCHAR
            );
        """
        self.cursor.execute(sql)
        self.connection.commit()

        self._upload_csv("datasets\\flights.csv", "flights")

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

        self._upload_csv("datasets\\airlines.csv", "airlines")

    def _create_table_airports(self):
        sql = """
            CREATE TABLE IF NOT EXISTS airports (
                id SERIAL PRIMARY KEY,
                IATA_CODE VARCHAR(5) NOT NULL,
                AIRPORT VARCHAR(50) NOT NULL,
                CITY VARCHAR(50) NOT NULL,
                STATE VARCHAR(50) NOT NULL,
                COUNTRY VARCHAR(50) NOT NULL,
                LATITUDE REAL NOT NULL,
                LONGITUDE REAL NOT NULL
            );
        """
        self.cursor.execute(sql)
        self.connection.commit()

        self._upload_csv("datasets\\airports.csv", "airports")

    def _create_table_cancellation_codes(self):
        sql = """
            CREATE TABLE IF NOT EXISTS cancellation_codes (
                id SERIAL PRIMARY KEY,
                CANCELLATION_REASON CHAR(1) CHECK(CANCELLATION_REASON IN ('A','B','C','D')) NOT NULL,
                CANCELLATION_DESCRIPTION VARCHAR(50) NOT NULL,
            );
        """
        self.cursor.execute(sql)
        self.connection.commit()

        self._upload_csv("datasets\\cancellation_codes.csv", "cancellation_codes")

    def _create_table_main(self):
        sql = """
            CREATE TABLE IF NOT EXISTS main AS
                SELECT
                    f.YEAR,
                    f.MONTH,
                    f.DAY,
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
                    CANCELLATION_REASON CHAR(1) CHECK(NULLIF(CANCELLATION_REASON, '') IS NULL OR CANCELLATION_REASON IN ('A', 'B', 'C', 'D')) AS CANCELLATION_REASON,
                    cc.CANCELLATION_DESCRIPTION,
                    NULLIF(f.AIR_SYSTEM_DELAY, '')::INTEGER AS AIR_SYSTEM_DELAY,
                    NULLIF(f.SECURITY_DELAY, '')::INTEGER AS SECURITY_DELAY,
                    NULLIF(f.AIRLINE_DELAY, '')::INTEGER AS AIRLINE_DELAY,
                    NULLIF(f.LATE_AIRCRAFT_DELAY, '')::INTEGER AS LATE_AIRCRAFT_DELAY,
                    NULLIF(f.WEATHER_DELAY, '')::INTEGER AS WEATHER_DELAY
                FROM flights f
                    INNER JOIN airlines al
                        ON f.AIRLINE = al.AIRLINE
                    INNER JOIN airports ap1
                        on f.ORIGIN_AIRPORT = ap1.AIRPORT
                    INNER JOIN airports ap2
                        on f.DESTINATION_AIRPORT = ap2.AIRPORT
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



