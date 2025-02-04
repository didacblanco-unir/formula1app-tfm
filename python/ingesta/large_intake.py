import requests
import psycopg2
import time
from psycopg2.extras import execute_values
import json
from datetime import timedelta, datetime
from main_intake import connect_to_db, retry_on_failure, insert_data


API_BASE_URL = "https://api.openf1.org/v1/"

LARGE_TABLES = [
    "car_data",
    "intervals",
    "location",
]

    
def get_race_sessions_info(conn):
    query = """
        SELECT session_key, meeting_key, circuit_short_name, year FROM formula1.sessions
        WHERE session_name = 'Race';
        """
    with conn.cursor() as cur:
        cur.execute(query)
        rows = cur.fetchall()

    meeting_sessions = [row for row in rows]
    return meeting_sessions

def get_drivers_for_session(conn, session_id):
    query = """
        SELECT driver_number
        FROM formula1.drivers
        WHERE session_key = %s;
    """
    with conn.cursor() as cur:
        cur.execute(query, (session_id,))
        rows = cur.fetchall()
    
    driver_numbers = [row[0] for row in rows]
    return driver_numbers

def get_dates_for_session(conn, session_key):

    query = """
        SELECT date_start, date_end
        FROM formula1.sessions
        WHERE session_key = %s;
    """
    with conn.cursor() as cur:
        cur.execute(query, (session_key,))
        rows = cur.fetchall()
    
    date_start_str, date_end_str = rows[0]

    date_format = "%Y-%m-%dT%H:%M:%S"
    date_start = datetime.strptime(date_start_str[:-6], date_format)
    date_end = datetime.strptime(date_end_str[:-6], date_format)

    if not date_start or not date_end:
        return []

    # Generar lista de fechas con intervalos de 10 minutos
    current_date = date_start
    dates = []
    while current_date <= date_end:
        dates.append(current_date)
        current_date += timedelta(minutes=10)

    return dates
    

def fetch_data_for_session_driver(table, session_key, driver_number):

    url = f"{API_BASE_URL}{table}?session_key={session_key}&driver_number={driver_number}"
    print(f"[INFO] GET {url}")
    resp = requests.get(url)
    if resp.status_code == 200:
        return resp.json()
    elif resp.status_code == 429:
        raise requests.HTTPError(response=resp)  # Lanza error 429 para que lo maneje el decorador
    else:
        print(f"[ERROR] Error {resp.status_code} en la tabla {table}, "
              f"session_key={session_key}, driver_number={driver_number}")
        return None

@retry_on_failure(max_retries=5, retry_delay=2)
def fetch_data_for_session_driver_date(table, session_key, driver_number, datetime):
    datetime2 = datetime + timedelta(minutes=10)
    date = datetime.date()
    time = datetime.time()
    date2 = datetime2.date()
    time2 = datetime2.time()

    url = f"{API_BASE_URL}{table}?session_key={session_key}&driver_number={driver_number}&date>={date}T{time}+00:00&date<{date2}T{time2}+00:00"
    print(f"[INFO] GET {url}")
    
    resp = requests.get(url)
    if resp.status_code == 200:
        return resp.json()
    elif resp.status_code == 429:
        # Lanza el error para que el decorador lo maneje
        raise requests.HTTPError(response=resp)
    else:
        print(f"[ERROR] Error {resp.status_code} en la tabla {table}, "
              f"session_key={session_key}, driver_number={driver_number}")
        return None

def add_to_pg(conn, table, records, meeting_key, session_key, driver_number, date):
    if records:
        insert_data(conn, table, records)
        # Registrar el log de ingesta
        log_ingestion(conn, meeting_key, session_key, driver_number, table, date)

def is_already_ingested(conn, meeting_key, session_key, table_name, driver_number):
    
    sql = f"""
    SELECT 1 FROM formula1.{table_name}
    WHERE meeting_key = %s AND session_key = %s AND driver_number = %s
    LIMIT 1;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (meeting_key, session_key, driver_number))
        return cur.fetchone() is not None

def log_ingestion(conn, meeting_key, session_key, driver_number, table_name, date):
    try:
        sql = """
        INSERT INTO logs.large_ingestion_logs (meeting_key, session_key, driver_number, table_name, date, last_ingested)
        VALUES (%s, %s, %s, %s, %s, NOW())
        ON CONFLICT (meeting_key, session_key, table_name)
        DO UPDATE SET last_ingested = EXCLUDED.last_ingested;
        """
        with conn.cursor() as cur:
            cur.execute(sql, (meeting_key, session_key, driver_number, table_name, date))
        conn.commit()
        print(f"[INFO] Log registrado para {table_name}, session_key={session_key}.")
        print(f"[INFO] driver_number = {driver_number}.")
    except Exception as e:
        conn.rollback()
        print(f"[ERROR] Al registrar log de ingesta: {e}")

def main():
    # 1. Conectarse a la base de datos
    conn = connect_to_db()
    if not conn:
        return 

    try:

        race_meeting_sessions = get_race_sessions_info(conn)

        for session_key, meeting_key, circuit, year in race_meeting_sessions:

            print(f"[INFO] -------------------------------------------------------------------------------")
            print(f"[INFO] Circuit: {circuit}")
            print(f"[INFO] Year: {year}")
            print(f"[INFO] -------------------------------------------------------------------------------")

            driver_numbers = get_drivers_for_session(conn, session_key)

            for driver_number in driver_numbers:
                # car_data = fetch_data_for_session_driver(table="car_data", session_key=session_key, driver_number=driver_number)
                # intervals_data = fetch_data_for_session_driver(table="intervals", session_key=session_key, driver_number=driver_number)
                # location_data = fetch_data_for_session_driver(table="location", session_key=session_key, driver_number=driver_number)
                
                tables_to_intake = []

                # if is_already_ingested(conn, meeting_key, session_key, "car_data", driver_number):
                #     print(f"[INFO] Datos ya procesados para car_data, session_key={session_key}. Saltando...")
                # else:
                #     tables_to_intake.append("car_data")
                    
                if is_already_ingested(conn, meeting_key, session_key, "intervals", driver_number):
                    print(f"[INFO] Datos ya procesados para intervals_data, session_key={session_key}. Saltando...")
                else:
                    tables_to_intake.append("intervals")

                # if is_already_ingested(conn, meeting_key, session_key, "location", driver_number):
                #     print(f"[INFO] Datos ya procesados para location, session_key={session_key}. Saltando...")
                # else:
                #     tables_to_intake.append("location")

                for table in tables_to_intake:
                    print(f"[INFO] Insertando datos de {table} para session_key={session_key}")

                    if table == "intervals":
                        records = fetch_data_for_session_driver(table, session_key, driver_number)
                        add_to_pg(conn, table, records, meeting_key, session_key, driver_number, date=None)

                    # else:
                    #     dates = get_dates_for_session(conn, session_key)
                    #     for date in dates:
                    #         records = fetch_data_for_session_driver_date(table, session_key, driver_number, date)
                    #         add_to_pg(conn, table, records, meeting_key, session_key, driver_number, date)


    except Exception as e:
        print(f"[ERROR] {e}")
    finally:
        conn.close()
        print("[INFO] Conexión a PostgreSQL cerrada.")


if __name__ == "__main__":
    main()