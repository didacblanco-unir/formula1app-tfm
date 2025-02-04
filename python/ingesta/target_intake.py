## Ingesta de datos específicos
# Script copia de main_intake.py para descargar session_key indicados y tabla indicada

import requests
import psycopg2
import time
from psycopg2.extras import execute_values
import json
from functools import wraps

# -----------------------------------------------------------------------------
# CONFIGURACIÓN
# -----------------------------------------------------------------------------
DB_CONFIG = {
    "dbname": "mydatabase",
    "user": "admin",
    "password": "admin_password",
    "host": "localhost",
    "port": 5432
}

API_BASE_URL = "https://api.openf1.org/v1/" 

TABLES_WITH_SESSION_KEY = [
    "laps"
]

SCHEMA_PREFIX = "formula1."


# -----------------------------------------------------------------------------
# FUNCIONES DE CONEXIÓN Y DESCARGA
# -----------------------------------------------------------------------------
def connect_to_db():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        print("[INFO] Conectado a PostgreSQL.")
        return conn
    except Exception as e:
        print(f"[ERROR] No se pudo conectar a PostgreSQL: {e}")
        return None

def retry_on_failure(max_retries=20, retry_delay=2):

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            retries = 0
            while retries < max_retries:
                try:
                    result = func(*args, **kwargs)
                    retries = 0 
                    return result
                except requests.RequestException as e:
                    print(f"[ERROR] Error de conexión: {e}. Retrying...")
                except requests.HTTPError as e:
                    response = e.response
                    if response.status_code == 429:
                        retry_after = int(response.headers.get("Retry-After", retry_delay))
                        print(f"[WARN] Límite alcanzado (429). Esperando {retry_after} segundos...")
                        time.sleep(retry_after)
                    elif 500 <= response.status_code < 600:
                        print(f"[ERROR] Error del servidor {response.status_code}. Retrying...")
                    else:
                        print(f"[ERROR] HTTP {response.status_code}: {response.text}")
                        break 
                retries += 1
                print(f"[WARN] Retries: {retries}, Backing off...")
                print(f"[WARN] Retry delay: {retry_delay * retries} segundos")
                time.sleep(retry_delay * retries)
            print("[ERROR] Exceso de reintentos. Abortando...")
            return None
        return wrapper
    return decorator



@retry_on_failure(max_retries=5, retry_delay=2)
def fetch_data_for_session_key(table, session_key):
    url = f"{API_BASE_URL}{table}?session_key={session_key}"
    print(f"[INFO] GET {url}")
    resp = requests.get(url)
    if resp.status_code == 200:
        return resp.json()
    elif resp.status_code == 429:
        raise requests.HTTPError(response=resp)
    else:
        print(f"[ERROR] Error {resp.status_code} en la tabla {table}, session_key={session_key}")
        return None


def fetch_data_for_table(table, session_keys):
    all_records = []
    for sk in session_keys:
        records = fetch_data_for_session_key(table, sk)
        if records:
            all_records.extend(records)
    return all_records


# -----------------------------------------------------------------------------
# FUNCIONES PARA INSERTAR EN POSTGRES
# -----------------------------------------------------------------------------
def insert_data(conn, table_name, records):
    if not records:
        print(f"[INFO] No hay registros que insertar en {table_name}.")
        return
    
    columns = list(records[0].keys()) 
    placeholders = ", ".join(["%s"] * len(columns))
    col_names = ", ".join(columns)
    
    sql = f"""
        INSERT INTO {SCHEMA_PREFIX}{table_name} ({col_names})
        VALUES %s
        ON CONFLICT DO NOTHING
        -- Opcional: Ajusta la parte de ON CONFLICT si usas llaves primarias
        -- y quieres actualizar en lugar de ignorar.
    """

    values = []
    for rec in records:
        row = [rec.get(col) for col in columns]
        values.append(row)

    try:
        with conn.cursor() as cur:
            execute_values(cur, sql, values)
        conn.commit()

        print(f"[INFO] Insertados \033[32m{len(records)} registros\033[0m en {table_name}.")
    except Exception as e:
        conn.rollback()
        print(f"[ERROR] Al insertar en {table_name}: {e}")


# -----------------------------------------------------------------------------
# MAIN
# -----------------------------------------------------------------------------
def main():
    # 1. Conectarse a la base de datos
    conn = connect_to_db()
    if not conn:
        return 

    try:

        # 5. Obtener todas las sessions de esos meeting_keys
        sessions_data = [
            "9222",
            "7763",
            "7764",
            "7765",
            "7766",
            "7767",
            "7768",
            "7953",
            "7772",
            "7773",
            "7774",
            "7775",
            "7779",
            "7780",
            "7781",
            "7782",
            "7783",
            "7787",
            "9063",
            "9064",
            "9278",
            "9069",
            "9070",
            "9071",
            "9072",
            "9073",
            "9074",
            "9078",
            "9087",
            "9088",
            "9089",
            "9090",
            "9094",
            "9103",
            "9214",
            "9215",
            "9298",
            "9220",
            "9221",
            "9206",
            "9207",
            "9212",
            "9204",
            "9489",
            "9549",
            "9616",
            "9635"
        ]

        records = fetch_data_for_table("pit", sessions_data)
        insert_data(conn, "pit", records)


    except Exception as e:
        print(f"[ERROR] {e}")
    finally:
        conn.close()
        print("[INFO] Conexión a PostgreSQL cerrada.")


if __name__ == "__main__":
    main()
