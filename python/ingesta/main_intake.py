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

API_BASE_URL = "https://api.openf1.org/v1/"  # Ajusta la URL base de tu API

# Tablas que tienen la columna session_key y que queremos descargar
TABLES_WITH_SESSION_KEY = [
    #"car_data",
    "drivers",
    #"intervals",
    "laps",
    #"location",
    "pit",
    "position",
    "race_control",
    #"sessions",
    "stints",
    #"weather"
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

def fetch_meetings_year_gt_2020():
    url = f"{API_BASE_URL}meetings?year>2020"
    print(f"[INFO] GET {url}")
    resp = requests.get(url)
    if resp.status_code == 200:
        return resp.json() 
    else:
        print(f"[ERROR] Error {resp.status_code} al obtener meetings.")
        return []

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
                    # Manejo de errores de conexión
                    print(f"[ERROR] Error de conexión: {e}. Retrying...")
                except requests.HTTPError as e:
                    # Manejo de errores HTTP específicos
                    response = e.response
                    if response.status_code == 429:  # Límite alcanzado
                        retry_after = int(response.headers.get("Retry-After", retry_delay))
                        print(f"[WARN] Límite alcanzado (429). Esperando {retry_after} segundos...")
                        time.sleep(retry_after)
                    elif 500 <= response.status_code < 600:  # Errores del servidor
                        print(f"[ERROR] Error del servidor {response.status_code}. Retrying...")
                    else:
                        print(f"[ERROR] HTTP {response.status_code}: {response.text}")
                        break  # No reintentar para otros errores (404, etc.)
                retries += 1
                print(f"[WARN] Retries: {retries}, Backing off...")
                print(f"[WARN] Retry delay: {retry_delay * retries} segundos")
                time.sleep(retry_delay * retries)  # Backoff exponencial
            print("[ERROR] Exceso de reintentos. Abortando...")
            return None
        return wrapper
    return decorator

@retry_on_failure(max_retries=5, retry_delay=2)
def fetch_sessions_for_meeting(conn, meeting_key):
    # 1. Buscar en PostgreSQL
    try:
        sql = """
        SELECT session_key, session_name, date_start, date_end, meeting_key
        FROM formula1.sessions
        WHERE meeting_key = %s;
        """
        with conn.cursor() as cur:
            cur.execute(sql, (meeting_key,))
            rows = cur.fetchall()
            if rows:
                print(f"[INFO] Sesiones obtenidas de PostgreSQL para meeting_key={meeting_key}.")
                return [
                    {
                        "session_key": row[0],
                        "session_name": row[1],
                        "date_start": row[2],
                        "date_end": row[3],
                        "meeting_key": row[4]
                    }
                    for row in rows
                ]
    except Exception as e:
        print(f"[ERROR] Error al buscar sesiones en PostgreSQL: {e}")

    # 2. Si no están en PostgreSQL, buscar en la API
    url = f"{API_BASE_URL}sessions?meeting_key={meeting_key}"
    print(f"[INFO] GET {url}")
    resp = requests.get(url)
    if resp.status_code == 200:
        data = resp.json()

        # 3. Guardar las sesiones en PostgreSQL
        try:
            sql_insert = """
            INSERT INTO formula1.sessions (session_key, session_name, date_start, date_end, meeting_key)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (session_key) DO NOTHING;
            """
            with conn.cursor() as cur:
                for session in data:
                    cur.execute(sql_insert, (
                        session["session_key"],
                        session.get("session_name"),
                        session.get("date_start"),
                        session.get("date_end"),
                        session["meeting_key"]
                    ))
            conn.commit()
            print(f"[INFO] Sesiones insertadas en PostgreSQL para meeting_key={meeting_key}.")
        except Exception as e:
            conn.rollback()
            print(f"[ERROR] Error al insertar sesiones en PostgreSQL: {e}")

        return data

    elif resp.status_code == 429:
        raise requests.HTTPError(response=resp)  # Lanza error 429 para que lo maneje el decorador
    else:
        print(f"[ERROR] Error {resp.status_code} al obtener sessions para meeting_key={meeting_key}")
        return None



def fetch_sessions_for_meetings(conn, meeting_keys):
    all_sessions = []
    for mk in meeting_keys:
        sessions = fetch_sessions_for_meeting(conn, mk)
        if sessions:
            all_sessions.extend(sessions)
    return all_sessions


@retry_on_failure(max_retries=5, retry_delay=2)
def fetch_data_for_session_key(table, session_key):
    url = f"{API_BASE_URL}{table}?session_key={session_key}"
    print(f"[INFO] GET {url}")
    resp = requests.get(url)
    if resp.status_code == 200:
        return resp.json()
    elif resp.status_code == 429:
        raise requests.HTTPError(response=resp)  # Lanza error 429 para que lo maneje el decorador
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


def log_ingestion(conn, meeting_key, session_key, table_name):
    try:
        sql = """
        INSERT INTO logs.ingestion_logs (meeting_key, session_key, table_name, last_ingested)
        VALUES (%s, %s, %s, NOW())
        ON CONFLICT (meeting_key, session_key, table_name)
        DO UPDATE SET last_ingested = EXCLUDED.last_ingested;
        """
        with conn.cursor() as cur:
            cur.execute(sql, (meeting_key, session_key, table_name))
        conn.commit()
        print(f"[INFO] Log registrado para {table_name}, session_key={session_key}.")
    except Exception as e:
        conn.rollback()
        print(f"[ERROR] Al registrar log de ingesta: {e}")

def is_already_ingested(conn, meeting_key, session_key, table_name):
    try:
        sql = """
        SELECT 1 FROM logs.ingestion_logs
        WHERE meeting_key = %s AND session_key = %s AND table_name = %s
        LIMIT 1;
        """
        with conn.cursor() as cur:
            cur.execute(sql, (meeting_key, session_key, table_name))
            return cur.fetchone() is not None
    except Exception as e:
        print(f"[ERROR] Al verificar log de ingesta: {e}")
        return False

# -----------------------------------------------------------------------------
# MAIN
# -----------------------------------------------------------------------------
def main():
    # 1. Conectarse a la base de datos
    conn = connect_to_db()
    if not conn:
        return

    try:
        meetings_data = fetch_meetings_year_gt_2020() # solo hay datos desde 2023
        if not meetings_data:
            print("[INFO] No hay meetings con year > 2020.")
            return
        
        # 3. Extraer solo los meeting_keys
        meeting_keys = [m["meeting_key"] for m in meetings_data if "meeting_key" in m]
        
        # 4. Insertar los meetings en PostgreSQL (si quieres guardarlos también)
        insert_data(conn, "meetings", meetings_data)

        # 5. Obtener todas las sessions de esos meeting_keys
        sessions_data = fetch_sessions_for_meetings(conn, meeting_keys)
        if not sessions_data:
            print("[INFO] No hay session_keys para esos meeting_keys.")
            return

        # 6. Insertar las sessions en la tabla sessions
        insert_data(conn, "sessions", sessions_data)

        # 7. Obtenemos la lista de session_keys y las listas de (meeting_key, session_key)
        # session_keys = [s["session_key"] for s in sessions_data if "session_key" in s]
        meeting_sessions = [(s["meeting_key"], s["session_key"]) for s in sessions_data if "meeting_key" in s and "session_key" in s]
        
        # 8. Para cada tabla, llamamos a la API con los session_keys y luego insertamos
        for table in TABLES_WITH_SESSION_KEY:
            
            for mk, sk in meeting_sessions:
                print(f"[INFO] Procesando {table}")
                print(f"[INFO] meeting_key={mk}, session_key={sk}")
                if is_already_ingested(conn, meeting_key=mk, session_key=sk, table_name=table):
                    print(f"[INFO] Datos ya procesados para {table}, session_key={sk}. Saltando...")
                    continue
                
                records = fetch_data_for_table(table, [sk])
                if records:
                    insert_data(conn, table, records)
                    # Registrar el log de ingesta
                    log_ingestion(conn, meeting_key=mk, session_key=sk, table_name=table)


    except Exception as e:
        print(f"[ERROR] {e}")
    finally:
        conn.close()
        print("[INFO] Conexión a PostgreSQL cerrada.")


if __name__ == "__main__":
    main()
