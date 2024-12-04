# Primera prueba de conexión a la API de OpenF1 y a la base de datos de PostgreSQL

import requests
import pandas as pd
import psycopg2

# Configuración de conexión a la base de datos PostgreSQL
DB_CONFIG = {
    "dbname": "mydatabase",
    "user": "admin",
    "password": "admin_password",
    "host": "localhost",
    "port": 5432
}

# Función para conectar a la base de datos
def connect_to_db():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        print("Conexión a PostgreSQL exitosa.")
        return conn
    except Exception as e:
        print(f"Error al conectar a PostgreSQL: {e}")
        return None

# Función para crear la tabla drivers si no existe
def create_drivers_table(conn):
    query = """
    CREATE TABLE IF NOT EXISTS drivers (
        driver_id SERIAL PRIMARY KEY,
        name VARCHAR(100),
        country VARCHAR(50),
        team VARCHAR(100)
    );
    """
    try:
        with conn.cursor() as cursor:
            cursor.execute(query)
            conn.commit()
        print("Tabla 'drivers' creada o ya existe.")
    except Exception as e:
        print(f"Error al crear la tabla 'drivers': {e}")

# Función para insertar datos en la tabla
def insert_drivers(conn, drivers):
    query = """
    INSERT INTO drivers (name, country, team)
    VALUES (%s, %s, %s)
    ON CONFLICT DO NOTHING;
    """
    try:
        with conn.cursor() as cursor:
            cursor.executemany(query, drivers)
            conn.commit()
        print("Datos de los pilotos insertados correctamente.")
    except Exception as e:
        print(f"Error al insertar datos: {e}")

# Función para descargar datos desde la API y guardarlos en PostgreSQL
def fetch_and_store_drivers():
    # URL de la API
    url = "https://api.openf1.org/v1/drivers?session_key=9158"
    
    try:
        # Solicitar datos desde la API
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            # Convertir datos a una lista de tuplas para PostgreSQL
            drivers = [(item["full_name"], item["country_code"], item["team_name"]) for item in data]

            # Conectar a la base de datos
            conn = connect_to_db()
            if conn:
                create_drivers_table(conn)
                insert_drivers(conn, drivers)
                conn.close()
        else:
            print(f"Error al acceder a la API: {response.status_code}")
    except Exception as e:
        print(f"Error al obtener datos de la API: {e}")

# Ejecutar el proceso
fetch_and_store_drivers()
