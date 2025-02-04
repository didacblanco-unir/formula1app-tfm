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

# Función para ejecutar la query que le pasamos
def run_query(conn, query):
    if not conn:
        print("No hay conexión a la base de datos. Saliendo...")
        return

    try:
        with conn.cursor() as cursor:
            cursor.execute(query)
            conn.commit()
        print("Query ejecutada correctamente.")
    except Exception as e:
        print(f"Error al ejecutar la query: {e}")
    finally:
        conn.close()
        print("Conexión cerrada.")

def main():
    path = "."
    # Leemos el contenido del archivo query.txt
    query_content = open(path + "query.txt", "r").read()

    # Obtenemos la conexión
    conn = connect_to_db()
    # Ejecutamos la query
    run_query(conn, query_content)

if __name__ == "__main__":
    main()
