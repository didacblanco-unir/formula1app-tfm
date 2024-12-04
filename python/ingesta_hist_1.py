## Archivo base para hacer insert de un csv a la PostgreSQL

import psycopg2
import pandas as pd

# Conectar a PostgreSQL
conn = psycopg2.connect(
    dbname="mydatabase",
    user="admin",
    password="admin_password",
    host="localhost"
)

# Leer un CSV y cargarlo en una tabla
df = pd.read_csv("drivers.csv")
cursor = conn.cursor()

for _, row in df.iterrows():
    cursor.execute(
        "INSERT INTO drivers (name, country, team) VALUES (%s, %s, %s)",
        (row['name'], row['country'], row['team'])
    )

conn.commit()
conn.close()
