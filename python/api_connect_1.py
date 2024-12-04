## Archivo base para hacer una solicitud GET a la API de OpenF1

import requests
import pandas as pd

# Definir la URL de la API para obtener datos de las carreras de este año
url = "https://api.openf1.org/v1/sessions?session_name=Race&year=2024"

# Realizar la solicitud GET a la API
response = requests.get(url)

# Verificar si la solicitud fue exitosa
if response.status_code == 200:
    # Convertir la respuesta JSON en un DataFrame de pandas
    data = response.json()
    df = pd.DataFrame(data)
    
    # Guardar el DataFrame en un archivo CSV
    df.to_csv("csv/races2024.csv", index=False)
    print("Datos de los pilotos guardados en 'races2024.csv'")
else:
    print(f"Error al acceder a la API: {response.status_code}")
