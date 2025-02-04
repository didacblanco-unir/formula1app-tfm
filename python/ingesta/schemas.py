import requests
import json
import re
from datetime import datetime
from dateutil.parser import isoparse

TABLES = [
    "car_data?driver_number=55&session_key=9159&speed>=315&drs=12",
    "drivers?driver_number=1&session_key=9158",
    "intervals?session_key=9165&interval<0.005",
    "laps?session_key=9161&driver_number=63&lap_number=8",
    "location?session_key=9161&driver_number=81&date>2023-09-16T13:03:35.200&date=2023-09-16T13:03:35.292000+00:00",
    "meetings?year=2023&country_name=Singapore",
    "pit?session_key=9158&pit_duration<26",
    "position?meeting_key=1217&driver_number=40&position=3",
    "race_control?flag=BLACK AND WHITE&driver_number=1&date>=2023-01-01&date<2023-09-01",
    "sessions?country_name=Belgium&session_name=Sprint&year=2023",
    "stints?session_key=9165&tyre_age_at_start>=3&lap_end=20",
    "team_radio?session_key=9158&driver_number=11&date=2023-09-15T09:40:43.005000",
    "weather?meeting_key=1208&wind_direction>=130&track_temperature>=52"
]

BASE_URL = "https://api.openf1.org/v1" 

def infer_data_type(value):

    if value is None:
        return "TEXT"

    val_str = str(value).strip()

    if val_str == "":
        return "TEXT"

    try:
        int_val = int(val_str)
        return "INT"
    except ValueError:
        pass

    try:
        float_val = float(val_str)
        return "FLOAT"
    except ValueError:
        pass

    date_formats = ["%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"]
    for f in date_formats:
        try:
            datetime.strptime(val_str, f)
            return "DATE"
        except ValueError:
            pass

    return "TEXT"

def unify_types(type_list):
    if not type_list:
        return "TEXT"

    if all(t == "INT" for t in type_list):
        return "INT"
    if "FLOAT" in type_list or ("INT" in type_list and "FLOAT" in type_list):
        return "FLOAT"
    if "DATE" in type_list and len(set(type_list)) == 1:
        return "DATE"

    return "TEXT"

def infer_schema_from_sample(table_name, sample_data):

    if not sample_data:
        print(f"La tabla {table_name} está vacía o no tiene datos.")
        return {}

    field_types = {}

    for record in sample_data:
        for field, value in record.items():
            t = infer_data_type(value)
            if field not in field_types:
                field_types[field] = []
            field_types[field].append(t)

    schema = {}
    for field, types in field_types.items():
        unified_type = unify_types(types)
        schema[field] = unified_type

    return schema

def generate_sql_create(table_name, schema_dict):
    sql_fields = []
    for field, data_type in schema_dict.items():
        if data_type == "INT":
            sql_type = "INTEGER"
        elif data_type == "FLOAT":
            sql_type = "REAL"
        elif data_type == "DATE":
            sql_type = "DATE"
        else:
            sql_type = "TEXT"

        sql_fields.append(f"  `{field}` {sql_type}")

    fields_str = ",\n".join(sql_fields)
    create_stmt = f"CREATE TABLE {table_name} (\n{fields_str}\n);"
    return create_stmt

def main():
    for table_name in TABLES:
        url = f"{BASE_URL}/{table_name}"
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"Error al conectarse con la tabla '{table_name}': {e}")
            continue
        
        table_name = table_name.split('?')[0]
        data = response.json()

        if not isinstance(data, list):
            print(f"El endpoint de '{table_name}' no devolvió una lista. Revisa el formato.")
            continue

        sample_data = data[:50]

        schema_dict = infer_schema_from_sample(table_name, sample_data)

        if schema_dict:
            create_table_sql = generate_sql_create(table_name, schema_dict)
            print(create_table_sql)
            with open("schema_output.txt", "a", encoding="utf-8") as out_file:
                out_file.write(create_table_sql)
                out_file.write("\n" + ("-" * 80) + "\n")
        print("-" * 80)

if __name__ == "__main__":
    main()