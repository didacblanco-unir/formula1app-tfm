# producers.py (parte: productor para position)
from kafka import KafkaProducer
import psycopg2
import json
import time
from datetime import timedelta

def produce_position(simulated_time):

    producer = KafkaProducer(
        bootstrap_servers='kafka:9092',
        value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8')
    )
    conn = psycopg2.connect(
        host="timescaledb_postgres",
        port=5432,
        user="admin",
        password="admin_password",
        dbname="mydatabase"
    )
    cur = conn.cursor()
    start_time = simulated_time - timedelta(seconds=30)
    query = f"""
        SELECT driver_number, session_key, meeting_key, date, position
        FROM formula1.position
        WHERE date > '{start_time.isoformat()}' AND date <= '{simulated_time.isoformat()}'
        ORDER BY date ASC;
    """
    cur.execute(query)
    rows = cur.fetchall()
    columns = [desc[0] for desc in cur.description]
    for row in rows:
        record = dict(zip(columns, row))
        producer.send("position_topic", record)
        print("Registro de position enviado:", record)
    cur.close()
    conn.close()

def produce_car_data(simulated_time):

    producer = KafkaProducer(
        bootstrap_servers='kafka:9092',
        value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8')
    )
    
    conn = psycopg2.connect(
        host="timescaledb_postgres",
        port=5432,
        user="admin",
        password="admin_password",
        dbname="mydatabase"
    )
    cur = conn.cursor()
    
    start_time = simulated_time - timedelta(seconds=30)
    
    query = f"""
        SELECT driver_number, session_key, rpm, speed, n_gear, throttle, brake, drs, date, meeting_key
        FROM formula1.car_data
        WHERE date >= '{start_time.isoformat()}' AND date <= '{simulated_time.isoformat()}'
        ORDER BY date ASC;
    """
    cur.execute(query)
    rows = cur.fetchall()
    
    columns = [desc[0] for desc in cur.description]
    
    for row in rows:
        record = dict(zip(columns, row))
        producer.send("car_data_topic", record)
        print("Registro de car_data enviado:", record)
        # time.sleep(0.25)  
    
    cur.close()
    conn.close()
