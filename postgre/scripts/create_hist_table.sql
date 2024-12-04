CREATE SCHEMA IF NOT EXISTS formula1;

CREATE TABLE IF NOT EXISTS formula1.drivers (
    driver_id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    country VARCHAR(50),
    team VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS formula1.races (
    race_id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    circuit VARCHAR(100),
    date DATE
);

CREATE TABLE IF NOT EXISTS formula1.lap_times (
    lap_id SERIAL PRIMARY KEY,
    race_id INT REFERENCES formula1.races(race_id),
    driver_id INT REFERENCES formula1.drivers(driver_id),
    lap_number INT,
    lap_time INTERVAL,
    sector1_time INTERVAL,
    sector2_time INTERVAL,
    sector3_time INTERVAL
);

CREATE TABLE IF NOT EXISTS formula1.results (
    result_id SERIAL PRIMARY KEY,
    race_id INT REFERENCES formula1.races(race_id),
    driver_id INT REFERENCES formula1.drivers(driver_id),
    position INT,
    points INT,
    fastest_lap BOOLEAN
);

