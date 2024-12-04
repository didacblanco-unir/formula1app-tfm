CREATE SCHEMA IF NOT EXISTS formula1;

CREATE TABLE IF NOT EXISTS formula1.drivers (
    driver_id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    date_of_birth DATE,
    nationality VARCHAR(50),
    car_number INT UNIQUE
);

CREATE TABLE IF NOT EXISTS formula1.teams (
    team_id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    country VARCHAR(50),
    foundation_year INT
);

CREATE TABLE IF NOT EXISTS formula1.seasons (
    season_id SERIAL PRIMARY KEY,
    year INT NOT NULL UNIQUE,
    champion_driver_id INT REFERENCES formula1.drivers(driver_id),
    champion_team_id INT REFERENCES formula1.teams(team_id)
);

CREATE TABLE IF NOT EXISTS formula1.circuits (
    circuit_id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    location VARCHAR(100),
    length_km DECIMAL(5, 2)
);

CREATE TABLE IF NOT EXISTS formula1.races (
    race_id SERIAL PRIMARY KEY,
    season_id INT REFERENCES formula1.seasons(season_id),
    name VARCHAR(100) NOT NULL,
    date DATE NOT NULL,
    circuit_id INT REFERENCES formula1.circuits(circuit_id)
);

CREATE TABLE IF NOT EXISTS formula1.results (
    result_id SERIAL PRIMARY KEY,
    race_id INT REFERENCES formula1.races(race_id),
    driver_id INT REFERENCES formula1.drivers(driver_id),
    team_id INT REFERENCES formula1.teams(team_id),
    position INT,
    points DECIMAL(5, 2),
    laps_completed INT,
    total_time INTERVAL
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

CREATE TABLE IF NOT EXISTS formula1.telemetry (
    telemetry_id SERIAL PRIMARY KEY,
    lap_id INT REFERENCES formula1.lap_times(lap_id),
    timestamp TIMESTAMPTZ NOT NULL,
    speed_kph DECIMAL(5, 2),
    rpm INT,
    throttle DECIMAL(5, 2),
    brake DECIMAL(5, 2),
    gear INT
);

-- Conversion a hypertable (tablas particionadas y optimizadas para valores temporales)


SELECT create_hypertable('formula1.lap_times', 'lap_number');

SELECT create_hypertable('formula1.telemetry', 'timestamp');
