-- comando para habilitar extensión
--CREATE EXTENSION IF NOT EXISTS timescaledb;

-------------------------------------------------------------------------------
-- 1) meetings 
-------------------------------------------------------------------------------
DROP TABLE IF EXISTS formula1.meetings CASCADE;
CREATE TABLE formula1.meetings (
  meeting_key           INTEGER        PRIMARY KEY,
  meeting_name          TEXT,
  meeting_official_name TEXT,
  location              TEXT,
  country_key           INTEGER,
  country_code          TEXT,
  country_name          TEXT,
  circuit_key           INTEGER,
  circuit_short_name    TEXT,
  date_start            TEXT,
  gmt_offset            TEXT,
  year                  INTEGER
);

-------------------------------------------------------------------------------
-- 2) sessions 
-------------------------------------------------------------------------------
DROP TABLE IF EXISTS formula1.sessions CASCADE;
CREATE TABLE formula1.sessions (
  session_key        INTEGER PRIMARY KEY,
  location           TEXT,
  country_key        INTEGER,
  country_code       TEXT,
  country_name       TEXT,
  circuit_key        INTEGER,
  circuit_short_name TEXT,
  session_type       TEXT,
  session_name       TEXT,
  date_start         TEXT,
  date_end           TEXT,
  gmt_offset         TEXT,
  meeting_key        INTEGER NOT NULL,
  year               INTEGER,
  FOREIGN KEY (meeting_key) REFERENCES formula1.meetings(meeting_key)
);

-------------------------------------------------------------------------------
-- 3) drivers 
-------------------------------------------------------------------------------
DROP TABLE IF EXISTS formula1.drivers CASCADE;
CREATE TABLE formula1.drivers (
  driver_number   INTEGER,
  session_key     INTEGER,
  broadcast_name  TEXT,
  full_name       TEXT,
  name_acronym    TEXT,
  team_name       TEXT,
  team_colour     TEXT,
  first_name      TEXT,
  last_name       TEXT,
  headshot_url    TEXT,
  country_code    TEXT,
  meeting_key     INTEGER,
  PRIMARY KEY (driver_number, session_key), 
  FOREIGN KEY (session_key) REFERENCES formula1.sessions(session_key),
  FOREIGN KEY (meeting_key) REFERENCES formula1.meetings(meeting_key)
);

-------------------------------------------------------------------------------
-- 4) car_data
-------------------------------------------------------------------------------
DROP TABLE IF EXISTS formula1.car_data CASCADE;
CREATE TABLE formula1.car_data (
  driver_number INTEGER,
  session_key   INTEGER,
  rpm           INTEGER,
  speed         INTEGER,
  n_gear        INTEGER,
  throttle      INTEGER,
  brake         INTEGER,
  drs           INTEGER,
  date          TEXT,
  meeting_key   INTEGER,
  PRIMARY KEY (driver_number, session_key, date),
  FOREIGN KEY (driver_number, session_key) REFERENCES formula1.drivers(driver_number, session_key),
  FOREIGN KEY (meeting_key) REFERENCES formula1.meetings(meeting_key)
);
-- Convertimos la tabla en una hypertable
SELECT create_hypertable('formula1.car_data', by_range('date'));
-------------------------------------------------------------------------------
-- 5) intervals
-------------------------------------------------------------------------------
DROP TABLE IF EXISTS formula1.intervals CASCADE;
CREATE TABLE formula1.intervals (
  driver_number  INTEGER,
  session_key    INTEGER,
  gap_to_leader  TEXT,
  interval       TEXT,
  date           TEXT,
  meeting_key    INTEGER,
  PRIMARY KEY (driver_number, session_key, date), 
  FOREIGN KEY (driver_number, session_key) REFERENCES formula1.drivers(driver_number, session_key),
  FOREIGN KEY (meeting_key) REFERENCES formula1.meetings(meeting_key)
);

-- Convertimos la tabla en una hypertable
SELECT create_hypertable('formula1.intervals', by_range('date'));

-------------------------------------------------------------------------------
-- 6) laps 
-------------------------------------------------------------------------------
DROP TABLE IF EXISTS formula1.laps CASCADE;
CREATE TABLE formula1.laps (
  driver_number     INTEGER,
  session_key       INTEGER,
  meeting_key       INTEGER,
  i1_speed          INTEGER,
  i2_speed          INTEGER,
  st_speed          INTEGER,
  date_start        TEXT,
  lap_duration      REAL,
  is_pit_out_lap    TEXT,
  duration_sector_1 REAL,
  duration_sector_2 REAL,
  duration_sector_3 REAL,
  segments_sector_1 TEXT,
  segments_sector_2 TEXT,
  segments_sector_3 TEXT,
  lap_number        INTEGER,
  PRIMARY KEY (driver_number, session_key, lap_number),
  FOREIGN KEY (driver_number, session_key) REFERENCES formula1.drivers(driver_number, session_key),
  FOREIGN KEY (meeting_key) REFERENCES formula1.meetings(meeting_key)
);

-------------------------------------------------------------------------------
-- 7) location (convertida a hypertable)
-------------------------------------------------------------------------------
DROP TABLE IF EXISTS formula1.location CASCADE;
CREATE TABLE formula1.location (
  driver_number INTEGER,
  session_key   INTEGER,
  meeting_key   INTEGER,
  date          TIMESTAMP WITH TIME ZONE NOT NULL, 
  x             INTEGER,
  y             INTEGER,
  z             INTEGER,
  PRIMARY KEY (driver_number, session_key, date),
  FOREIGN KEY (driver_number, session_key) REFERENCES formula1.drivers(driver_number, session_key),
  FOREIGN KEY (meeting_key) REFERENCES formula1.meetings(meeting_key)
);

-- Convertimos la tabla en una hypertable
SELECT create_hypertable('formula1.location', by_range('date'));

-------------------------------------------------------------------------------
-- 8) pit (convertida a hypertable)
-------------------------------------------------------------------------------
DROP TABLE IF EXISTS formula1.pit CASCADE;
CREATE TABLE formula1.pit (
  driver_number  INTEGER,
  session_key    INTEGER,
  meeting_key    INTEGER,
  date           TIMESTAMP WITH TIME ZONE NOT NULL,
  pit_duration   REAL,
  lap_number     INTEGER,
  PRIMARY KEY (driver_number, session_key, date),
  FOREIGN KEY (driver_number, session_key) REFERENCES formula1.drivers(driver_number, session_key),
  FOREIGN KEY (meeting_key) REFERENCES formula1.meetings(meeting_key)
);

-- Convertimos la tabla en una hypertable
SELECT create_hypertable('formula1.pit', by_range('date'));

-------------------------------------------------------------------------------
-- 9) position (convertida a hypertable)
-------------------------------------------------------------------------------
DROP TABLE IF EXISTS formula1.position CASCADE;
CREATE TABLE formula1.position (
  driver_number  INTEGER,
  session_key    INTEGER,
  meeting_key    INTEGER,
  date           TIMESTAMP WITH TIME ZONE NOT NULL,
  position       INTEGER,
  PRIMARY KEY (driver_number, session_key, date),
  FOREIGN KEY (driver_number, session_key) REFERENCES formula1.drivers(driver_number, session_key),
  FOREIGN KEY (meeting_key) REFERENCES formula1.meetings(meeting_key)
);

-- Convertimos la tabla en una hypertable
SELECT create_hypertable('formula1.position', by_range('date'));

-------------------------------------------------------------------------------
-- 10) race_control (convertida a hypertable)
-------------------------------------------------------------------------------
DROP TABLE IF EXISTS formula1.race_control CASCADE;
CREATE TABLE formula1.race_control (
  driver_number  INTEGER,
  session_key    INTEGER,
  meeting_key    INTEGER,
  date           TIMESTAMP WITH TIME ZONE NOT NULL,
  lap_number     INTEGER,
  category       TEXT,
  flag           TEXT,
  scope          TEXT,
  message        TEXT,
  sector         TEXT,
  PRIMARY KEY (driver_number, session_key, date),
  FOREIGN KEY (driver_number, session_key) REFERENCES formula1.drivers(driver_number, session_key),
  FOREIGN KEY (meeting_key) REFERENCES formula1.meetings(meeting_key)
);

-- Convertimos la tabla en una hypertable
SELECT create_hypertable('formula1.race_control', by_range('date'));

-------------------------------------------------------------------------------
-- 11) stints (clave compuesta: driver_number + session_key)
-------------------------------------------------------------------------------
DROP TABLE IF EXISTS formula1.stints CASCADE;
CREATE TABLE formula1.stints (
  driver_number      INTEGER,
  session_key        INTEGER,
  meeting_key        INTEGER,
  stint_number       INTEGER,
  lap_start          INTEGER,
  lap_end            INTEGER,
  compound           TEXT,
  tyre_age_at_start  INTEGER,
  PRIMARY KEY (driver_number, session_key, stint_number), 
  FOREIGN KEY (driver_number, session_key) REFERENCES formula1.drivers(driver_number, session_key),
  FOREIGN KEY (meeting_key) REFERENCES formula1.meetings(meeting_key)
);

-------------------------------------------------------------------------------
-- 12) team_radio (convertida a hypertable)
-------------------------------------------------------------------------------
DROP TABLE IF EXISTS formula1.team_radio CASCADE;
CREATE TABLE formula1.team_radio (
  driver_number  INTEGER,
  session_key    INTEGER,
  meeting_key    INTEGER,
  date           TIMESTAMP WITH TIME ZONE NOT NULL,
  recording_url  TEXT,
  PRIMARY KEY (driver_number, session_key, date),
  FOREIGN KEY (driver_number, session_key) REFERENCES formula1.drivers(driver_number, session_key),
  FOREIGN KEY (meeting_key) REFERENCES formula1.meetings(meeting_key)
);

-- Convertimos la tabla en una hypertable
SELECT create_hypertable('formula1.team_radio', by_range('date'));


-------------------------------------------------------------------------------
-- 14) ingestion_logs (para registro de ingestión)
-------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ingestion_logs (
    meeting_key INTEGER NOT NULL,
    session_key INTEGER NOT NULL,
    table_name TEXT NOT NULL,
    last_ingested TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (meeting_key, session_key, table_name)
);

CREATE TABLE IF NOT EXISTS logs.large_ingestion_logs (
    meeting_key INTEGER NOT NULL,
    session_key INTEGER NOT NULL,
    driver_number INTEGER NOT NULL,
    table_name TEXT NOT NULL,
    last_ingested TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (meeting_key, session_key, table_name)
);


-------------------------------------------------------------------------------
-- 15) Tabla consolidada para modelo
-------------------------------------------------------------------------------

CREATE SCHEMA IF NOT EXISTS f1_consolidated;

drop table if exists f1_consolidated.features
CREATE TABLE f1_consolidated.features (       
    date TIMESTAMPTZ NOT NULL,                
    circuit_key VARCHAR(50) NOT NULL,         
    session_key VARCHAR(100) NOT NULL,        
    driver_number INTEGER NOT NULL,           
    lap_number INTEGER NOT NULL,              
    current_lap_time NUMERIC,                 
    race_percentage_completed NUMERIC,        
    current_tire VARCHAR(20),                 
    laps_on_current_tire INTEGER,             
    box_stops INTEGER,                        
    previous_lap_time NUMERIC,                
    lap_time_delta NUMERIC,                   
    accumulated_time NUMERIC,                 
    position_in_race INTEGER,                 
    time_difference_with_leader NUMERIC,      
    sector_1_time NUMERIC,                    
    sector_2_time NUMERIC,                    
    sector_3_time NUMERIC,                    
    box_stop_time NUMERIC,                    
    time_since_last_box_stop NUMERIC,         
    created_at TIMESTAMPTZ DEFAULT now(),     
    PRIMARY KEY (session_key, driver_number, lap_number) 
);

-- Índice para optimizar consultas por session_key
CREATE INDEX idx_session_key ON f1_consolidated.features (session_key);

-- Índice para optimizar consultas por driver_number
CREATE INDEX idx_driver_number ON f1_consolidated.features (driver_number);

-- Índice compuesto para consultas que utilicen session_key y lap_number
CREATE INDEX idx_session_lap ON f1_consolidated.features (session_key, lap_number);

-------------------------------------------------------------------------------
-- 15) Tablas para la visualización en Grafana
-------------------------------------------------------------------------------

CREATE SCHEMA IF NOT EXISTS visual;
CREATE TABLE IF NOT EXISTS circuit_points (
    circuit_key  INTEGER NOT NULL,
    point_order  SERIAL,           
    x            NUMERIC,          
    y            NUMERIC,
    z            NUMERIC,          
    description  TEXT,             
    PRIMARY KEY (circuit_key, point_order)
);

CREATE TABLE IF NOT EXISTS visual.sessions (
    year           INTEGER,
    meeting_key    INTEGER,
    session_key    INTEGER,
    location       TEXT,
    session_name   TEXT,
    driver_number  INTEGER,
    broadcast_name TEXT
);

INSERT INTO visual.sessions (year, meeting_key, session_key, location, session_name, driver_number, broadcast_name)
SELECT 
    s.year, 
    s.meeting_key, 
    s.session_key, 
    s.location, 
    s.session_name, 
    d.driver_number, 
    d.broadcast_name
FROM formula1.sessions AS s
INNER JOIN formula1.drivers AS d
    ON s.session_key = d.session_key;

CREATE TABLE IF NOT EXISTS visual.best_laps (
  driver_number    INTEGER,
  session_key      INTEGER,
  meeting_key      INTEGER,
  lap_number       INTEGER,
  date_start       TEXT,
  next_date_start  TEXT,
  lap_duration     REAL,
  PRIMARY KEY (driver_number, session_key)
);

INSERT INTO visual.best_laps (driver_number, session_key, meeting_key, lap_number, date_start, next_date_start, lap_duration)
WITH ranked_laps AS (
  SELECT 
    l.driver_number,
    l.session_key,
    l.meeting_key,
    l.lap_number,
    l.date_start,
    LEAD(l.date_start) OVER (PARTITION BY l.driver_number, l.session_key ORDER BY l.date_start) AS next_date_start,
    l.lap_duration,
    ROW_NUMBER() OVER (PARTITION BY l.driver_number, l.session_key ORDER BY l.lap_duration ASC) AS rn
  FROM formula1.laps l
)
SELECT 
  driver_number,
  session_key,
  meeting_key,
  lap_number,
  date_start,
  next_date_start,
  lap_duration
FROM ranked_laps
WHERE rn = 1;

CREATE TABLE IF NOT EXISTS visual.best_lap_speed (
  driver_number    INTEGER,
  session_key      INTEGER,
  meeting_key      INTEGER,
  lap_number       INTEGER,
  lap_date_start   TEXT,
  next_date_start  TEXT,
  lap_duration     REAL,
  car_date         REAL,
  speed            INTEGER,
  PRIMARY KEY (driver_number, session_key, lap_number, car_date)
);

INSERT INTO visual.best_lap_speed (
  driver_number, session_key, meeting_key, lap_number, 
  lap_date_start, next_date_start, lap_duration, 
  car_date, speed
)
SELECT
  bl.driver_number,
  bl.session_key,
  bl.meeting_key,
  bl.lap_number,
  bl.date_start AS lap_date_start,
  bl.next_date_start,
  bl.lap_duration,
  cd.date - bl.date_start AS car_date,
  cd.speed
FROM visual.best_laps bl
JOIN formula1.car_data cd
  ON cd.driver_number = bl.driver_number
  AND cd.session_key = bl.session_key
WHERE cd.date >= bl.date_start
  AND cd.date < bl.next_date_start;

