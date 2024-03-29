CREATE SCHEMA ctd AUTHORIZATION pointyhaired;
GRANT USAGE ON SCHEMA ctd TO ctdusers;

CREATE TABLE ctd.loaded_file(
    id                  SERIAL,
    insertion_time      TIMESTAMP(2),
    file_time           TIMESTAMP(2),
    filename            VARCHAR(45),
    file_hash           CHAR(33),
    PRIMARY KEY(id)
    );

ALTER TABLE ctd.loaded_file OWNER TO pointyhaired;
GRANT SELECT ON ctd.loaded_file TO ctdusers;
GRANT INSERT, UPDATE, DELETE ON ctd.loaded_file TO alice;
GRANT UPDATE ON SEQUENCE ctd.loaded_file_id_seq TO alice;


CREATE TABLE ctd.cruise(
    id		SERIAL,
    shiplnk	INTEGER,
    name	VARCHAR,
    PRIMARY KEY(id)
    );

ALTER TABLE ctd.cruise OWNER TO pointyhaired;
GRANT SELECT ON ctd.cruise TO ctdusers;
GRANT INSERT, UPDATE, DELETE ON ctd.cruise TO alice;
GRANT UPDATE ON SEQUENCE ctd.cruise_id_seq TO alice;

CREATE TABLE ctd.station(
    id			SERIAL,
    filelnk	    	INTEGER,
    cruiselnk		INTEGER,
    /* Don't make sense date+time and datetime columns
       WOCE has it's own format, probably it should come from a view
       What would be more efficient? datetime or date + time?
       Should I consider the possible case of have date or time only?
    */
    --woce_time       VARCHAR(30),   -- WOCE time of day, (hhmmss.dd UTC), not sure what is it!?!
    --woce_date       VARCHAR(30),   -- WOCE date, (yyyymmdd UTC),
    --tempo           TIME,
    --data            DATE,
    datetime		TIMESTAMP(0), -- WITH TIME ZONE,
    --latitude        VARCHAR(50),
    --longitude       VARCHAR(50),
    location		GEOGRAPHY(POINT,4326), -- Not sure if make sense have it here
    position_qc         SMALLINT,
    --name            VARCHAR(50),
    -- cast , ?!?!
    cast_number		VARCHAR,
    station_number  VARCHAR,
    woce_version    VARCHAR(25),
    --shipname        VARCHAR(50),
    conventions     VARCHAR(50),
    woce_id         VARCHAR(50),
    woce_ctd_flag_desc  VARCHAR(150),
    notes           VARCHAR(500), -- Notes section from the .cnv's header
    PRIMARY KEY(id),
    --UNIQUE(filelnk,platform,datetime,data_mode),
    FOREIGN KEY (filelnk)
      REFERENCES ctd.loaded_file (id)
      ON UPDATE CASCADE
      ON DELETE CASCADE,
    FOREIGN KEY (cruiselnk)
      REFERENCES ctd.cruise (id)
      ON UPDATE CASCADE
      ON DELETE CASCADE
    );

ALTER TABLE ctd.station OWNER TO pointyhaired;
GRANT SELECT ON ctd.station TO ctdusers;
GRANT INSERT, UPDATE, DELETE ON ctd.station TO alice;
GRANT UPDATE ON SEQUENCE ctd.station_id_seq TO alice;


CREATE TABLE ctd.data(
    id			SERIAL,
    stationlnk		INTEGER NOT NULL,
    timeS		REAL,       --Time Elapsed [seconds]
    timeS_qc		SMALLINT,    --woce_flags
    depth		REAL,
    pressure            REAL,
    pressure_qc         SMALLINT,
    temperature         REAL,
    --temperature         NUMERIC(7,3),
    temperature_qc      SMALLINT,
    conductivity        REAL,
    --conductivity        NUMERIC(6,2),
    conductivity_qc     SMALLINT,
    salinity            REAL,
    --salinity            NUMERIC(6,3),
    salinity_qc         SMALLINT,
    potemperature         REAL,
    --flag  , O que é flag?!?!
    --flag_qc   , ?!?!
    potemperature_qc      SMALLINT,
    -- Should I include density?!
    PRIMARY KEY(id),
    FOREIGN KEY (stationlnk)
      REFERENCES ctd.station (id)
      ON UPDATE CASCADE
      ON DELETE CASCADE
    );

ALTER TABLE ctd.data OWNER TO pointyhaired;
GRANT SELECT ON ctd.data TO ctdusers;
GRANT INSERT, UPDATE, DELETE ON ctd.data TO alice;
GRANT UPDATE ON SEQUENCE ctd.data_id_seq TO alice;



CREATE TABLE ctd.station_flags (
    id                  INTEGER NOT NULL,
    possible_datetime   BOOLEAN,
    possible_location   BOOLEAN,
    at_sea              BOOLEAN,
    FOREIGN KEY (id)
        REFERENCES ctd.station (id)
        ON UPDATE CASCADE
        ON DELETE CASCADE,
    UNIQUE (id)
    );

CREATE TABLE ctd.data_flag (
    id                  INTEGER NOT NULL,
    measure		VARCHAR(3),	-- [T, S, T2, O, C]
    ML			SMALLINT, -- Machine Learn evaluation
    --ML_T		SMALLINT, -- Machine Learn evaluation for T
    --ML_S		SMALLINT, -- Machine Learn evaluation for S
    global_range	BOOLEAN,
    --global_rangeT       BOOLEAN,
    --global_rangeS       BOOLEAN,
    not_spike		BOOLEAN,
    --not_spikeT          BOOLEAN,
    --not_spikeS          BOOLEAN,
    not_gradient	BOOLEAN,
    --not_gradientT       BOOLEAN,
    --not_gradientS       BOOLEAN,
    not_digitroll	BOOLEAN,
    --digitrollT		BOOLEAN,
    --digitrollS		BOOLEAN,
    climatology		BOOLEAN,
    --climatologyt	BOOLEAN,
    --climatologys	BOOLEAN,
    FOREIGN KEY (id)
        REFERENCES ctd.data (id)
        ON UPDATE CASCADE
        ON DELETE CASCADE,
    UNIQUE (id, measure)/*,
    CONSTRAINT measure CHECK
        (measure ='t' OR measure = 't2'
          OR measure = 's' OR measure = 's2'
          OR measure = 'c' OR measure = 'c2'
          OR measure = 'o' OR measure = 'o2')*/
    );
-- CREATE TYPE element_type AS ENUM ('t', 's');

ALTER TABLE ctd.data_flag OWNER TO pointyhaired;
GRANT SELECT ON ctd.data_flag TO ctdusers;
GRANT INSERT, UPDATE, DELETE ON ctd.data_flag TO alice;

/*
CREATE TABLE pirata.loaded_files(
    id                  SERIAL,
    insertion_time      TIMESTAMP(2),
    file_time           TIMESTAMP(2),
    filename            VARCHAR(45),
    file_hash           CHAR(33),
    --N_orig              INTEGER,
    --N_duplicate         INTEGER,
    --N_raw               INTEGER,
    --ship_imono          VARCHAR(50),
    --UNIQUE(id)
    PRIMARY KEY(id)
    );


CREATE TABLE pirata.cruise(
    id		SERIAL,
    shiplnk	INTEGER,
    name	VARCHAR
    );


CREATE TABLE pirata.station(
    id			SERIAL,
    --filelnk		INTEGER,
    cruiselnk		INTEGER,
    datetime            TIMESTAMP(0) WITH TIME ZONE,
    --datetime_qc         SMALLINT,
    latitude            DOUBLE PRECISION,
    longitude           DOUBLE PRECISION,
    location		GEOGRAPHY(POINT,4326),
    position_qc         SMALLINT,
    PRIMARY KEY(id)--,
    --UNIQUE(filelnk,platform,datetime,data_mode),
    /*FOREIGN KEY (filelnk)
      REFERENCES pirata.file (id)
      ON UPDATE CASCADE
      ON DELETE CASCADE */
    );

CREATE TABLE pirata.data(
    id          SERIAL,
    stationlnk  INTEGER,
    depth		REAL,
    pressure            REAL,
    --pressure_qc         SMALLINT,
    --pressure_adjusted   REAL,
    --pressure_adjusted_qc        SMALLINT,
    --pressure_adjusted_error     REAL,
    temperature         REAL,
    --temperature         NUMERIC(7,3),
    --temperature_qc      SMALLINT,
    --temperature_adjusted        REAL,
    --temperature_adjusted_qc     SMALLINT,
    --temperature_adjusted_error  REAL,
    conductivity        REAL,
    --conductivity        NUMERIC(6,2),
    salinity            REAL,
    --salinity            NUMERIC(6,3),
    salinity_qc         SMALLINT,
    --salinity_adjusted   REAL,
    --salinity_adjusted_qc        SMALLINT,
    --salinity_adjusted_error     REAL,
    PRIMARY KEY(id),
    FOREIGN KEY (stationlnk)
      REFERENCES ctd.station (id)
      ON UPDATE CASCADE
      ON DELETE CASCADE
    );


CREATE TABLE pirata.station_flags (
    id                  INTEGER,
    possible_datetime   BOOLEAN,
    possible_location   BOOLEAN,
    FOREIGN KEY (id)
        REFERENCES pirata.station (id)
        ON UPDATE CASCADE
        ON DELETE CASCADE,
    UNIQUE (id)
    );

CREATE TABLE pirata.data_flags (
    id                  INTEGER,
    global_rangeT       BOOLEAN,
    global_rangeS       BOOLEAN,
    not_spikeT          BOOLEAN,
    not_spikeS          BOOLEAN,
    not_gradientT       BOOLEAN,
    not_gradientS       BOOLEAN,
    FOREIGN KEY (id)
        REFERENCES pirata.data (id)
        ON UPDATE CASCADE
        ON DELETE CASCADE,
    UNIQUE (id)
    );
*/


GRANT USAGE ON SCHEMA ctd TO ctdusers;
GRANT SELECT ON ctd.loaded_file TO ctdusers;
GRANT SELECT ON ctd.cruise TO ctdusers;
GRANT SELECT ON ctd.station TO ctdusers;
GRANT SELECT ON ctd.data TO ctdusers;
GRANT SELECT ON ctd.station_flags TO ctdusers;
GRANT SELECT ON ctd.data_flags TO ctdusers;

GRANT INSERT, UPDATE, DELETE ON ctd.loaded_file TO alice;
-- GRANT INSERT, UPDATE         ON ctd.loaded_file_id_seq TO alice;
GRANT INSERT, UPDATE, DELETE ON ctd.cruise TO alice;
-- GRANT INSERT, UPDATE         ON ctd.cruise_id_seq TO alice;
GRANT INSERT, UPDATE, DELETE ON ctd.station TO alice;
-- GRANT INSERT, UPDATE         ON ctd.station_id_seq TO alice;
GRANT INSERT, UPDATE, DELETE ON ctd.data TO alice;
-- GRANT INSERT, UPDATE         ON ctd.data_id_seq TO alice;
GRANT INSERT, UPDATE, DELETE ON ctd.station_flags TO alice;
GRANT INSERT, UPDATE, DELETE ON ctd.data_flags TO alice;

-- pirata.* not included

GRANT USAGE ON SCHEMA pirata_raw TO ctdusers;
GRANT SELECT ON pirata_raw.loaded_file TO ctdusers;
GRANT SELECT ON pirata_raw.cruise TO ctdusers;
GRANT SELECT ON pirata_raw.station TO ctdusers;
GRANT SELECT ON pirata_raw.data TO ctdusers;
GRANT SELECT ON pirata_raw.station_flags TO ctdusers;
GRANT SELECT ON pirata_raw.data_flags TO ctdusers;

GRANT INSERT, UPDATE, DELETE ON pirata_raw.loaded_file TO alice;
-- GRANT INSERT, UPDATE         ON pirata_raw.loaded_file_id_seq TO alice;
GRANT INSERT, UPDATE, DELETE ON pirata_raw.cruise TO alice;
-- GRANT INSERT, UPDATE         ON pirata_raw.cruise_id_seq TO alice;
GRANT INSERT, UPDATE, DELETE ON pirata_raw.station TO alice;
-- GRANT INSERT, UPDATE         ON pirata_raw.station_id_seq TO alice;
GRANT INSERT, UPDATE, DELETE ON pirata_raw.data TO alice;
-- GRANT INSERT, UPDATE         ON pirata_raw.data_id_seq TO alice;
GRANT INSERT, UPDATE, DELETE ON pirata_raw.station_flags TO alice;
GRANT INSERT, UPDATE, DELETE ON pirata_raw.data_flags TO alice;

-- pirata.* not included

