CREATE SCHEMA argo AUTHORIZATION pointyhaired;
GRANT USAGE ON SCHEMA argo TO argousers;

CREATE LANGUAGE plpgsql;


CREATE TABLE argo.files(
    id		SERIAL,
    basename	VARCHAR(40),
    date_creation	TIMESTAMP(0),
    date_update		TIMESTAMP(0),
    db_insertion	TIMESTAMP(2) NOT NULL DEFAULT NOW(),
    -- file_time           TIMESTAMP(2),
    file_hash           CHAR(33),
    PRIMARY KEY(id)/*,
    UNIQUE(basename)*/
    );

ALTER TABLE argo.files OWNER TO pointyhaired;
GRANT SELECT ON argo.files TO argousers;
GRANT INSERT, UPDATE, DELETE ON argo.files TO alice;

# ============================================================================

CREATE TABLE argo.profile(
    id		SERIAL,
    filelnk	INTEGER,
    date_creation	TIMESTAMP(0),
    date_update		TIMESTAMP(0),
    platform	INTEGER,
    cycle	INTEGER,
    data_centre		CHAR(2),
    dc_reference	VARCHAR(32),
    data_mode		CHAR(1),
    datetime		TIMESTAMP(0),
    datetime_qc		SMALLINT,
    inst_reference	VARCHAR(64),
    latitude		DOUBLE PRECISION,
    longitude		DOUBLE PRECISION,
    --position        POINT,
    location        GEOGRAPHY(POINT,4326),
    position_qc		SMALLINT,
    gmaoa_qc        BOOLEAN,
    PRIMARY KEY(id),
    --UNIQUE(filelnk,platform,datetime,data_mode),
    FOREIGN KEY (filelnk)
      REFERENCES argo.files (id)
      ON UPDATE CASCADE
      ON DELETE CASCADE
    );

--CREATE INDEX profile_data_mode_index ON argo.profile(data_mode);
CREATE INDEX profile_platform_index ON argo.profile(platform);
CREATE INDEX profile_datetime_index ON argo.profile(datetime);
-- CREATE INDEX profile_cycle_index ON argo.profile(cycle);
CREATE INDEX profile_location    ON argo.profile USING GIST(location); 

ALTER TABLE argo.profile OWNER TO pointyhaired;
GRANT SELECT ON argo.profile TO argousers;
GRANT INSERT, UPDATE, DELETE ON argo.profile TO alice;


/*
CREATE OR REPLACE FUNCTION argo.joint_qc() RETURNS TRIGGER AS $joint_qc$
    BEGIN
        IF (TG_OP = 'UPDATE') THEN
            IF (NEW.global_ranget = TRUE) & (NEW.global_ranges = TRUE)
                UPDATE argo.profile SET joint_qc = True  SELECT NEW.id;
            RETURN NEW;
        END IF;
        RETURN NULL; -- result is ignored since this is an AFTER trigger
    END;
$joint_qc$ LANGUAGE plpgsql;
*/







/*
CREATE TABLE argo.profile_analysis(
    id      INTEGER,
    z20,
    FOREIGN KEY(id)
      REFERENCE argo.profile(id)
      ON UPDATE CASCADE
      ON DELETE CASCADE
    );
*/

CREATE TABLE argo.levels(
    id			SERIAL,
    profilelnk		INTEGER,
    pressure		REAL,
    pressure_qc		SMALLINT,
    pressure_adjusted	REAL,
    pressure_adjusted_qc	SMALLINT,
    pressure_adjusted_error	REAL,
    temperature 	REAL,
    temperature_qc	SMALLINT,
    temperature_adjusted	REAL,
    temperature_adjusted_qc	SMALLINT,
    temperature_adjusted_error	REAL,
    salinity		REAL,
    salinity_qc		SMALLINT,
    salinity_adjusted	REAL,
    salinity_adjusted_qc	SMALLINT,
    salinity_adjusted_error	REAL,
    PRIMARY KEY(id),
    FOREIGN KEY (profilelnk)
      REFERENCES argo.profile (id)
      ON UPDATE CASCADE
      ON DELETE CASCADE
    );


CREATE INDEX levels_profile_index ON argo.levels(profilelnk);
CREATE INDEX levels_pressure_index ON argo.levels(pressure);
CREATE INDEX levels_temperature_index ON argo.levels(temperature);
CREATE INDEX levels_salinity_index ON argo.levels(salinity);

ALTER TABLE argo.levels OWNER TO pointyhaired;
GRANT SELECT ON argo.levels TO argousers;
GRANT INSERT, UPDATE, DELETE ON argo.levels TO alice;

# ============================================================================


CREATE TABLE argo.levels_30days(
    id                  SERIAL,
    profilelnk          INTEGER,
    pressure            REAL,
    temperature         REAL,
    salinity            REAL,
    PRIMARY KEY(id),
    FOREIGN KEY (profilelnk)
      REFERENCES argo.profile (id)
      ON UPDATE CASCADE
      ON DELETE CASCADE
    );

CREATE INDEX levels_30days_profile_index ON argo.levels_30days(profilelnk);
CREATE INDEX levels_30days_pressure_index ON argo.levels_30days(pressure);
CREATE INDEX levels_30days_temperature_index ON argo.levels_30days(temperature);
CREATE INDEX levels_30days_salinity_index ON argo.levels_30days(salinity);

CREATE TABLE argo.levels_flags(
    id                  INTEGER,
    global_rangeT       BOOLEAN,
    global_rangeS       BOOLEAN,
    not_spikeT          BOOLEAN,
    not_spikeS          BOOLEAN,
    not_gradientT       BOOLEAN,
    not_gradientS       BOOLEAN,
    FOREIGN KEY (id)
      REFERENCES argo.levels (id)
      ON UPDATE CASCADE
      ON DELETE CASCADE,
      UNIQUE (id)
    );



GRANT SELECT ON argo.profile_good TO argousers;
# ================







CREATE OR REPLACE FUNCTION argo.sync_levels_flags() RETURNS TRIGGER AS $levels_flags$
    BEGIN
        IF (TG_OP = 'INSERT') THEN
            INSERT INTO argo.levels_flags(id) SELECT NEW.id;
            RETURN NEW;
        END IF;
        RETURN NULL; -- result is ignored since this is an AFTER trigger
    END;
$levels_flags$ LANGUAGE plpgsql;

CREATE TRIGGER levels_flags
AFTER INSERT ON argo.levels
    FOR EACH ROW EXECUTE PROCEDURE argo.sync_levels_flags();



/* According to http://www.usgodae.org/argo/argo-dm-user-manual.pdf
0 No QC was performed
1 Good data
2 Probably good data
3 Bad, potentially correctible
4 Bad
5 Value changed
8 Interpolated
*/

CREATE OR REPLACE VIEW argo.profile_last30days AS
    SELECT id, extract(epoch from datetime) as t, datetime, 
        ST_Y(location::geometry) as latitude, 
	ST_X(location::geometry) as longitude
	FROM argo.profile WHERE datetime > (now()-interval '1 months') 
        ORDER BY id;



CREATE OR REPLACE VIEW argo.data_last30days AS
    SELECT l.id, l.profilelnk, p.datetime, 
        ST_Y(location::geometry) as latitude, 
	ST_X(location::geometry) as longitude,
	l.pressure, l.temperature, l.salinity
        FROM argo.profile AS p
	    -- JOIN argo.levels AS l
	    JOIN argo.levels_30days AS l
	    ON (p.id = l.profilelnk)
	WHERE p.datetime > (now()-interval '1 months')
	ORDER by p.datetime;


-- ARGO profiles with valid position and datetime.
CREATE OR REPLACE VIEW argo.profile_good AS
    SELECT *
        FROM argo.profile
        WHERE ((position_qc = 1) OR (position_qc = 2) OR (position_qc = 5) OR (position_qc = 8))
            AND ((datetime_qc = 1) OR (datetime_qc = 2) OR (datetime_qc = 5) OR (datetime_qc = 8));

-- ARGO levels with good pressure and temperature
CREATE OR REPLACE VIEW argo.levels_tgood AS
    SELECT *
        FROM argo.levels
        WHERE (pressure_qc = 1 OR pressure_qc = 2)
            AND (temperature_qc = 1 OR temperature_qc = 2)
        ;

-- ARGO profiles selected to estimate the Z20 (profile_goog + levels_tgood)
CREATE OR REPLACE VIEW argo.profile_z20 AS
    SELECT *
        FROM argo.profile_good
        WHERE id in
            (SELECT profilelnk
                FROM argo.levels_tgood
                GROUP BY profilelnk
                HAVING MIN(temperature)<19.5 AND MAX(temperature)>20.5
                    AND count(*)>10
                )
            ;

--CREATE INDEX levels_profilelnk_index ON argo.levels(profilelnk);
-- Need to work on these views!!!
CREATE OR REPLACE VIEW argo.profiles_tropical_atlantic AS 
    SELECT id,platform,datetime,latitude,longitude, position, location 
        FROM argo.profile AS p 
        WHERE ST_Covers(ST_GeographyFromText('SRID=4326;POLYGON((20 -23, 20 23, -70 23, -70 -23, 20 -23))'),location);

CREATE OR REPLACE VIEW argo.tropical_atlantic AS 
    SELECT l.id,platform,datetime,latitude,longitude,p.position,pressure,temperature,salinity 
        FROM argo.profile AS p INNER JOIN argo.levels AS l ON (p.id = l.profilelnk) 
        WHERE ST_Covers(ST_GeographyFromText('SRID=4326;POLYGON((20 -23, 20 23, -70 23, -70 -23, 20 -23))'),location);
        --WHERE latitude>=-23 AND latitude<=23 AND longitude<=20 AND longitude>=-70;


/*
A view to clean the realtime register on the database when there are one equivalent delayedmode for that.
This view should be used only on this case, so I'm not using this view, but injecting it into the function itself.

CREATE OR REPLACE VIEW profile_with_delayedmode AS
    SELECT * FROM profile 
        WHERE (platform, datetime,cycle) IN 
            (SELECT platform, datetime, cycle FROM profile WHERE data_mode='R' 
            INTERSECT 
            SELECT platform, datetime, cycle FROM profile WHERE data_mode='D')
    ;
*/

/* Another possible solution would be a trigger, so as soon as the data is inserted, the system checks for an older one and if is there, clean it.

CREATE OR REPLACE FUNCTION keepargoprofilesclean() RETURNS TRIGGER AS $keepargoprofilesclean$
    BEGIN
        IF (TG_OP = 'INSERT') THEN
            INSERT INTO profile(id) SELECT NEW.id;
            RETURN NEW;
        END IF;
        RETURN NULL; -- result is ignored since this is an AFTER trigger
    END;
$keepargoprofilesclean$ LANGUAGE plpgsql;

CREATE TRIGGER keepclean
AFTER INSERT ON profile
    FOR EACH ROW EXECUTE PROCEDURE keepargoprofilesclean();
*/        

-- Function to delete the realtime data when there is a delayedmode one that.
CREATE OR REPLACE FUNCTION argo.cleanargorealtime() RETURNS integer AS $$
    BEGIN
	DELETE FROM argo.profile WHERE id IN 
	    (SELECT id FROM argo.profile
	        WHERE 
		    (platform, datetime,cycle) IN
		        (SELECT platform, datetime, cycle FROM argo.profile WHERE data_mode='R'
			INTERSECT
			SELECT platform, datetime, cycle FROM argo.profile WHERE data_mode='D'
			)
		    AND data_mode='R'
            )
	;
	RETURN NULL;
    END;
    $$ LANGUAGE plpgsql;

/*
Work more on that. Think about a query to clean duplicated delayedmode data, but the criteria should be keep the most recent updated origin file.

profile ids of duplicated ones

select id from profile where (platform, datetime, cycle, data_mode) IN (select platform, datetime, cycle, data_mode from profile group by platform, datetime, cycle, data_mode having(count(*)>1));

*/

CREATE OR REPLACE FUNCTION argo.cleanargooutdated() RETURNS text AS $$
    DECLARE
        mviews RECORD;
	BEGIN
	    FOR mviews IN (SELECT platform, datetime, cycle, data_mode FROM argo.profile GROUP BY platform, datetime, cycle, data_mode HAVING(count(*)>1)) LOOP
	        DELETE FROM argo.profile WHERE id IN (SELECT p.id FROM argo.files AS f JOIN argo.profile AS p ON (f.id=p.filelnk) WHERE platform=mviews.platform AND datetime=mviews.datetime AND cycle=mviews.cycle AND data_mode=mviews.data_mode ORDER BY date_update DESC OFFSET 1);
	        --DELETE profile WHERE id IN (SELECT p.id FROM files AS f JOIN profile AS p ON (f.id=p.filelnk) WHERE platform=19018 AND datetime='2001-01-08 04:46:59' AND cycle=2 AND data_mode='D' ORDER BY date_update DESC OFFSET 1);
	    END LOOP;
	RETURN NULL;
    END;
    $$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION argo.cleanargo() RETURNS text AS $$
    BEGIN
        PERFORM argo.cleanargorealtime();
	PERFORM argo.cleanargooutdated();
	RETURN NULL;
    END;
    $$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION argo.update_30levels() RETURNS text AS $$
    BEGIN
	DELETE FROM argo.levels_30days
	    WHERE profilelnk IN
	        (SELECT id FROM argo.profile
                    WHERE datetime < (now()-interval '1 months'));
        INSERT INTO argo.levels_30days 
            (id, profilelnk, pressure, temperature, salinity) 
            (SELECT id, profilelnk, pressure, temperature, salinity 
	        FROM argo.levels 
                WHERE profilelnk IN 
                    (SELECT id 
	                FROM argo.profile 
	                WHERE datetime >= (now()-interval '1 months'))
                    AND id NOT IN (SELECT id FROM argo.levels_30days)
            );
	ANALYZE argo.levels_30days;
	REINDEX TABLE argo.levels_30days;
	RETURN NULL;
    END;
    $$ LANGUAGE plpgsql;


