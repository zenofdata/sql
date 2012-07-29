CREATE TABLE xbt.shipline(
    id		SERIAL,
    ...
    );

CREATE TABLE xbt.shiptransect(
    id		SERIAL,
    ...
    );

CREATE TABLE xbt.fall_rate(
    id		SERIAL,
    ...
    );


CREATE TABLE xbt.transect(
    id		SERIAL,
    imono	INTEGER,	-- 0-01-005 Buoy/platform identifier
    callsign	VARCHAR(6),
    shipname	VARCHAR(50),
    shipline_lnk	INTEGER,
    shiptransect_lnk	INTEGER,
    agency		INTEGER,
    ...
    FOREIGN KEY (shipline_lnk)
        REFERENCES xbt.shipline(id)
    FOREIGN KEY (shiptransect_lnk)
        REFERENCES xbt.shiptransect(id)
    );


CREATE TABLE xbt.profile(
    id		SERIAL,
    transect_lnk	INTEGER,
    data		DATE,
    tempo		TIME,
    position		POINT,	--(Lon,Lat)
    height		SMALINT,
    pressure_flag	,
    temperature_flag	,
    salinity_flag	,
    conductivity_flag	,
    fall_rate_lnk	INTEGER,
    ...
    FOREIGN KEY (transect_lnk)
        REFERENCES xbt.transect(id)
    );


CREATE TABLE xbt.levels(
    id		SERIAL,
    profile_lnk	INTEGER,
    temperature		,
    salinity		,
    conductivity	,
    FOREIGN KEY (profile_lnk)
        REFERENCES xbt.profile(id)
    );

-- INDEXES

-- VIEWS

-- FUNCTIONS
