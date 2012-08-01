CREATE OR REPLACE VIEW ctd.station AS
    SELECT c.id as cruiseid, c.shiplnk, c.name as cruisename, 
        p.id as profileid, datetime, date(datetime) as woce_date, 
	"time"(datetime) as woce_time, latitude, longitude, location, 
	position_qc, cast_number, station_number, woce_version, woce_id, woce_ctd_flag_desc
	FROM pirata_raw.cruise as c INNER JOIN pirata_raw.profile as p ON (c.id = p.cruiselnk);


CREATE OR REPLACE VIEW ctd.data_qc AS
    SELECT 
        df.id, df.profilelnk, df.timeS, df.depth, df.pressure, df.temperature, df.conductivity, df.salinity, df.potemperature,
        df.global_rangeT, df.global_rangeS, df.not_spikeT, df.not_spikeS, df.not_gradientT, df.not_gradientS,
        pf.possible_datetime, pf.possible_location
      FROM ctd.profile_flags AS pf RIGHT JOIN 
      (SELECT 
        d.id, d.profilelnk, d.timeS, d.depth, d.pressure, d.temperature, d.conductivity, d.salinity, d.potemperature,
        f.global_rangeT, f.global_rangeS, f.not_spikeT, f.not_spikeS,
        f.not_gradientT, f.not_gradientS, f.climatologyT, f.climatologyS
          FROM ctd.data as d LEFT JOIN ctd.data_flags as f 
          ON (d.id = f.id)) AS df
    ON (pf.id = df.profilelnk);


CREATE OR REPLACE VIEW ctd.cruises_data_qc AS
    SELECT
      p.cruiseid, p.name, p.id AS profilelnk, p.location, p.station_number, p.t,
      d.id, d.times, d.depth, d.temperature, d.conductivity, d.salinity, d.potemperature, 
        d.global_ranget, d.global_ranges, d.not_spikes, d.not_spiket, d.not_gradientt, d.not_gradients, d.possible_datetime, d.possible_location
      FROM
        (SELECT 
          c.id AS cruiseid, c.name, 
          p.id, p.location, p.station_number, p.datetime as t
          FROM ctd.cruise AS c JOIN ctd.profile AS p ON (c.id = p.cruiselnk))
          AS p
        JOIN
        ctd.data_qc AS d
        ON (p.id = d.profilelnk);

CREATE OR REPLACE VIEW ctd.cruises_data_good AS
    SELECT cruiseid, name, profilelnk, location, station_number, extract(epoch from t) as t,
      ST_X(ST_AsText(location)) AS longitude, ST_Y(ST_AsText(location)) AS latitude, 
      id, times, depth, temperature, conductivity, salinity, potemperature 
      FROM ctd.cruises_data_qc
      WHERE global_ranget IS TRUE AND global_ranges IS TRUE AND not_spikes IS TRUE AND 
        not_spiket IS TRUE AND not_gradientt IS TRUE AND not_gradients IS TRUE AND
        possible_datetime IS TRUE AND possible_location IS TRUE;



GRANT SELECT on ctd.data_qc TO ctdusers;
