



/* ########################################################################## */
CREATE TABLE ship(
    --id        SERIAL,
    imono   INTEGER,
    name    VARCHAR,
    table_ref   VARCHAR(50),
    PRIMARY KEY (imono)
);

INSERT INTO ships(imono,name,table_ref) VALUES(9161728,'Explorer''s of the Seas','explorerofseas');
INSERT INTO ships(imono,name,table_ref) VALUES(9183518,'M/V Explorer','mvexplorer');
INSERT INTO ships(imono,name,table_ref) VALUES(6621636,'Miller Freeman','millerfreeman');
INSERT INTO ships(imono,name,table_ref) VALUES(8901406,'Oleander','oleander');
INSERT INTO ships(imono,name,table_ref) VALUES(9152741,'Albert Rickmers','albertrickmers');
INSERT INTO ships(imono,name,table_ref) VALUES(9105786,'Ronald H. Brown','ronaldbrown');
INSERT INTO ships(imono,name,table_ref) VALUES(8993227,'Nancy Foster','nancyfoster');
INSERT INTO ships(imono,name,table_ref) VALUES(6711003,'Rainier','rainier');
INSERT INTO ships(imono,name,table_ref) VALUES(9270335,'Oscar Dyson','oscardyson');
INSERT INTO ships(imono,name,table_ref) VALUES(7333195,'David Starr Jordan','davidsjordan');
INSERT INTO ships(imono,name,table_ref) VALUES(6710920,'Fairweather','fairweather');

/* ########################################################################## */
CREATE TABLE callsign(
    --id        SERIAL,
    imonolnk    INTEGER,
    callsign    VARCHAR(10),
    startdate   TIMESTAMP(0),
    enddate TIMESTAMP(0),
    --UNIQUE(id),
    FOREIGN KEY (imonolnk)
      REFERENCES ship(imono)
      ON UPDATE CASCADE
      ON DELETE CASCADE
    );

INSERT INTO callsign(imonolnk,callsign) VALUES(9161728,'ELWX5');
INSERT INTO callsign(imonolnk,callsign) VALUES(9161728,'C6SE4');
INSERT INTO callsign(imonolnk,callsign) VALUES(9183518,'C6TN4');
INSERT INTO callsign(imonolnk,callsign) VALUES(6621636,'WTDM');
INSERT INTO callsign(imonolnk,callsign) VALUES(8901406,'PJJU');
INSERT INTO callsign(imonolnk,callsign) VALUES(9152741,'ELVZ5');
INSERT INTO callsign(imonolnk,callsign) VALUES(9105786,'WTEC');
INSERT INTO callsign(imonolnk,callsign) VALUES(8993227,'WTER');
INSERT INTO callsign(imonolnk,callsign) VALUES(6711003,'WTEF');
INSERT INTO callsign(imonolnk,callsign) VALUES(9270335,'WTEP');
INSERT INTO callsign(imonolnk,callsign) VALUES(7333195,'WTDK');
INSERT INTO callsign(imonolnk,callsign) VALUES(6710920,'WTEB');



CREATE VIEW shipinfo AS
    SELECT imono,name,table_ref,callsign,startdate,enddate
        FROM ships JOIN callsign ON (ships.imono=callsign.imonolnk);


/* ########################################################################## */

