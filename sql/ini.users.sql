#!/bin/bash


# PointyHaired - Owner. Only used locally to create things. Do nothing
#   Only local access
CREATE ROLE pointyhaired WITH 
    NOSUPERUSER CREATEDB NOCREATEROLE INHERIT LOGIN ENCRYPTED 
    PASSWORD 'msG968PW';

# Alice - Maintenance. Do backups, automatic data updates like load new ARGO, etc
#   Local access only
CREATE ROLE alice WITH 
    NOSUPERUSER NOCREATEDB NOCREATEROLE INHERIT LOGIN ENCRYPTED 
    PASSWORD 'twiceonsundays';

# Asok - The pydap user. Do all the job, but is not trustable
#   Remote access, from PyDAP server by SSL. Select only
CREATE ROLE asok WITH 
    NOSUPERUSER NOCREATEDB NOCREATEROLE INHERIT LOGIN ENCRYPTED 
    PASSWORD 'withsuggar?';

# Elbonian, Testing environment. Cannot see the production area, only testing
#   Locall access only.
CREATE ROLE elbonian WITH 
    NOSUPERUSER NOCREATEDB NOCREATEROLE INHERIT LOGIN ENCRYPTED 
    PASSWORD 'minhoca';

CREATE ROLE luizirber WITH 
    NOSUPERUSER NOCREATEDB NOCREATEROLE INHERIT LOGIN ENCRYPTED 
    PASSWORD 'G+E+B=1';

CREATE ROLE roberto WITH 
    NOSUPERUSER NOCREATEDB NOCREATEROLE INHERIT LOGIN ENCRYPTED 
    PASSWORD 'G+E+B=1';

CREATE ROLE castelao WITH 
    NOSUPERUSER NOCREATEDB NOCREATEROLE INHERIT LOGIN ENCRYPTED 
    PASSWORD 'leeward';


# ==== Grupos =========================================
CREATE ROLE pgis_users NOINHERIT;

CREATE ROLE argousers NOINHERIT;
CREATE ROLE ctdusers NOINHERIT;
CREATE ROLE tsgusers NOINHERIT;
CREATE ROLE xbtusers NOINHERIT;


GRANT pgis_users TO alice;
GRANT pgis_users TO asok;
GRANT pgis_users TO elbonian;

GRANT ctdusers TO alice;
GRANT ctdusers TO asok;
GRANT ctdusers TO elbonian;

GRANT argousers TO alice;
GRANT argousers TO asok;
GRANT argousers TO elbonian;
