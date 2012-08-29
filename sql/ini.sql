apt-get udpate
apt-get upgrade
apt-get dist-upgrade

apt-get -s install postgresql postgresql-9.1-postgis  pgtune

apt-get -s install python-psycopg2 python-numpy python-pip
sudo pip install pupynere

sudo su postgres
psql -d template_postgis -f /usr/share/postgresql/9.1/contrib/postgis-1.5/postgis.sql
psql -d template_postgis -f /usr/share/postgresql/9.1/contrib/postgis-1.5/spatial_ref_sys.sql
psql -d template_postgis -f /usr/share/postgresql/9.1/contrib/postgis_comments.sql

# ===================================================================
# To install psql 9.1 into squeezy
apt-get  -t squeeze-backports install postgresql-plpython-9.1 postgresql-contrib-9.1
apt-get  -t squeeze-backports install postgresql-plpython-9.1 postgresql-contrib-9.1
http://trac.osgeo.org/postgis/wiki/UsersWikiPostGIS20Debian60src

createdb template_postgis
createlang plpgsql template_postgis
psql -d template_postgis -f /usr/share/postgresql/8.4/contrib/postgis-2.0/postgis.sql
psql -d template_postgis -f /usr/share/postgresql/8.4/contrib/postgis-2.0/spatial_ref_sys.sql
psql -d template_postgis -f /usr/share/postgresql/8.4/contrib/postgis-2.0/postgis_comments.sql

psql -d template_postgis -f /usr/share/postgresql/8.4/contrib/postgis-2.0/rtpostgis.sql
psql -d template_postgis -f /usr/share/postgresql/8.4/contrib/postgis-2.0/raster_comments.sql

psql -d template_postgis -f /usr/share/postgresql/8.4/contrib/postgis-2.0/topology.sql
psql -d template_postgis -f /usr/share/postgresql/8.4/contrib/postgis-2.0/topology_comments.sql

# ===================================================================

GRANT SELECT ON spatial_ref_sys, geometry_columns TO GROUP pgis_users;



# Old stuff, maybe still pertinent.
# Check about the permissions to run GIS functions on
# http://www.paolocorti.net/2008/01/30/installing-postgis-on-ubuntu/

# ===================================================================
# postgresql.conf
listen_addresses = 'localhost, 10.209.107.242'

sudo pgtune --type DW  -i  /etc/postgresql/9.1/main/postgresql.conf  -o  /etc/postgresql/9.1/main/postgresql.conf.pgtune

sudo vim /etc/sysctl.d/30-postgresql-shm.conf
kernel.shmmax = 190500000

# ===================================================================
# superuser password
# "nitdb --pwprompt" request a password on the creation
#   and "-A md5" do not use trust authentication as default

sudo -u postgres psql template1

template1=# ALTER USER postgres with encrypted password 'AK@L34pxD9';

# =========================

# At pg_hba.conf, change to
local   all             postgres                                md5

# TYPE  DATABASE        USER            ADDRESS                 METHOD
local   zen             pointyhaired                            md5
local   zen             elbonian                                md5
local   zen             asok                                    md5
local   zen             alice                                   md5

# "local" is for Unix domain socket connections only            
#local   all             all                                     peer
# IPv4 local connections:
hostssl zen             elbonian, asok   192.168.1.40/24         md5
hostssl zen_test        elbonian        10.211.29.195/32        md5
hostssl zen_test        asok            10.211.29.195/32        md5
hostssl zen_test        asok            10.240.217.126/32       md5
hostssl zen_test        asok            10.112.221.203/32       md5
#host    all             all             127.0.0.1/32            md5


#pg_ident.conf
#zen             castelao                pointyhaired
#zen             castelao                alice
#zen             castelao                asok
#zen             castelao                elbonian


# ========================

CREATE DATABASE	zen WITH OWNER pointyhaired TEMPLATE template_postgis ENCODING 'UTF8';
CREATE DATABASE	zen_test WITH OWNER pointyhaired TEMPLATE template_postgis ENCODING 'UTF8';


#createlang -U pointyhaired plpythonu zen_test

#create schema argo AUTHORIZATION elbonian;
#CREATE LANGUAGE plpgsql;
#CREATE LANGUAGE plpythonu;

#DATABASE=zen_test
#DBUSER=elbonian

#psql -U elbonian -d ${DATABASE} -f ./argo.sql
