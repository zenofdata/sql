#!/usr/bin/env python
# -*- coding: utf-8 -*-

""" LIbrary to load CTD data into de DB

"""

import os
import codecs
from datetime import datetime

import numpy as np
from numpy import ma
import psycopg2

from cnv import cnv

import pdb

    #'data_path': "/Users/castelao/work/projects/python/pycnv/test_data",
cfg = {
    'data_path': "./",
    'cruise': "unkown",
    'sql':{
        'user':'elbonian',
        'dbname':'gmaoa_test',
        'host':'antares.ccst.inpe.br',
        }
    }

basename = 'dPIRX003.cnv'

cnv_file = os.path.join(cfg['data_path'], basename)

f = codecs.open(cnv_file, 'r', 'latin-1')
text = f.read()

ctd = cnv.CNV(text)


if 'cruise' in cfg:
    ctd.attributes['cruise'] = cfg['cruise']

ctd.attributes['datetime'] = datetime(2008,3,2,18,52,30)
ctd.attributes['latitude'] = 12.67433333333333
ctd.attributes['longitude'] = -38.0018333333


dsn = "".join(["%s='%s' " % (d,cfg['sql'][d]) for d in cfg['sql'].keys()])
conn = psycopg2.connect(dsn)

curs = conn.cursor()

#get cruise id
query = "SELECT id from pirata_raw.cruise WHERE name='%s'" % ctd.attributes['cruise']
curs.execute(query)
if curs.rowcount==0:
    curs.execute("INSERT INTO pirata_raw.cruise (name) \
            VALUES (%s) \
            RETURNING id", 
            (ctd.attributes['cruise'],)
            )

cruiseid = curs.fetchone()[0]
conn.commit()
print "This cruise: %s" % cruiseid

# pegar station e ver se existe no pirata_raw.profile
curs.execute("SELECT id FROM pirata_raw.profile \
        WHERE cruiselnk=%s AND datetime=%s AND location=ST_GeomFromText('POINT(%s %s)', 4326)" ,
        (cruiseid, ctd.attributes['datetime'], ctd.attributes['longitude'], ctd.attributes['latitude']),
        )
if curs.rowcount==0:
    curs.execute("INSERT INTO pirata_raw.profile \
        (cruiselnk, datetime, location) \
        VALUES(%s, %s, ST_GeomFromText('POINT(%s %s)', 4326)) \
        RETURNING id" ,
        (cruiseid, ctd.attributes['datetime'],
            ctd.attributes['longitude'], ctd.attributes['latitude']
            )
        )
else:
    print "Já inseri %s vezes este dado" % curs.rowcount
    #return
    import sys; sys.exit()

profileid = curs.fetchone()[0]

#query = "INSERT INTO pirata_raw.profile (cruiselnk, datetime, latitude, longitude, woce_date, woce_time, station_number) VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING id"
#curs.execute(query, (cruiselnk, datetime_, latitude, longitude, woce_date, woce_time, station) )


pdb.set_trace()
num = ctd['timeS'].shape[0]

# convert to list, replaces masked values with None, which gets adapted to NULL in the PSQL driver.
time = ctd['timeS'].tolist()
#depth = depth.tolist()
press = ctd['prDM'].tolist()
temp = ctd['potemp090C'].tolist()
sal = ctd['sal00'].tolist()
for i in range(num):
    #query = "INSERT INTO pirata_raw.data (profilelnk, times, depth, pressure, temperature, salinity) VALUES (%s, %s, %s, %s, %s, %s)"
    query = "INSERT INTO pirata_raw.data (profilelnk, times, pressure, temperature, salinity) VALUES (%s, %s, %s, %s, %s)"

    #curs.execute (query, (profileid, (time[i]), (depth[i]),(press[i]), (temp[i]), (sal[i]),) )  
    curs.execute (query, (profileid, (time[i]), (press[i]), (temp[i]), (sal[i]),) )  


print "Inserted %d records into pirata_raw.data" % num


conn.commit()

