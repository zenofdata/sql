#!/usr/bin/env python
# -*- coding: utf-8 -*-

""" LIbrary to load CTD data into de DB

"""

import os
import codecs
from datetime import datetime
import glob

import numpy as np
from numpy import ma
import psycopg2

from cnv import cnv

import pdb

cfg = {
    'data_path': "/Users/castelao/work/inpe/PIRATA/Dados CTD/Split",
    'data_file_pattern': "dPIRA*.cnv",
    'defaults_file': '/Users/castelao/work/projects/python/pycnv/test_data/pirata13_defaults.yaml',
    'sql':{
        'user':'alice',
        'password': 'twiceonsundays',
        'dbname':'zen',
        'host':'192.168.1.40',
        }
    }

cnv_filenames = glob.glob(os.path.join(cfg['data_path'], cfg['data_file_pattern']))

#default_file = os.path.join(cfg['data_path'], 'pirata13_defaults.yaml')

#f = codecs.open(cnv_file, 'r', 'latin-1')
#text = f.read()

dsn = "".join(["%s='%s' " % (d,cfg['sql'][d]) for d in cfg['sql'].keys()])
conn = psycopg2.connect(dsn)


for cnv_filename in cnv_filenames:
    print cnv_filename
    ctd = cnv.fCNV(cnv_filename, cfg['defaults_file'])
    print ctd.attributes

    curs = conn.cursor()

    query = "SELECT id from ctd.loaded_file WHERE file_hash='%s'" % ctd.attributes['md5']
    curs.execute(query)
    if curs.rowcount==0:
        curs.execute("INSERT INTO ctd.loaded_file \
                (insertion_time, filename, file_hash) \
                VALUES (NOW(), %s, %s) \
                RETURNING id", \
                (ctd.attributes['filename'], ctd.attributes['md5'])
                )
        fileid = curs.fetchone()[0]

        #get cruise id
        query = "SELECT id from ctd.cruise WHERE name='%s'" % ctd.attributes['cruise']
        curs.execute(query)
        if curs.rowcount==0:
            curs.execute("INSERT INTO ctd.cruise (name) \
                VALUES (%s) \
                RETURNING id", 
                (ctd.attributes['cruise'],)
                )
        cruiseid = curs.fetchone()[0]
        print "This cruise: %s" % cruiseid

        ## pegar station e ver se existe no pirata_raw.profile
        #curs.execute("SELECT id FROM pirata_raw.profile \
        #        WHERE cruiselnk=%s AND datetime=%s AND location=ST_GeomFromText('POINT(%s %s)', 4326)" ,
        #        (cruiseid, ctd.attributes['datetime'], ctd.attributes['longitude'], ctd.attributes['latitude']),
        #        )
        curs.execute("INSERT INTO ctd.station \
            (cruiselnk, datetime, location, filelnk) \
            VALUES(%s, %s, ST_GeomFromText('POINT(%s %s)', 4326), %s) \
            RETURNING id" ,
            (cruiseid, ctd.attributes['datetime'],
                ctd.attributes['longitude'], ctd.attributes['latitude'],
                fileid
                )
            )  

        profileid = curs.fetchone()[0]

        #query = "INSERT INTO pirata_raw.profile (cruiselnk, datetime, latitude, longitude, woce_date, woce_time, station_number) VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING id"
        #curs.execute(query, (cruiselnk, datetime_, latitude, longitude, woce_date, woce_time, station) )
        
        num = ctd['timeS'].shape[0]
        
        # convert to list, replaces masked values with None, which gets adapted to NULL in the PSQL driver.
        time = ctd['timeS'].tolist()
        #depth = depth.tolist()
        press = ctd['pressure'].tolist()
        temp = ctd['temperature'].tolist()
        sal = ctd['salinity'].tolist()
        for i in range(num):
            #query = "INSERT INTO pirata_raw.data (profilelnk, times, depth, pressure, temperature, salinity) VALUES (%s, %s, %s, %s, %s, %s)"
            query = "INSERT INTO ctd.data (stationlnk, times, pressure, temperature, salinity) VALUES (%s, %s, %s, %s, %s)"
        
            #curs.execute (query, (profileid, (time[i]), (depth[i]),(press[i]), (temp[i]), (sal[i]),) )  
            curs.execute (query, (profileid, (time[i]), (press[i]), (temp[i]), (sal[i]),) )  
        
        
        print "Inserted %d records into pirata_raw.data" % num
        
        
        conn.commit()

