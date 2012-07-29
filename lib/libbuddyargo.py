#!/usr/bin/env python

# -*- coding: Latin-1 -*-
# vim: tabstop=4 shiftwidth=4 expandtab

""" A library just to deal with buddy check with Argo floats.

    !!!!ATENTION!!! Improve it. Load the data using MA, then use the masks to minmize the inserts.
"""

__author__ = "Guilherme Castelao"

def dap_argo_urls(dia=None,maxdt=None,ini_date=None,fin_date=None):
    """This function return a list of available argo urls datasets
         accessible through the DAP.
    """
    import urllib
    from datetime import datetime
    from datetime import timedelta

    if ini_date == None: ini_date = datetime(2004,2,1)
    if (fin_date == None):
        if maxdt == None:
            fin_date = datetime.now()
        else:
            fin_date = ini_date + maxdt
    #if dia == None:
    #    ini_date = datetime(2004,2,1)
    #    fin_date = datetime.now()
    #else:
    #    if maxdt==None:
    #        maxdt = timedelta(days=10)
    #    ini_date=dia-maxdt
    #    fin_date=dia+maxdt

    # Maybe move the system to use:
    # http://dapper.pmel.noaa.gov/dapper/argo/argo_all.cdp.info
    
    dap_url_base="http://www.ifremer.fr/cgi-bin/nph-dods/data/in-situ/argo/latest_data"
    
    input_files_dt=timedelta(days=1)
    url_list=[]
    
    d=ini_date
    #y_test = 1
    #m_test = 0
    f = urllib.urlopen(dap_url_base) 
    text = f.read()
    while d<=fin_date:
        #url = "%s/%s/%s/%s_prof.nc" % (dap_url_base,d.year,d.strftime("%m"),d.strftime("%Y%m%d"))
        #if (d.year != y_test) or (d.month != m_test):
        #    y_test=d.year
        #    m_test=d.month
        #    #url = "%s/%s/%s/" % (dap_url_base,d.year,d.strftime("%m"))
        #    #f = urllib.urlopen(url) 
        #    text = f.read()
        url = dap_url_base
        basename = 'R%s_prof.nc' % d.strftime("%Y%m%d")
        if basename in text:
          url_list.append("%s/%s" % (url,basename)) 
        basename = 'D%s_prof.nc' % d.strftime("%Y%m%d")
        if basename in text:
          url_list.append("%s/%s" % (url,basename)) 
        d+=input_files_dt
    
    return url_list


def nocd_ncfile_decode(filename):
    """ Decode a NetCDF ARGO file from NODC
    """
    from numpy import ma
    import string
    from datetime import datetime
    from datetime import timedelta
    from pupynere import netcdf_file
    ncf = netcdf_file(filename)
    data = {}
    data['date_ref'] = datetime.strptime(string.join(ncf.variables['reference_date_time'][:],''),"%Y%m%d%H%M%S")
    data['date_creation'] = datetime.strptime(string.join(ncf.variables['date_update'][:],''),"%Y%m%d%H%M%S")
    data['date_update'] = datetime.strptime(string.join(ncf.variables['date_update'][:],''),"%Y%m%d%H%M%S")
    data['platform'] = int(string.join(ncf.variables['platform_number'],''))
    data['cycle'] = ncf.variables['cycle_number'][0]
    data['data_centre'] = string.join(ncf.variables['data_centre'][:],'').strip()  #2
    data['dc_reference'] = string.join(ncf.variables['dc_reference'][:],'').strip() #32
    data['data_mode'] = string.join(ncf.variables['data_mode'][:],'').strip() #1
    data['datetime'] = data['date_ref']+timedelta(days=int(ncf.variables['juld'][:]))
    data['datetime_qc'] = int(string.join(ncf.variables['juld_qc'],''))
    data['inst_reference'] = string.join(ncf.variables['inst_reference'][:],'').strip() #64
    data['latitude'] = ncf.variables['latitude'][0]
    data['longitude'] = ncf.variables['longitude'][0]
    data['position_qc'] = int(string.join(ncf.variables['position_qc'],''))
    for k in ['pressure', 'pressure_adjusted']:
        if k in ncf.variables.keys():
            data[k] = ma.array(ncf.variables[k][:]) #,dataset.PRES._FillValue)
            data[k+'_qc'] = ma.array(ncf.variables[k+'_qc'][:,0], dtype='i')
    for k in ['temperature', 'temperature_adjusted', 'salinity', 'salinity_adjusted']:
        if k in ncf.variables.keys():
            data[k] = ma.array(ncf.variables[k][0,:,0,0])
            data[k+'_qc'] = ma.array(ncf.variables[k+'_qc'][:,0], dtype='i')
    #data['salinity_adjusted_error'] = ma.masked_values(dataset.PSAL_ADJUSTED_ERROR[:],dataset.PSAL_ADJUSTED_ERROR._FillValue)
    return data


#def 
import urllib
#from datetime import datetime
#from datetime import timedelta
import re
import shutil

url_base = "http://data.nodc.noaa.gov/opendap/argo/data/"
f = urllib.urlopen(url_base)
text = f.read()
f.close()
tarfiles = re.findall(r"<a href=\"(.*)\">argo_\S{2}\d+\.tgz</a>", text)

import tempfile
import os.path
import numpy
import string
import psycopg2
from datetime import datetime

for tgzf in tarfiles:
    tgz = urllib.urlopen(url_base+tgzf)
    dtmp = tempfile.mkdtemp()
    
    argo_files_table = "argo.files"
    argo_profile_table = "argo.profile"
    argo_levels_table = "argo.levels"
    dbname = 'zen_test'
    dbuser = 'elbonian'
    dbhost = 'localhost'
    dbpass = 'minhoca'
    
    
    dsn='dbname=%s user=%s host=%s password=%s' % (dbname, dbuser, dbhost, dbpass)
    conn = psycopg2.connect(dsn)
    
    query = "SELECT id FROM %s WHERE basename='%s'" % (argo_files_table, tgzf)
    curs = conn.cursor()
    curs.execute(query)
    n_rows = curs.rowcount
    if n_rows == 1:
        print "Looks like %s was already included before" % tgzf
    elif n_rows > 1:
        print "OOppss, looks like this file were included more then once before. Sounds like a problem!"
    elif n_rows == 0:
        #print "Openning url: %s" % url
        try:
            #f = tempfile.NamedTemporaryFile(delete=False)
            f = open(os.path.join(dtmp,tgzf),'w')
            f.write(tgz.read())
            f.close()

            import tarfile
            tar = tarfile.open(os.path.join(dtmp,tgzf))
            tar.extractall(path=dtmp)
            argo_files = []
            for root, dirs, files in os.walk(dtmp):
                for file in files:
                    if re.match('.*\.nc',file):
                        argo_files.append( os.path.join(root,file))

            print "Processing file: %s" % tgzf
            query="INSERT INTO %s(basename) VALUES('%s')" % (argo_files_table, tgzf)
            curs.execute(query)
            for argo_file in argo_files:
                basename = os.path.basename(argo_file)
                data = nocd_ncfile_decode(argo_file)
                # ============================================================
                # Storing information about the data file.
                tnow = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-4]
                #query="INSERT INTO %s(basename,date_creation,date_update) VALUES('%s','%s','%s')" % (argo_files_table,basename,data['date_creation'],data['date_update'])
                query="INSERT INTO %s(basename, db_insertion) VALUES('%s','%s')" % (argo_files_table, basename, tnow)
                curs.execute(query)
                #query = "SELECT id FROM %s WHERE basename='%s' AND date_creation='%s' AND date_update = '%s'" % (argo_files_table,basename,data['date_creation'],data['date_update'])
                query = "SELECT id FROM %s WHERE basename='%s' AND db_insertion = '%s'" % (argo_files_table, basename, tnow) 
                curs.execute(query)
                fileid=curs.fetchall()[0][0]
                #query="INSERT INTO %s (filelnk,date_creation,date_update,platform,cycle,data_centre,dc_reference,data_mode,datetime,datetime_qc,inst_reference,latitude,longitude,location,position_qc) VALUES(%s,'%s','%s',%s,%s,'%s','%s','%s','%s',%s,'%s',%s,%s,'(%s,%s)',%s)"  % (argo_profile_table,fileid,data['date_creation'],data['date_update'],data['platform'],data['cycle'],data['data_centre'],data['dc_reference'],data['data_mode'],data['datetime'],data['datetime_qc'],data['inst_reference'],data['latitude'],data['longitude'],data['longitude'],data['latitude'],data['position_qc'])
                query="INSERT INTO %s (filelnk,date_creation,date_update,platform,cycle,data_centre,dc_reference,data_mode,datetime,datetime_qc,inst_reference,latitude,longitude,location,position_qc) VALUES(%s,'%s','%s',%s,%s,'%s','%s','%s','%s',%s,'%s',%s,%s, ST_GeomFromText('POINT(%s %s)', 4326),%s)"  % (argo_profile_table,fileid,data['date_creation'],data['date_update'],data['platform'],data['cycle'],data['data_centre'],data['dc_reference'],data['data_mode'],data['datetime'],data['datetime_qc'],data['inst_reference'],data['latitude'],data['longitude'],data['longitude'],data['latitude'],data['position_qc'])
                #query="INSERT INTO %s (filelnk,date_creation,date_update,platform,cycle,data_centre,dc_reference,data_mode,datetime,datetime_qc,inst_reference,latitude,longitude,position_qc) VALUES(%s,'%s','%s',%s,%s,'%s','%s','%s','%s',%s,'%s',%s,%s,%s)"  % (argo_profile_table,fileid,data['date_creation'],data['date_update'],data['platform'],data['cycle'],data['data_centre'],data['dc_reference'],data['data_mode'],data['datetime'],data['datetime_qc'],data['inst_reference'],data['latitude'],data['longitude'],data['position_qc'])
                query=query.replace("'(--,--)'","NULL").replace('--',"NULL")
                curs.execute(query)
                #oid = curs.lastrowid
                #print oid
                #query = "SELECT id FROM %s where oid = %s" % (argo_profile_table,oid)
                #curs.execute(query)
                #print curs.fetchall()
                #query = "SELECT id FROM %s WHERE filelnk=%s AND platform=%s AND data_centre='%s' AND dc_reference='%s' AND data_mode='%s' AND datetime='%s' AND datetime_qc=%s AND inst_reference='%s' AND latitude=%s AND longitude=%s AND location~=point(%s,%s) AND position_qc=%s" % (argo_profile_table,fileid,data['platform'],data['data_centre'],data['dc_reference'],data['data_mode'],data['datetime'],data['datetime_qc'],data['inst_reference'],data['latitude'],data['longitude'],data['longitude'],data['latitude'],data['position_qc'])
                query = "SELECT id FROM %s WHERE filelnk=%s AND platform=%s AND data_centre='%s' AND dc_reference='%s' AND data_mode='%s' AND datetime='%s' AND datetime_qc=%s AND inst_reference='%s' AND latitude=%s AND longitude=%s AND position_qc=%s" % (argo_profile_table,fileid,data['platform'],data['data_centre'],data['dc_reference'],data['data_mode'],data['datetime'],data['datetime_qc'],data['inst_reference'],data['latitude'],data['longitude'], data['position_qc'])
                query = query.replace("~=point(--,--)","=NULL").replace('--',"NULL").replace('=NULL',' is NULL').replace("point(--,--)","NULL")
                curs.execute(query)
                profileid=curs.fetchall()[0][0]
                #ind_good_level = False
                #ind_good_level = ind_good_level &((data['temperature'][p].mask) & (data['temperature'][p].mask))==False
                row_fields = ['pressure','pressure_qc','pressure_adjusted','pressure_adjusted_qc','pressure_adjusted_error','temperature','temperature_qc','temperature_adjusted','temperature_adjusted_qc','temperature_adjusted_error','salinity','salinity_qc','salinity_adjusted','salinity_adjusted_qc','salinity_adjusted_error']
                for z in numpy.arange(data['pressure'].size): #[ind_good_level]:
                    fields = []
                    values = []
                    for field in row_fields:
                        if field in data.keys():
                            fields.append(field)
                            values.append(data[field][z])
                    query = "INSERT INTO %s(profilelnk,%s) VALUES(%s,%s)" % (argo_levels_table, string.join(fields,","), profileid, string.join([str(v) for v in values],","))
                    query = query.replace('--',"NULL").replace('=NULL',' is NULL')
                    curs.execute(query)
            conn.commit()
            #
            curs.execute("SELECT argo.cleanargorealtime()")
            conn.commit()
        except:
            print "Problems to insert data from the file %s" % basename
            print "QUERY: %s" % query
            conn.rollback()
            #raise
        finally:
            shutil.rmtree(dtmp)
            print "Deleted: %s" % dtmp









def argo_catalog(urls,dbname = 'phod_test',dbuser='phod_test'):
    """
    """
    argo_files_table = "argo.argo_files"
    argo_profile_table = "argo.argo_profile"
    argo_levels_table = "argo.argo_levels"

    from tsgsql import TSGSQL
    db = TSGSQL(dbname=dbname, dbuser=dbuser)
    #import dap.client
    from pydap.client import open_url
    import string
    from datetime import datetime
    from datetime import timedelta
    import os
    import numpy
    from numpy import ma

    # %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
    import psycopg2
    dsn='dbname=%s user=%s' % (dbname,dbuser)
    conn = psycopg2.connect(dsn)

    for url in urls:
        basename = os.path.basename(url)
        query = "SELECT id FROM %s WHERE basename='%s'" % (argo_files_table,basename)
        curs = conn.cursor()
        curs.execute(query)
        n_rows = curs.rowcount

        #if curs.rowcount==1:
        #    id=curs.fetchall()[0][0]
        #elif curs.rowcount==0:
        #    insert = "INSERT INTO argo_files(basename) VALUES('%s')" % (basename)
        #    curs.execute(insert)
        #    curs.execute(query)
        #    id=curs.fetchone()[0]
        #elif curs.rowcount>1:
        #    print "Ops, looks like there are more then one!!!"
        #    id=None

        if n_rows == 1:
            print "Looks like %s was already included before" % basename
        elif n_rows > 1:
            print "OOppss, looks like this file were included more then once before. Sounds like a problem!"
        elif n_rows == 0:
            print "Openning url: %s" % url
            try:
                # ==== First Grab the data ========================
                data={}
                #dataset=dap.client.open(url)
                dataset=open_url(url)
                #
                data['date_ref']=datetime.strptime(string.join(dataset.REFERENCE_DATE_TIME[:],''),"%Y%m%d%H%M%S")
                data['date_creation']=datetime.strptime(string.join(dataset.DATE_CREATION[:],''),"%Y%m%d%H%M%S")
                data['date_update']=datetime.strptime(string.join(dataset.DATE_UPDATE[:],''),"%Y%m%d%H%M%S")
                data['platform']=numpy.array([int(string.join(i,'')) for i in dataset.PLATFORM_NUMBER[:]])
                data['cycle']=numpy.array(dataset.CYCLE_NUMBER[:])
                data['data_centre'] = numpy.array([(string.join(s,'')).strip() for s in dataset.DATA_CENTRE[:]]) #2
                data['dc_reference'] = numpy.array([(string.join(s,'')).strip() for s in dataset.DC_REFERENCE[:]]) #32
                data['data_mode']=numpy.array(dataset.DATA_MODE[:],dtype='|S1') #1
                data['datetime']=numpy.array([data['date_ref']+timedelta(days=j) for j in dataset.JULD[:]])
                #data['datetime_qc']=ma.masked_values(dataset.JULD_QC[:],dataset.JULD_QC._FillValue)
                data['datetime_qc']=numpy.array(dataset.JULD_QC[:],dtype='i')
                data['inst_reference']= numpy.array([(string.join(s,'')).strip() for s in dataset.INST_REFERENCE[:]]) #64
                data['latitude'] = ma.masked_values(dataset.LATITUDE[:],dataset.LATITUDE._FillValue)
                data['longitude'] = ma.masked_values(dataset.LONGITUDE[:],dataset.LONGITUDE._FillValue)
                data['position_qc'] = ma.masked_values(dataset.POSITION_QC[:],dataset.POSITION_QC._FillValue)
                data['pressure'] = ma.masked_values(dataset.PRES[:],dataset.PRES._FillValue)
                data['pressure_qc'] = ma.masked_values(dataset.PRES_QC[:],dataset.PRES_QC._FillValue)
                data['pressure_adjusted'] = ma.masked_values(dataset.PRES_ADJUSTED[:],dataset.PRES_ADJUSTED._FillValue)
                data['pressure_adjusted_qc'] = ma.masked_values(dataset.PRES_ADJUSTED_QC[:],dataset.PRES_ADJUSTED_QC._FillValue)
                data['pressure_adjusted_error'] = ma.masked_values(dataset.PRES_ADJUSTED_ERROR[:],dataset.PRES_ADJUSTED_ERROR._FillValue)
                data['temperature'] = ma.masked_values(dataset.TEMP[:],dataset.TEMP._FillValue)
                data['temperature_qc'] = ma.masked_values(dataset.TEMP_QC[:],dataset.TEMP_QC._FillValue)
                data['temperature_adjusted'] = ma.masked_values(dataset.TEMP_ADJUSTED[:],dataset.TEMP_ADJUSTED._FillValue)
                data['temperature_adjusted_qc'] = ma.masked_values(dataset.TEMP_ADJUSTED_QC[:],dataset.TEMP_ADJUSTED_QC._FillValue)
                data['temperature_adjusted_error'] = ma.masked_values(dataset.TEMP_ADJUSTED_ERROR[:],dataset.TEMP_ADJUSTED_ERROR._FillValue)
                data['salinity'] = ma.masked_values(dataset.PSAL[:],dataset.PSAL._FillValue)
                data['salinity_qc'] = ma.masked_values(dataset.PSAL_QC[:],dataset.PSAL_QC._FillValue)
                data['salinity_adjusted'] = ma.masked_values(dataset.PSAL_ADJUSTED[:],dataset.PSAL_ADJUSTED._FillValue)
                data['salinity_adjusted_qc'] = ma.masked_values(dataset.PSAL_ADJUSTED_QC[:],dataset.PSAL_ADJUSTED_QC._FillValue)
                data['salinity_adjusted_error'] = ma.masked_values(dataset.PSAL_ADJUSTED_ERROR[:],dataset.PSAL_ADJUSTED_ERROR._FillValue)
                # ============================================================
                n=(data['platform']).size
                for k in data:
                    #if data[k].size!=n:
                    #    print "Problems on the file parser. Arrays with different sizes."
                    #    return
                    if type(data[k])==numpy.ndarray:
                        s=data[k].shape
                        if len(s)==1:
                            for i in range(s[0]):
                                if data[k][i]==' ':
                                    #print k,i
                                    data[k][i]='NULL'
                        elif len(s)==2:
                            for i in range(s[0]):
                                for j in range(s[1]):
                                    if data[k][i][j]==' ':
                                        #print k,i
                                        data[k][i][j]='NULL'
                print "There are %i registers for this file. (%s)" % (n,datetime.now())
                # ============================================================
                # Storing information about the data file.
                query="INSERT INTO %s(basename,date_creation,date_update) VALUES('%s','%s','%s')" % (argo_files_table,basename,data['date_creation'],data['date_update'])
                curs.execute(query)
                query = "SELECT id FROM %s WHERE basename='%s' AND date_creation='%s' AND date_update = '%s'" % (argo_files_table,basename,data['date_creation'],data['date_update'])
                curs.execute(query)
                fileid=curs.fetchall()[0][0]
                #print "fieldid: ",fileid
                # Work on this, It will be the next system.
                # Storing information about the profile.
                #for p in range((data['platform']).size):
                for p in range(n):
                    #print "p: ",p
                    #fields=['platform','cycle','data_centre','dc_reference','data_mode','datetime','datetime_qc','inst_reference','latitude','longitude','position_qc']
                    #values=[fileid]
                    #for f in fields:
                    #    v=data[f][p]
                    #    if type(v) == numpy.core.ma.MaskedArray:
                    #        if v.mask==True:
                    #            values.append('NULL')
                    #        else:
                    #            values.append(v.data)
                    #    else:
                    #        if v in ['','_']:
                    #            values.append('NULL')
                    #        else:
                    #            values.append(v)
                    #insert="INSERT INTO %s (%s) VALUES %s" % (argo_profile_table,string.join(fields,','),tuple(values))
                    query="INSERT INTO %s (filelnk,platform,cycle,data_centre,dc_reference,data_mode,datetime,datetime_qc,inst_reference,latitude,longitude,position,position_qc) VALUES(%s,%s,%s,'%s','%s','%s','%s',%s,'%s',%s,%s,'(%s,%s)',%s)"  % (argo_profile_table,fileid,data['platform'][p],data['cycle'][p],data['data_centre'][p],data['dc_reference'][p],data['data_mode'][p],data['datetime'][p],data['datetime_qc'][p],data['inst_reference'][p],data['latitude'][p],data['longitude'][p],data['longitude'][p],data['latitude'][p],data['position_qc'][p])
                    query=query.replace("'(--,--)'","NULL").replace('--',"NULL")
                    curs.execute(query)
                    query = "SELECT id FROM %s WHERE filelnk=%s AND platform=%s AND data_centre='%s' AND dc_reference='%s' AND data_mode='%s' AND datetime='%s' AND datetime_qc=%s AND inst_reference='%s' AND latitude=%s AND longitude=%s AND position~=point(%s,%s) AND position_qc=%s" % (argo_profile_table,fileid,data['platform'][p],data['data_centre'][p],data['dc_reference'][p],data['data_mode'][p],data['datetime'][p],data['datetime_qc'][p],data['inst_reference'][p],data['latitude'][p],data['longitude'][p],data['longitude'][p],data['latitude'][p],data['position_qc'][p])
                    query = query.replace("~=point(--,--)","=NULL").replace('--',"NULL").replace('=NULL',' is NULL').replace("point(--,--)","NULL")
                    curs.execute(query)
                    profileid=curs.fetchall()[0][0]
                    # Storing the levels
                    row_fields = ['pressure','pressure_qc','pressure_adjusted','pressure_adjusted_qc','pressure_adjusted_error','temperature','temperature_qc','temperature_adjusted','temperature_adjusted_qc','temperature_adjusted_error','salinity','salinity_qc','salinity_adjusted','salinity_adjusted_qc','salinity_adjusted_error']
                    #for z in numpy.arange(data['pressure'][p].size):
                    ind_good_level = ((data['temperature'][p].mask) & (data['temperature'][p].mask))==False
                    for z in numpy.arange(data['pressure'][p].size)[ind_good_level]:
                        #print "z: ",z
                        #query="INSERT INTO %s(profilelnk,pressure,pressure_qc,pressure_adjusted,pressure_adjusted_qc,pressure_adjusted_error,temperature,temperature_qc,temperature_adjusted,temperature_adjusted_qc,temperature_adjusted_error,salinity,salinity_qc,salinity_adjusted,salinity_adjusted_qc,salinity_adjusted_error) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)" % (argo_levels_table,profileid,data['pressure'][p][z],data['pressure_qc'][p][z],data['pressure_adjusted'][p][z],data['pressure_adjusted_qc'][p][z],data['pressure_adjusted_error'][p][z],data['temperature'][p][z],data['temperature_qc'][p][z],data['temperature_adjusted'][p][z],data['temperature_adjusted_qc'][p][z],data['temperature_adjusted_error'][p][z],data['salinity'][p][z],data['salinity_qc'][p][z],data['salinity_adjusted'][p][z],data['salinity_adjusted_qc'][p][z],data['salinity_adjusted_error'][p][z])
                        #values=[]
                        #for f in row_fields: values.append(data[f][p][z])
                        #    print f
                        query="INSERT INTO %s(profilelnk,%s) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)" % (argo_levels_table,string.join(row_fields,','),profileid,data['pressure'][p][z],data['pressure_qc'][p][z],data['pressure_adjusted'][p][z],data['pressure_adjusted_qc'][p][z],data['pressure_adjusted_error'][p][z],data['temperature'][p][z],data['temperature_qc'][p][z],data['temperature_adjusted'][p][z],data['temperature_adjusted_qc'][p][z],data['temperature_adjusted_error'][p][z],data['salinity'][p][z],data['salinity_qc'][p][z],data['salinity_adjusted'][p][z],data['salinity_adjusted_qc'][p][z],data['salinity_adjusted_error'][p][z])
                        curs.execute(query.replace('--',"NULL").replace('=NULL',' is NULL'))
                conn.commit()
                # I'm not sure if this is the best solution.n

                #  Maybe only for now, for the big first time run. And after
                #  that, only on the final end of the loop.
                #curs.execute("SELECT cleanargo();")
                curs.execute("SELECT argo.cleanargorealtime()")
                conn.commit()
            except:
                print "Problems to insert data from the file %s" % basename
                print "QUERY: %s" % query
                conn.rollback()
                raise
    #curs.execute("SELECT cleanargo();")
    curs.execute("SELECT argo.cleanargorealtime()")
    conn.commit()
    return

def update():
    """ Update the local ARGO DB
    """

    urls=dap_argo_urls()
    argo_catalog(urls)
    return


def matchup(table,table_buddy_argo,dt_limit=(5*24),dL_lim=100e3):
    """

        !!!ATENTION!!! I restricted to qc=1, which means good data only. Think more about that. Maybe include the flag 2 too, but that means probably right. Actually I'll use 1 and 2 now.
    """
    #table='delayedmode.explorerofseas'
    #table_buddy_argo = 'delayedmode.explorerofseas_buddy_argo'

    import numpy

    dbname = 'tsg'
    dbuser='tsg'
    from tsgsql import TSGSQL
    db = TSGSQL(dbname=dbname, dbuser=dbuser)

    from datetime import timedelta

    from fluid.common.distance import distance

    # !!!ATENTION!!! A raw solution
    ddeg = dL_lim/1856./60.
    dt_limit = timedelta(hours=dt_limit)

    # ========================================================================
    query="SELECT DISTINCT extract(year from datetime) FROM %s" % (table)
    #years = db.generalselect(query)
    years = db.generalselect(query,'column')[0]
    for year in years:
        query="SELECT MIN(latitude),MAX(latitude),MIN(longitude),MAX(longitude) FROM %s WHERE datetime>%i AND datetime<%i;" % (table,int(year),int(year)+1)
        limits = db.generalselect(query)[0]
        # Tosco!!!! Improve it. Don't use 15 days hard encoded!
        query_profile = "SELECT * FROM argo_profile WHERE datetime>='%s-12-15' AND datetime<'%s-01-15' AND latitude>%f AND latitude<%f AND longitude>%f AND longitude<%f" % (int(year)-1,int(year)+1,limits[0]-ddeg,limits[1]+ddeg,limits[2]-ddeg,limits[3]+ddeg)
        query_levels = "SELECT * FROM argo_levels WHERE pressure<=10 AND (pressure_qc=1 OR pressure_qc=2) AND (temperature_qc=1 OR temperature_qc=2) AND (salinity_qc=1 OR salinity_qc=2)"
        query = "SELECT p.id,p.datetime,p.latitude,p.longitude,l.pressure,l.temperature, l.salinity FROM (%s) AS p JOIN (%s) AS l ON (p.id=l.profilelnk)" % (query_profile,query_levels)
        possible_argo_match = db.generalselect(query)
        if possible_argo_match != None:
            for argo in possible_argo_match:
                #print "argo: ",argo
                query="SELECT * from %s WHERE datetime>'%s' AND datetime<'%s' AND latitude>%s AND latitude<%s AND longitude>%s AND longitude<%s" % (table,argo[1]-dt_limit,argo[1]+dt_limit,argo[2]-ddeg,argo[2]+ddeg,argo[3]-ddeg,argo[3]+ddeg)
                data_matched= db.generalselect(query,'column')
                if data_matched!=None:
                    data={'argoid':numpy.array(data_matched[0]),'datetime':numpy.array(data_matched[1]),'latitude':numpy.array(data_matched[2]),'longitude':numpy.array(data_matched[3]),'temperature':numpy.array(data_matched[5]),'salinity':numpy.array(data_matched[8])}
                    output={}
                    # ==== Closest in time ===================================
                    #index=(data['datetime']-argo[1])==(data['datetime']-argo[1]).min()
                    index=numpy.array([t.days*24+round(t.seconds/3600.) for t in (data['datetime']-argo[1])]).argmin()
                    dt=(data['datetime']-argo[1])[index]
                    output['dt_time']=dt.days*24+round(dt.seconds/3600.)
                    output['dL_time']=round(distance(argo[2],argo[3],data['latitude'][index],data['longitude'][index])*1e-3)
                    output['datetime_time']=data['datetime'][index]
                    output['latitude_time']=data['latitude'][index]
                    output['longitude_time']=data['longitude'][index]
                    output['temperature_time']=data['temperature'][index]
                    output['dtemp_time']=argo[5]-data['temperature'][index]
                    output['salinity_time']=data['salinity'][index]
                    output['dsal_time']=argo[6]-data['salinity'][index]
                    # ==== Closest in dist ===================================
                    index=(distance(data['latitude'],data['longitude'],argo[2],argo[3])).argmin()
                    dt=(data['datetime'][index]-argo[1])
                    output['dt_dist']=dt.days*24+round(dt.seconds/3600.)
                    output['dL_dist']=round(distance(argo[2],argo[3],data['latitude'][index],data['longitude'][index])*1e-3)
                    output['datetime_dist']=data['datetime'][index]
                    output['latitude_dist']=data['latitude'][index]
                    output['longitude_dist']=data['longitude'][index]
                    output['temperature_dist']=data['temperature'][index]
                    output['dtemp_dist']=argo[5]-data['temperature'][index]
                    output['salinity_dist']=data['salinity'][index]
                    output['dsal_dist']=argo[6]-data['salinity'][index]
                    # ==== Median ============================================
                    #dt
                    #dl
                    output['n']=data['datetime'].size 
                    output['dL'],output['dL_pstd']=make_stats(distance(data['latitude'],data['longitude'],argo[2],argo[3]))
                    #output['dt'],output['dt_pstd']=make_stats([d.days*24*60*60+d.seconds for d in (data['datetime']-data['datetime'].min())])
                    output['dt'],output['dt_pstd']=make_stats([d.days*24+(d.seconds/3600.) for d in (data['datetime']-argo[1])])
                    #output['datetime']=data['datetime'].min()+timedelta(seconds=output['dt'])
                    output['datetime']=argo[1]+timedelta(hours=output['dt'])
                    output['latitude'],output['latitude_pstd']=make_stats(data['latitude'])
                    output['longitude'],output['longitude_pstd']=make_stats(data['longitude'])
                    output['temperature'],output['temperature_pstd']=make_stats(data['temperature'])
                    output['dtemp']=argo[5]-output['temperature']
                    output['salinity'],output['salinity_pstd']=make_stats(data['salinity'])
                    output['dsal']=argo[6]-output['salinity']
                    #insert = "INSERT INTO %s(argo_id) VALUES(%i);" % (table_buddy_argo,data_matched[0][0])
                    insert = "INSERT INTO %s(argo_profile_id,dt_time,dL_time,datetime_time,latitude_time,longitude_time,temperature_time,dtemp_time,salinity_time,dsal_time,dt_dist,dL_dist,datetime_dist,latitude_dist,longitude_dist,temperature_dist,dtemp_dist,salinity_dist,dsal_dist,n,datetime,dt,dt_pstd,latitude,latitude_pstd,longitude,longitude_pstd,temperature,temperature_pstd,dtemp,salinity,salinity_pstd,dsal) VALUES(%i,%i,%i,'%s',%f,%f,%f,%f,%f,%f,%i,%i,'%s',%f,%f,%f,%f,%f,%f,%i,'%s',%i,%i,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f)" % (table_buddy_argo,argo[0],output['dt_time'],output['dL_time'],output['datetime_time'],output['latitude_time'],output['longitude_time'],output['temperature_time'],output['dtemp_time'],output['salinity_time'],output['dsal_time'],output['dt_dist'],output['dL_dist'],output['datetime_dist'],output['latitude_dist'],output['longitude_dist'],output['temperature_dist'],output['dtemp_dist'],output['salinity_dist'],output['dsal_dist'],output['n'],output['datetime'],int(output['dt']),int(output['dt_pstd']),output['latitude'],output['latitude_pstd'],output['longitude'],output['longitude_pstd'],output['temperature'],output['temperature_pstd'],output['dtemp'],output['salinity'],output['salinity_pstd'],output['dsal'])
                    print "argo: ",argo
                    print "output: ",output
                    print "insert: ",insert
                    db.generalinsert(insert)
                    #import pylab
                    #pylab.plot(data['longitude'],data['latitude'],'b.')
                    #pylab.plot([argo[3]],[argo[2]],'rx')
                    #pylab.plot([output['longitude_time']],[output['latitude_time']],'ro')
                    #pylab.plot([output['longitude_dist']],[output['latitude_dist']],'rs')
                    #pylab.show()

def make_stats(data):
    """
        !!!ATENTION!!!! think about it
          In a case of a non regular track or even two tracks inside the same 
            time x space window, a bimodal dataset wouldn't be a surprise. On this case
            if I use the median, it would jump for the bigger mode.
    """
    import numpy
    #import rpy
    #data[var]['n'] = data[var]['data'].size
    mean = numpy.mean(data)
    std = numpy.std(data)
    #median = rpy.r.median(data)
    #quantile=rpy.r.quantile(data)
    #pstd=(quantile['75%']-quantile['25%'])/1.349
    #return median,pstd
    return mean, std

def grab_argo_data(ship,urls,dtwin=None,dLwin=1e5,verbose=False):
    """
        !!!ATENTION!!!!!
        This function is deprecated. Is more efficient to simply grab all data, 
          and maintain a local ARGO DB.


        Grab ARGO data pertinent to TSG dataset

        Check on the available files at the Coriolis DAP server for samples inside
          a defined timeXspace window in respect to the TSG dataset.

        I'm still not sure if this would be the best solution. Before I was taking
          each TSG sample and then look for something in the ARGO DAP dataset, but
          it's not efficient, since close sequential TSG data should accept the
          same ARGO data, plus the fact that ARGO data from one certain day could
          be spread along files in different days. I think that read once all the
          ARGO data is the most efficient way.

        This need to be improved.
    """
    dbname = 'tsg_test'
    dbuser='tsg_test'
    ship_table = 'realtime.millerfreeman_raw'
    from tsgsql import TSGSQL
    db = TSGSQL(dbname=dbname, dbuser=dbuser)

    import dap.client
    import string
    import time
    from datetime import datetime
    from datetime import timedelta

    if dtwin == None:
        dtwin = timedelta(days=10)

    dLwin = dLwin/1856./60. # 100Km / 1 NM / 60 minutes ~= degrees

    #
    for url in urls:
        print "Openning url: %s" % url
        try:
            dataset=dap.client.open(url)
            #
            ref_string_date = string.join(dataset.REFERENCE_DATE_TIME[:],'')
            date_ref = datetime.strptime(ref_string_date,"%Y%m%d%H%M%S")
            #
            n=dataset.JULD[:].size
            #
            #import sql
            for i in range(n):
                t = date_ref+timedelta(days=dataset.JULD[i])
                x = dataset.LONGITUDE[i]
                y = dataset.LATITUDE[i]
                if verbose == True:
                    print i,t,x,y
                query = "SELECT id from %s where datetime>\'%s\' and datetime<\'%s\' AND longitude>%f AND longitude<%f AND latitude>%f AND latitude<%f" % (ship_table,t-dtwin,t+dtwin,x-dLwin,x+dLwin,y-dLwin,y+dLwin)
                data=db.generalselect(query)
                if data is not None:
                    print "Yes, I got something"
                    print i,t,x,y
                    #if (dataset.PRES_QC[i,0][0][0]=='1') & (dataset.PSAL_QC[i,0][0][0]=='1') & (dataset.TEMP_QC[i,0][0][0]=='1'):
                    #    if verbose==True:
                    #        print "Argo Q.C.s: %s, %s, %s" % (dataset.PRES_QC[i,0][0][0],dataset.PSAL_QC[i,0][0][0],dataset.TEMP_QC[i,0][0][0])
                    data['platform']=int(string.join(dataset.PLATFORM_NUMBER[i],''))
                    data['date_creation']=datetime.strptime(string.join(dataset.DATE_CREATION[:],''),"%Y%m%d%H%M%S")
                    data['date_update']=datetime.strptime(string.join(dataset.DATE_UPDATE[:],''),"%Y%m%d%H%M%S")
                    data['data_mode']=dataset.DATA_MODE[i]
                    data['datetime']=t
                    data['datetime_qc']=dataset.JULD_QC[i]
                    data['latitude'] = y
                    data['longitude'] = x
                    data['position_qc'] = dataset.POSITION_QC[i]
                    data['pressure'] = dataset.PRES[i][0]
                    data['pressure_qc'] = dataset.PRES_QC[i][0]
                    data['pressure_adjusted'] = dataset.PRES_ADJUSTED[i][0]
                    data['pressure_adjusted_qc'] = dataset.PRES_ADJUSTED_QC[i][0]
                    data['pressure_adjusted_error'] = dataset.PRES_ADJUSTED_ERROR[i][0]
                    data['temperature'] = dataset.TEMP[i][0]
                    data['temperature_qc'] = dataset.TEMP_QC[i][0]
                    data['temperature_adjusted'] = dataset.TEMP_ADJUSTED[i][0]
                    data['temperature_adjusted_qc'] = dataset.TEMP_ADJUSTED_QC[i][0]
                    data['temperature_adjusted_error'] = dataset.TEMP_ADJUSTED_ERROR[i][0]
                    data['salinity'] = dataset.PSAL[i][0]
                    data['salinity_qc'] = dataset.PSAL_QC[i][0]
                    data['salinity_adjusted'] = dataset.PSAL_ADJUSTED[i][0]
                    data['salinity_adjusted_qc'] = dataset.PSAL_ADJUSTED_QC[i][0]
                    data['salinity_adjusted_error'] = dataset.PSAL_ADJUSTED_ERROR[i][0]
                    #t_update=datetime(*time.strptime(string.join(dataset.DATE_UPDATE[:],''),"%Y%m%d%H%M%S")[:6])
                    #
                    #query = "INSERT INTO buddy_argo(platform,datetime,lat,lon,temperature,salinity,pressure,mode) VALUES(%s,\'%s\',%s,%s,%s,%s,%s,\'%s\')" % (platform,t.strftime("%Y-%m-%d %H:%M:%S"),x,y,temp,psal,pres,mode)
                    query = "INSERT INTO %s(platform,date_creation,date_update,data_mode,datetime,datetime_qc,latitude,longitude,position_qc,pressure,pressure_qc,pressure_adjusted,pressure_adjusted_qc,pressure_adjusted_error,temperature,temperature_qc,temperature_adjusted,temperature_adjusted_qc,temperature_adjusted_error,salinity,salinity_qc,salinity_adjusted,salinity_adjusted_qc,salinity_adjusted_error) VALUES(%s,\'%s\',\'%s\',\'%s\',\'%s\',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);" % ('buddy_argo2',data['platform'],data['date_creation'],data['date_update'],data['data_mode'],data['datetime'],data['datetime_qc'],data['latitude'],data['longitude'],data['position_qc'],data['pressure'],data['pressure_qc'],data['pressure_adjusted'],data['pressure_adjusted_qc'],data['pressure_adjusted_error'],data['temperature'],data['temperature_qc'],data['temperature_adjusted'],data['temperature_adjusted_qc'],data['temperature_adjusted_error'],data['salinity'],data['salinity_qc'],data['salinity_adjusted'],data['salinity_adjusted_qc'],data['salinity_adjusted_error'])
                    #query=db.set_INSERT('buddy_argo',data)
                    db.generalinsert(query)

                    #    if verbose == True:
                    #        print query
                    #    sql.generalinsert(query)

        except:
            if verbose==True:
                print "Oh oh, problems at: %s" % url
            pass


    return
