#!/usr/bin/env python

# -*- coding: Latin-1 -*-
# vim: tabstop=4 shiftwidth=4 expandtab

""" A library to load data from the NODC into the psql

    !!!!ATENTION!!! Improve it. Load the data using MA, then use the masks to minmize the inserts.
"""

__author__ = "Guilherme Castelao"


import string
from datetime import datetime
from datetime import timedelta
import re
import shutil
import urllib
import tempfile
import os.path
import logging
import tarfile

import numpy
from numpy import ma
from pupynere import netcdf_file

import psycopg2


def nocd_ncfile_decode(filename):
    """ Decode a NetCDF ARGO file from NODC
    """
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


def get_nodc_filelist(url_base):
    """
    """
    f = urllib.urlopen(url_base)
    text = f.read()
    f.close()
    import pdb; pdb.set_trace()
    tarfiles = re.findall(r"<a href=\"(.*)\">argo_\S{2}\d+\.tgz</a>", text)
    return tarfiles

def download_nodc_file(tgzf, url_base="http://data.nodc.noaa.gov/opendap/argo/data/"):
    """ Download the tar.gz files from nodc
    """
    tgz = urllib.urlopen(url_base+tgzf)
    dtmp = tempfile.mkdtemp()
    try:
        f = open(os.path.join(dtmp,tgzf),'w')
        f.write(tgz.read())
        f.close()
        return dtmp
    except:
        shutil.rmtree(dtmp)

def extract_targz(dtmp, tgzf):
    """Extract the tgzf inside the dtmp

       In the future I shouldn't extract everything into files,
         but walk into them in the memory, and use the physical
         disc only for the files that I need to handle.
    """
    tar = tarfile.open(os.path.join(dtmp,tgzf))
    try:
        tar.extractall(path=dtmp)
        argo_files = []
        for root, dirs, files in os.walk(dtmp):
            for file in files:
                if re.match('.*\.nc',file):
                    argo_files.append( os.path.join(root,file))
        return argo_files
    except:
        print "Unfinished function. Something went wrong. I should the delete the files that I unpacked"


class Load_nodc(object):
    """ A class to load data from the NODC into psql
    """
    def __init__(self, cfg=None):
        """
        """
        self.cfg = cfg
        self.set_defaults()

        self.set_db_conn()

        self.tarfiles = get_nodc_filelist(self.cfg['url_base'])

        # Temporary solution to reduce loads for testing purposes.
        from numpy.random import permutation
        import pdb; pdb.set_trace()
        self.filter_tarfiles()
        self.tarfiles = (numpy.array(self.tarfiles)[permutation(len(self.tarfiles))[:5]]).tolist()

        for tgzf in self.tarfiles:
            dtmp = download_nodc_file(tgzf)
            argo_files = extract_targz(dtmp, tgzf)

            results = []
            for argo_nc in argo_files:
                results.append(self.insert_nc(argo_nc))

            print "Results: ", results

            shutil.rmtree(dtmp)
            print "Deleted: %s" % dtmp

            curs = self.conn.cursor()
            curs.execute("SELECT argo.cleanargorealtime()")
            self.conn.commit()


    def set_defaults(self):
        print "Setting default config values"
        if self.cfg == None:
            self.cfg = {}
        if 'url_base' not in self.cfg:
            self.cfg['url_base'] = "http://data.nodc.noaa.gov/opendap/argo/data/"
        self.argo_files_table = "argo.files"
        self.argo_profile_table = "argo.profile"
        self.argo_levels_table = "argo.levels"

        print "Default cfg: ", self.cfg

    def set_db_conn(self):
        """
        """
        dsn='dbname=%s user=%s host=%s password=%s' % \
                (self.cfg['dbname'], self.cfg['dbuser'], self.cfg['dbhost'], self.cfg['dbpassword'],)
        self.conn = psycopg2.connect(dsn)

    def filter_tarfiles(self):
        """
        """
        toclean = []
        for tarfile in self.tarfiles:
            query = "SELECT id FROM %s WHERE basename='%s'" % (self.argo_files_table, tarfile)
            curs = self.conn.cursor()
            curs.execute(query)
            if curs.rowcount == 1:
                print "%s was already inserted. I'll ignore it." % tarfile
                toclean.append(tarfile)
            elif curs.rowcount > 1:
                print "OOppss, looks like this file were included more then once before. Sounds like a problem!"

        for tarfile in toclean:
                self.tarfiles.remove(tarfile)

    def insert_nc(self, argo_nc):
        """
        """
        basename = os.path.basename(argo_nc)
        query = "SELECT id FROM %s WHERE basename='%s'" % \
                (self.argo_files_table, basename)
        try:
            curs = self.conn.cursor()
            curs.execute(query)
        except:
            return False
        if curs.rowcount == 1:
            print "%s was already inserted. I'll not ignore this files." % argo_nc
            return True
        elif curs.rowcount > 1:
            print "OOppss, looks like this file were included more then once before. Sounds like a problem!"
            return False
    
        try:
            print "I'll process %s" % argo_nc
            data = nocd_ncfile_decode(argo_nc)
            # ============================================================
            # Storing information about the data file.
            tnow = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-4]
            #query="INSERT INTO %s(basename,date_creation,date_update) VALUES('%s','%s','%s')" % (argo_files_table,basename,data['date_creation'],data['date_update'])
            query="INSERT INTO %s(basename, db_insertion) VALUES('%s','%s') RETURNING %s.id" % \
                    (self.argo_files_table, basename, tnow, self.argo_files_table)
            curs.execute(query)
            fileid=curs.fetchone()[0]

            #query="INSERT INTO %s (filelnk,date_creation,date_update,platform,cycle,data_centre,dc_reference,data_mode,datetime,datetime_qc,inst_reference,latitude,longitude,location,position_qc) VALUES(%s,'%s','%s',%s,%s,'%s','%s','%s','%s',%s,'%s',%s,%s,'(%s,%s)',%s)"  % (argo_profile_table,fileid,data['date_creation'],data['date_update'],data['platform'],data['cycle'],data['data_centre'],data['dc_reference'],data['data_mode'],data['datetime'],data['datetime_qc'],data['inst_reference'],data['latitude'],data['longitude'],data['longitude'],data['latitude'],data['position_qc'])
            query="INSERT INTO %s (filelnk,date_creation,date_update,platform,cycle,data_centre,dc_reference,data_mode,datetime,datetime_qc,inst_reference,latitude,longitude,location,position_qc) VALUES(%s,'%s','%s',%s,%s,'%s','%s','%s','%s',%s,'%s',%s,%s, ST_GeomFromText('POINT(%s %s)', 4326),%s) RETURNING %s.id"  % (self.argo_profile_table,fileid,data['date_creation'],data['date_update'],data['platform'],data['cycle'],data['data_centre'],data['dc_reference'],data['data_mode'],data['datetime'],data['datetime_qc'],data['inst_reference'],data['latitude'],data['longitude'],data['longitude'],data['latitude'],data['position_qc'], self.argo_profile_table)
            #query="INSERT INTO %s (filelnk,date_creation,date_update,platform,cycle,data_centre,dc_reference,data_mode,datetime,datetime_qc,inst_reference,latitude,longitude,position_qc) VALUES(%s,'%s','%s',%s,%s,'%s','%s','%s','%s',%s,'%s',%s,%s,%s)"  % (argo_profile_table,fileid,data['date_creation'],data['date_update'],data['platform'],data['cycle'],data['data_centre'],data['dc_reference'],data['data_mode'],data['datetime'],data['datetime_qc'],data['inst_reference'],data['latitude'],data['longitude'],data['position_qc'])
            query=query.replace("'(--,--)'","NULL").replace('--',"NULL")
            curs.execute(query)
            profileid = curs.fetchone()[0]

            row_fields = ['pressure','pressure_qc','pressure_adjusted','pressure_adjusted_qc','pressure_adjusted_error','temperature','temperature_qc','temperature_adjusted','temperature_adjusted_qc','temperature_adjusted_error','salinity','salinity_qc','salinity_adjusted','salinity_adjusted_qc','salinity_adjusted_error']
            for z in numpy.arange(data['pressure'].size): #[ind_good_level]:
                fields = []
                values = []
                for field in row_fields:
                    if field in data.keys():
                        fields.append(field)
                        values.append(data[field][z])
                query = "INSERT INTO %s(profilelnk,%s) VALUES(%s,%s)" % (self.argo_levels_table, string.join(fields,","), profileid, string.join([str(v) for v in values],","))
                query = query.replace('--',"NULL").replace('=NULL',' is NULL')
                curs.execute(query)
            self.conn.commit()

            return True
        except:
            print "Problems to insert data from the file %s" % basename
            print "QUERY: %s" % query
            self.conn.rollback()
            #raise

            return False

def dap_argo_urls_coriolis(dia=None,maxdt=None,ini_date=None,fin_date=None):
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




cfg={'dbname':'zen_test', \
        'dbuser' : 'elbonian', \
        'dbpassword' : 'minhoca', \
        'dbhost' : 'localhost'
        }


if __name__ == '__main__':
    x = Load_nodc(cfg=cfg)
