#!/usr/bin/env python
# -*- coding: Latin-1 -*-
# vim: tabstop=4 shiftwidth=4 expandtab

#import psycopg
#import psycopg2

# ==== Global Variables ================
#dbname = 'tsg_test'
#dbuser = 'castelao'
# ====

""" ATENTION !!!!!

    Need to work on scape characters. I'm not sure if it should be here, or
      on the data entries, like the email parser, or both?!
      Remember that the DYSON's on the subject is a problem example for that
"""

class TSGSQL:
    """Class to deal with PostgreSQL data
    """
    def __init__(self,dbname,dbuser):
        #import psycopg2
        #dsn='dbname=%s user=%s' % (dbname,dbuser)
        #self.conn = psycopg2.connect(dsn)
        self.set_conn(dbname,dbuser)

    def set_conn(self,dbname,dbuser):
        import psycopg2
        dsn='dbname=%s user=%s' % (dbname,dbuser)
        self.conn = psycopg2.connect(dsn)
    # --------------------------------------------------------------------
    def set_INSERT(self,table,data,cols=None):
        """Create a query and the data list
        table => Where to insert
        data    => dictionary where the headers are equal to the 
            columns on the destinity table
        cols    => headers to be inserted

        ATENTION!!! IMPROVE IT:
        - set a check if all columns have the same number of values
        - mount the columns and values list with string.join(x,", "), is
            more elegant
        - A better way to deal with string fields. The problem is that they
            are interactable as are the lists.
        """

        # If cols aren't defined, use all headers of the dictionary data.
        if (cols is None):
            cols=data.keys()
        else:
            # Check if all the asked columns are headers on the dictionary data.
            for col in cols:
                if col not in data.keys():
                    print "There is no %s key in the data dictionary" % col
                    return

        # ---- Creates the query call
        query = "INSERT INTO %s" % (table)

        columns='('
        values='VALUES('
        # Insert the columns
        for col in cols:
            if data[col]!=None:
                columns += '%s,' % col
                values += '%s,'
        columns=columns[:-1]+')'
        values=values[:-1]+')'

        query = query + columns +' '+values
        
        # ---- Creates the data list
        query_data=[]
        # Bellow is an ugly solution when have a string field.
        #if (type(data[cols[0]]) == list) | (type(data[cols[0]]) == numpy.ndarray):
        if (type(data[cols[0]]) == list):
            n=len(data[cols[0]])
            for i in range(n):
                tmp=[]
                for col in cols:
                    #tmp.append("%s" % data[col][i])
                    if (type(data[col])==str) | (type(data[col])==int):
                        tmp.append(data[col])
                    elif data[col]!=None:
                        tmp.append(data[col][i])
                query_data.append(tuple(tmp))
        else:
            tmp=[]
            for col in cols:
                #tmp.append("%s" % data[col][i])
                #tmp.append(data[col])
                #if (type(data[col])==str) | (type(data[col])==int):
                #    tmp.append(data[col])
                if data[col]!=None:
                    tmp.append(data[col])
            query_data.append(tuple(tmp))
        return query,query_data

    # --------------------------------------------------------------------
    def insert_query(self,query,query_data):
        """
        """
        curs = self.conn.cursor()
        curs.executemany(query,query_data)
        self.conn.commit()

        return

    def generalselect(self,query,output=None):
        """
        """
        curs = self.conn.cursor()
        curs.execute(query)
        if curs.rowcount<1:
            return
        data=curs.fetchall()

        #col = [c[0] for c in curs.description] 
        #col = dict(zip(col, range(len(col)))) 
        if output=='column':
            data=[[row[i] for row in data] for i in range(len(curs.description))]

        return data


    def generalinsert(self,query):
        """
        """
        curs = self.conn.cursor()
        curs.execute(query)
        self.conn.commit()
        return
  
    def generalupdate(self,query):
        """
        """
        curs = self.conn.cursor()
        curs.execute(query)
        self.conn.commit()
        return
  
    def get_id(self,table,data,columns=None):
        """Temporary solution!!!! Improve it!

           ATENTION!!! Now only accepts one id per time, in despite the
             result is a tuple.
        """
        import string
        if columns == None:
            columns = data.keys()

        curs = self.conn.cursor()
        
        query_base = "SELECT id from %s" % (table)
        conditions = []
        #for d,t in zip(data["Date"],data["Time"]):
        for c in columns:
            #query = "%s WHERE date = '%s' AND time = '%s'" % (query_base,d,t)
            if c in data.keys():
                if data[c] == None:
                    conditions.append("%s is NULL" % (c))
                else:
                    conditions.append("%s = '%s'" % (c,data[c]))
            else:
                conditions.append("%s is NULL" % (c))
        query_cond = " WHERE %s" % string.join(conditions," AND ")
        query = query_base+query_cond
        curs.execute(query)
        id=curs.fetchone()
        if id is None:
            insert_columns=[]
            for col in columns:
                if col in data.keys():
                    insert_columns.append(col)
            if len(insert_columns)==0:
                return 
            [insert_query,insert_data] = self.set_INSERT(table,data,insert_columns)
            self.insert_query(insert_query,insert_data)
            curs.execute(query)
            id=curs.fetchone()

        return id[0]
    

    #def set_SELECT(table,cond=None):
    #    """
    #    """
    #
    #    query = "SELECT * from %s" % (table)
    #
    #    #if (cond!=None):
    #        #query =+" WHERE"
    #        #cols=cond.keys()
    #
    #        #for col in cols:

    def makedatetime(self,table,datecol="date",timecol="time",datetimecol="datetime",verbose=False):
        """Fullfill the column datetime based on the strings on the columns date
              and time.

           !!!!!!!! ATENTION !!!!! Don't forget to develop the error treatment
        """
        curs = self.conn.cursor()

        query="SELECT id, %s, %s FROM %s WHERE %s IS NULL AND date IS NOT NULL AND time IS NOT NULL" % (datecol,timecol,table,datetimecol)
        curs.execute(query)
        #rows = curs.fetchall()

        if verbose==True:
            print "Defining dates for %i records on table %s" % (curs.rowcount,table)
        
        import time
        data_datetime_flags_bad=[]
        #for row in rows:
        for i in range(curs.rowcount):
            row=curs.fetchone()
            id=row[0]
            d=row[1].strip()
            t=row[2].strip()
            #try:
            query="UPDATE %s SET %s='%s' where id=%s" % (table,datetimecol,"%s %s" % (d,t),id)
            self.generalupdate(query)
            #curs.execute(query)
            #self.conn.commit()
            #except:
            #self.conn.rollback()
            data_datetime_flags_bad.append(['id',False])
        return

    def makespeed(self,table):
        """
            !!! ATENTION !!!!
              Splited and work in blocks to avoid memory blow up. But there is a
              conceptual error here. If there is a block of more then n_at_once
              with one of the pertinent veriables None
        """
        import fluid.common.distance
        import numpy

        # First check if the oldest available data has a True flag. This is a query that should be important once on a ship table life time. I Should think a more efficient way, but at least is a stable solution without a really big demmand.
        #query = "SELECT possible_speed FROM %s_qc WHERE datetime=(SELECT min(datetime) FROM %s);" % (table,table)
        #if (self.generalselect(query,'column')[0][0] == None):
        #    self.logger.warn("The oldest register from %s, have a NULL possible_speed flag. You might want to update it to True, since there is no data to flag it as bad.")
        #    return

        #table='realtime.mvexplorer_raw'
        looplimit=1000
        while  looplimit>0:
            #print looplimit
            #query="SELECT min(datetime) from %s WHERE speed IS NULL" % (table)
            #query="SELECT max(datetime) from %s WHERE datetime < (SELECT min(datetime) from %s WHERE speed IS NULL)" % (table,table)
            query="SELECT MAX(t0) AS t0 FROM (SELECT MAX(datetime) AS t0 FROM %s WHERE speed IS NOT NULL UNION SELECT MIN(datetime) AS t0 FROM %s) AS select_t0;" % (table,table)
            curs = self.conn.cursor()
            curs.execute(query)
            t0 = curs.fetchone()[0]
            #print "t0: %s" % t0
            #
            n_at_once=10000
            query = "SELECT * FROM %s WHERE datetime >= '%s' ORDER BY datetime LIMIT %i" % (table,t0,n_at_once)
            query = "SELECT id, extract(epoch from datetime) AS seconds, latitude, longitude, speed FROM %s WHERE datetime >= '%s' ORDER BY datetime,id LIMIT %i" % (table,t0,n_at_once)
            curs.execute(query)
            #
            #id=numpy.zeros(n_at_once+2)
            #t=numpy.zeros(n_at_once+2,dtype='d')
            #lat=numpy.zeros(n_at_once+2,dtype='d')
            #lon=numpy.zeros(n_at_once+2,dtype='d')
            #speed=numpy.zeros(n_at_once+2,dtype='d')
            #
            n=curs.rowcount
            rows=curs.fetchall()
            #print "n: %s" % n
            if n<=1:
                looplimit=0
            for i in range(n-1):
                if (None not in rows[i][:4]) and (None not in rows[i+1][:4]):
                    dt=rows[i+1][1]-rows[i][1]
                    dx=fluid.common.distance.distance(rows[i][2],rows[i][3],rows[i+1][2],rows[i+1][3])
                    if dx==0:
                        sp=0
                    else:
                        sp=dx/dt
                    if sp != numpy.inf:
                        query = "UPDATE %s SET speed=%2.2f WHERE id=%i" % (table,round(sp,3),rows[i+1][0])
                    #print query
                    self.generalupdate(query)
            looplimit=looplimit-1
        return

        #n_at_once=1000

        #query="SELECT id, extract(epoch from datetime) AS seconds, latitude, longitude, speed FROM %s_qc WHERE datetime IS NOT NULL AND latitude IS NOT NULL AND longitude IS NOT NULL ORDER BY datetime,id;" % (table)
        ## I should move this query to a function inside the psql.
        ##query_startdatetime = "SELECT max(datetime) from %s WHERE datetime<(SELECT min(datetime) FROM %s_qc WHERE possible_speed IS NULL)" % (table,table)
        ##query="SELECT id, extract(epoch from datetime) AS seconds, latitude, longitude, speed FROM %s WHERE datetime >(%s) AND latitude IS NOT NULL AND longitude IS NOT NULL ORDER BY datetime,id;" % (table,query_startdatetime)
        ##print "Query to be done: %s" % query

        #curs = self.conn.cursor()
        #curs.execute(query)
        ##rows = curs.fetchall()

        ##data=self.generalselect(query,'column')
        #id=numpy.zeros(n_at_once+2)
        #t=numpy.zeros(n_at_once+2,dtype='d')
        #lat=numpy.zeros(n_at_once+2,dtype='d')
        #lon=numpy.zeros(n_at_once+2,dtype='d')
        #speed=numpy.zeros(n_at_once+2,dtype='d')

        #rows=curs.fetchmany(2)
        #for block in range(int(numpy.ceil((curs.rowcount-2.)/n_at_once))):
        #    rows[:2]=rows[-2:]
        #    rows[2:]=curs.fetchmany(n_at_once)
        #    for r,row in enumerate(rows):
        #        id[r]=row[0]
        #        t[r]=row[1]
        #        lat[r]=row[2]
        #        lon[r]=row[3]
        #        speed[r]=row[4]

        #    dt=t[1:]-t[:-1]
        #    dx = fluid.common.distance.distance(lat,lon)
        #    sp=dx/dt

        #    #index=(dt!=0)&(speed[1:]!=numpy.nan)
        #    index=(dt!=0)&(dt<(30*60))&(numpy.isnan(speed[1:]))

        #    #print "Speed calculated for %s registers." % len(id[index])

        #    #for i in numpy.arange(1,len(speed))[index]:
        #    for i,s in zip(id[1:][index],sp[index]):
        #        # sp[i-1] because is due the backward diferentiation. 
        #        # So the sp[1]=(x[1]-x[0])/(t[1]-t[0])
        #        query = "UPDATE %s SET speed=%2.2f WHERE id=%i" % (table,s.round(3),i)
        #        self.generalupdate(query)
        #        #curs.execute(query)
        #    #conn.commit()

        #return


def run_analyze():
    """

    Think about it!!!
    """

    import psycopg2.extensions
    conn = database.metadata.engine.connect()
    conn.connection.connection.set_isolation_level 
    (psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    conn.execute('VACUUM FULL ANALYZE;')
    conn.close()

    return
