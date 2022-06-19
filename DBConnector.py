# Standard Library
import json
import logging as logger
import time

# Third Party Libraries
import cx_Oracle

## Message Model
from Producer.message import Message 

class DBConnector :
  def __init__(self,name,dsn,username,passwd) -> None:
    self.name = name
    self.dsn = dsn
    self.username = username
    self.passwd = passwd
    self.encoding = "UTF-8"
    try: 
      self.pool = cx_Oracle.SessionPool(
        self.username,
        self.passwd,
        self.dsn,
        min=2,
        max=5,
        increment=3,
        getmode=cx_Oracle.SPOOL_ATTRVAL_TIMEDWAIT,
        encoding=self.encoding
      )
      # Acquire a connection from the pool
      connection = self.pool.acquire()
      self.connection = connection
      print("connected to : ",connection.version)
      logger.info(f"Connected to {connection.version} database")
    except cx_Oracle.DatabaseError as e:
      print("There is a problem with Oracle", e)
    finally:
      print("init session...")

  def query(self, sql, bind_variables=None) :
        try:
            conn = self.pool.acquire()
            cursor = conn.cursor()
            if bind_variables is None:
                logger.debug(f"Query: {sql}")
                cursor.execute(sql)
            else:
                logger.debug(f"Query: {sql} value: {bind_variables}")
                cursor.execute(sql, bind_variables)
            columns = [description[0] for description in cursor.description]
            results = []
            for row in cursor:
                results.append(dict(zip(columns, row)))
            self.pool.release(conn)
            return results
        except Exception as err:
            logger.exception(err) 

  def getCurrentSCN(self) :
    if(self.connection) :
      conn = self.pool.cursor()
      cursor = conn.cursor()
      query = cursor.execute('select current_scn from v$database')
      result = query.fetchone()
    return result[0]

  def dumpTable(self, table) :
    records=[]
    getTable = table.split(".")[1]
    getSegOwner = table.split(".")[0]

    conn = self.pool.acquire()
    cursor = conn.cursor()
    cursor.prefetchrows = 2500
    cursor.arraysize = 3000
    sql = 'SELECT * FROM (select * from {0}) WHERE rownum <= 20'.format(table)
    try:
      query = cursor.execute(sql)
      res = cursor.fetchall()

      # Get header 
      row_headers=[column[0] for column in query.description]
      # Get records
      for row in res :
        record = dict(zip(row_headers,row))

        message = Message(
        scn=0,
        seg_owner=getSegOwner,
        table_name=getTable,
        sql_redo=sql,
        operation="READ",
        data=record,
        timestamp=int(time.time()))
        ## add all to records
        records.append(message.dict())

      # Return all records 
      return records
      # return json.dumps(records,default=str)
    except Exception as err:
      logger.exception(err)