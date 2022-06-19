import logging as logger
import json

## Configuration
from Configuration import ConfigHandler

## Database 
from DBConnector import DBConnector

if __name__ == "__main__":
  connector = ConfigHandler.Connector()
  # connector = config.getConfiguration()

  asywdb = connector.config_data['ASYWDB']
  ecusdb = connector.config_data['ECUSDB']
  asywdbConn = DBConnector(asywdb["name"],
                                asywdb["dsn"],
                                asywdb["username"],
                                asywdb["password"])

  ecusdbConn = DBConnector(ecusdb["name"],
                              ecusdb["dsn"],
                              ecusdb["username"],
                              ecusdb["password"])
  #### get table with zero SCN
  ########################
  freshSync_asywdb = connector.getTableZeroSCN("ASYWDB")
  freshSync_ecusdb = connector.getTableZeroSCN("ECUSDB")

  ## fresh read all record 
  ########################
  if(len(freshSync_asywdb) > 0) :
    for table in freshSync_asywdb :
      print("---------------------------")
      print("🛑", table)
      data = asywdbConn.dumpTable(table)
      print(json.dumps(data, default=str))

  ## fresh read all record 
  ########################
  if(len(freshSync_ecusdb) > 0) :
    for table in freshSync_ecusdb :
      print("---------------------------")
      print("🛑", table)
      data = ecusdbConn.dumpTable(table)
      print(json.dumps(data, default=str))