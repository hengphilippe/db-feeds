## Handle Operation for Configuration
import json

class Connector:
  def __init__(self, filename = 'Configuration/connector.json') -> None:
    self.fileName = filename
    with open(self.fileName,"r") as data : 
      self.config_data = json.load(data)
    data.close() 

  def getTables(self,connectorName) :
    return self.config_data[connectorName]["tables"]

  def getTableZeroSCN(self, connectorName) :
    tables = self.getTables(connectorName)
    tblFresh = []
    for table in tables :
      if(int(table['SCN']) == 0) :
        tblFresh.append(table['name'])
    return tblFresh

  def getInitConnector(self) : 
    for connector in self.config_data :
      freshTable = self.getTableZeroSCN
      if(len(freshTable) > 0) :
        return 
      print(connector);  

  def setTableSCN(self, connectorName, tablename, scn) -> None :

    for table in self.config_data[connectorName]['tables'] :
      # print(table['name'], "->" , tablename)
      if(str(table['name']) == tablename) :
        table['SCN'] = scn
      else :
        print("something wrong with tablename")
    with open(self.fileName,"w") as jsonfile : 
      new_config_data = json.dump(config_data, jsonfile)
      print("scn updated.")
      jsonfile.close()

if __name__ == "__main__": 
  connect = Connector();
  connect.getInitConnector()