# -*- coding: utf-8 -*-
"""
Created on Mon Oct 18 20:38:19 2017

@author: Saad
"""

#Libraries

import sqlite3,math,datetime
import pandas,csv
from gurobipy import *
from geopy.distance import vincenty




#------------------------------------------ Question no 1 - Binary Transportation Problem ---------------------------------------------------------------------




#Creating DB file # For Preprocessing We used R to delete tuples from demand data for the stores which weren't avaiable.
db = sqlite3.connect('dominoPizza.db')
print "Pizza DB created successfully"

#Cursor to save data in DB
sqlTableObject = db.cursor()

#SQL Queries to Create Tables Within DB
sqlQueryStoresDistCentersTable = '''DROP TABLE IF EXISTS distCentersTable;CREATE TABLE IF NOT EXISTS distCentersTable (ID INTEGER PRIMARY KEY AUTOINCREMENT, DCENTER TEXT NOT NULL, STADDRESS TEXT, CITY TEXT, STATE TEXT, ZIPCODE TEXT, LATITUDE TEXT,LONGITUDE TEXT, WEEKLYSUPCAP FLOAT, REGIONALCOST FLOAT)'''                                
sqlQueryStoresCreationTable = '''DROP TABLE IF EXISTS tableDStores;CREATE TABLE IF NOT EXISTS tableDStores (ID INTEGER PRIMARY KEY AUTOINCREMENT, STORENUMBER TEXT NOT NULL, STADDRESS TEXT, CITY TEXT, STATE TEXT, ZIPCODE TEXT, LATITUDE TEXT,LONGITUDE TEXT)'''
sqlQueryStoresNetworkDistTable = '''DROP TABLE IF EXISTS distCostNetTable;CREATE TABLE IF NOT EXISTS distCostNetTable (ID INTEGER PRIMARY KEY AUTOINCREMENT, DCENTER TEXT, DOMINOSTORE TEXT, MILES FLOAT, shippingCost FLOAT, perDoughCost FLOAT)'''
sqlQueryStoresAvgDemandTable = '''DROP TABLE IF EXISTS avgDemandStoresTable;CREATE TABLE IF NOT EXISTS avgDemandStoresTable (ID INTEGER PRIMARY KEY AUTOINCREMENT, STORENUMBER TEXT NOT NULL, averageDailyDemand FLOAT, averageBiWeeklyDemand FLOAT)'''                                                           
sqlQueryStoresModelResultTable = '''DROP TABLE IF EXISTS resultTable;CREATE TABLE IF NOT EXISTS resultTable (ID INTEGER PRIMARY KEY AUTOINCREMENT, DistributionCenter TEXT,DominoStore TEXT, outputBinValue INT)'''
                                
                                
#Table Creation into DB                                                                                 
sqlTableObject.executescript(sqlQueryStoresAvgDemandTable)
sqlTableObject.executescript(sqlQueryStoresCreationTable)
sqlTableObject.executescript(sqlQueryStoresNetworkDistTable)
sqlTableObject.executescript(sqlQueryStoresDistCentersTable)
sqlTableObject.executescript(sqlQueryStoresModelResultTable)

db.commit()

#Store Information Being Stored in SQL Table
storesInformation = open('OR604 Good Dominos Data.csv','r')
readerObject = csv.reader(storesInformation)
readerObject.next() 

for rowData in readerObject:
    tupleToEnter = (rowData[0],rowData[2],rowData[3],rowData[4],rowData[5],rowData[6],rowData[7])
    sqlTableObject.execute(''' INSERT INTO tableDStores (STORENUMBER, STADDRESS, CITY, STATE, ZIPCODE,  LATITUDE, LONGITUDE) VALUES (?,?,?,?,?,?,?) ''', tupleToEnter)
    
storesInformation.close()
db.commit()

#Average Demand Being Calculated and Stored in SQL Table
demandData = pandas.read_csv('Daily Demand Data.csv',low_memory=False)
demandData.columns = ['Date','STORENUMBER','averageDailyDemand']
missingStoresMean = demandData['averageDailyDemand'].mean()
tempData = demandData.groupby(['STORENUMBER']).mean()
tempData['averageDailyDemand'] = tempData['averageDailyDemand'].apply(lambda x: math.ceil(x))
tempData['averageBiWeeklyDemand'] = (tempData['averageDailyDemand'] * 4).astype(float)
tempData.to_sql('avgDemandStoresTable',db,if_exists='append')    
db.commit()
                 
#Loading Store Number in a list
tempObject = sqlTableObject.execute('''Select STORENUMBER from tableDStores''')
storeNumber = tempObject.fetchall()
tempObject = sqlTableObject.execute('''Select STORENUMBER from avgDemandStoresTable''')
avgDemandStoresNumber = tempObject.fetchall()


AvgForMissingRows = []
for rowData in storeNumber:
    if rowData not in avgDemandStoresNumber :
        tupleToEnter = (rowData[0],math.ceil(missingStoresMean),math.ceil(missingStoresMean)*4)
        AvgForMissingRows.append(tupleToEnter)
    else:
        pass

tempObject = sqlTableObject.executemany('''INSERT INTO avgDemandStoresTable (STORENUMBER,averageDailyDemand,averageBiWeeklyDemand) VALUES (?,?,?)''', AvgForMissingRows)
db.commit()

#Loding DC data in tables
readData = open('OR 604 DC.csv','r')
readerObject = csv.reader(readData)
readerObject.next()

for rowData in readerObject:
     SupplyCapacity = int(rowData[7])
     tupleToEnter = (rowData[0],rowData[1],rowData[2],rowData[3],rowData[4],rowData[5],rowData[6], rowData[7], rowData[8])
     sqlTableObject.execute(''' INSERT INTO distCentersTable (DCENTER, STADDRESS, CITY, STATE, ZIPCODE,  LATITUDE, LONGITUDE, WEEKLYSUPCAP, REGIONALCOST) VALUES (?,?,?,?,?,?,?,?,?) ''', tupleToEnter)
    
readData.close()
db.commit()

#Loading Distribution Centres and Stores Data into Dictionaries
tempObject = sqlTableObject.execute('''SELECT * FROM distCentersTable''')
distCentersDict = tempObject.fetchall()
tempObject = sqlTableObject.execute('''SELECT * FROM tableDStores''')
storesDataDict = tempObject.fetchall()

#appending Data into Cost table
tempDictionary = []
for rowDistCenter in distCentersDict:
    distCenterID = rowDistCenter[1]
    distCenterLatLong = (rowDistCenter[6],rowDistCenter[7])
    costDCR = rowDistCenter[9]
    
    for EachStore in storesDataDict:
        storeID = EachStore[1]
        storeLatLong = (EachStore[6],EachStore[7])
        distInMiles = vincenty(distCenterLatLong ,storeLatLong ).miles
        shippingCost = round(distInMiles * costDCR,2)
        perDoughCost = round(shippingCost/9900,2)
        tupleToEnter = (distCenterID,storeID,distInMiles,shippingCost,perDoughCost)
        tempDictionary.append(tupleToEnter)

tempObject = sqlTableObject.executemany('''INSERT INTO distCostNetTable (DCENTER,DOMINOSTORE,MILES,shippingCost,perDoughCost) VALUES (?,?,?,?,?)''', tempDictionary)
db.commit()


#Loading Net Cost and Avg Demand into Dictionaries
tempObject = sqlTableObject.execute('''SELECT * FROM distCostNetTable''')
netCostDict = tempObject.fetchall()
tempObject = sqlTableObject.execute('''SELECT * FROM avgDemandStoresTable''')
avgDemandsDict = tempObject.fetchall()


#Dictionaries For Model Building
costPerDough = {}
for rowData in netCostDict:
    distCenterID = rowData[1]
    storeID = rowData[2]
    EachDoughCost = rowData[5]
    costPerDough[distCenterID,storeID] = float(EachDoughCost)
demandPerStore = {}
for rowData in avgDemandsDict:
    storeID = rowData[1]
    avgDemand = rowData[3]
    demandPerStore[storeID] = float(avgDemand)
distCentSupply = {}
for rowData in distCentersDict:
    distCenterID = rowData[1]
    SupCap = int((rowData[8]/7)*4)
    distCentSupply[distCenterID] = float(SupCap)


#Model optimization
DomPizzaModel = Model('DominosPizzaProblem')
DomPizzaModel.ModelSense = GRB.MINIMIZE 
DomPizzaModel.setParam('MIPFocus',1)
DomPizzaModel.setParam('TimeLimit',600)
DomPizzaModel.update()

# Defining Variables for Objective Function
myVars = {}
for DC,DS in costPerDough:
    myVars[DC,DS] = DomPizzaModel.addVar(obj=costPerDough[DC,DS]*demandPerStore[DS], vtype=GRB.BINARY, name = 'x_'+DC+'_'+DS)
DomPizzaModel.update()

#Contraints for Model
myConstrs = {}
for EachStore in demandPerStore:
    constrName = 'Demand_' + EachStore 
    MinDemand = float(demandPerStore[EachStore])
    myConstrs[constrName] = DomPizzaModel.addConstr(quicksum(myVars[EachDC,EachStore] * MinDemand for EachDC in distCentSupply) == MinDemand, name = constrName)

DomPizzaModel.update()


for EachDC in distCentSupply:
    constrName = 'Supply_' + EachDC 
    Sup = float(distCentSupply[EachDC])
    myConstrs[constrName] = DomPizzaModel.addConstr(quicksum(myVars[EachDC,EachStore] * demandPerStore[EachStore] for EachStore in demandPerStore) <= Sup, name = constrName)

DomPizzaModel.update()

#Writting in LP file
DomPizzaModel.write('DomPizzaModel.lp')
DomPizzaModel.optimize()

#printing Q1 Results on console
if DomPizzaModel.status == GRB.OPTIMAL:
    for temp in myVars:
        if myVars[temp].x > 0:
            print (temp, myVars[temp].x)

#Storing Q1 Results in Database
count=[]
if DomPizzaModel.status == GRB.OPTIMAL:
    objectiveFuctionResults = DomPizzaModel.objVal
    solutionDict = []
    for v in myVars:
        if myVars[v].x > 0.0:
            VarName = myVars[v].varName.split('_')
            DistributionCenter = VarName[1]
            DStore = VarName[2]
            outputBinValue = myVars[v].x
            DateTime = datetime.datetime.now()
            tupleToEnter = (DistributionCenter,DStore,int(outputBinValue))
            solutionDict.append(tupleToEnter)
            
                    
sqlTableObject.executemany('''INSERT INTO resultTable (DistributionCenter,DominoStore,outputBinValue) VALUES (?,?,?)''',solutionDict)
db.commit()







#------------------------------------------ Question no 2 - Ardent Mills Model---------------------------------------------------------------------   






sqlTableObject = db.cursor()

#Table Creation queries
sqlQueryArdentMillsTable = '''DROP TABLE IF EXISTS ardentMillsTable;CREATE TABLE IF NOT EXISTS ardentMillsTable (ID INTEGER PRIMARY KEY AUTOINCREMENT, MILLNUMBER TEXT NOT NULL, STADDRESS TEXT, CITY TEXT, STATE TEXT, ZIPCODE TEXT, LATITUDE TEXT,LONGITUDE TEXT, SUPPLYCAPACITY INT, COSTPERUNIT FLOAT, REGIONALCOST FLOAT)'''
sqlQueryMilltoDistCentTable = '''DROP TABLE IF EXISTS milltoDistCentTable;CREATE TABLE IF NOT EXISTS milltoDistCentTable (ID INTEGER PRIMARY KEY AUTOINCREMENT, MillID TEXT NOT NULL, DCenterID TEXT, Distance FLOAT, TravelCost FLOAT, CostPerDough FLOAT)'''
sqlQueryArdentResultTable = '''DROP TABLE IF EXISTS ardentResultTable;CREATE TABLE IF NOT EXISTS ardentResultTable (ID INTEGER PRIMARY KEY AUTOINCREMENT, Mill TEXT, DC TEXT, outputBinValue INT)'''
                                                                       
#Table Creation in Database
sqlTableObject.executescript(sqlQueryArdentMillsTable )
sqlTableObject.executescript(sqlQueryMilltoDistCentTable)
sqlTableObject.executescript(sqlQueryArdentResultTable)
db.commit()

#Reading Ardent Mills Data
ardentMillFileReader = open('Ardent Mills.csv','r')
readerObject = csv.reader(ardentMillFileReader)
readerObject.next() #Skipping header

for rowData in readerObject:
    tupleToEnter = (rowData[0],rowData[1],rowData[2],rowData[3],rowData[4],rowData[5],rowData[6],rowData[7],rowData[8],rowData[9])
    sqlTableObject.execute(''' INSERT INTO ardentMillsTable (MILLNUMBER, STADDRESS, CITY, STATE, ZIPCODE,  LATITUDE, LONGITUDE, SUPPLYCAPACITY, COSTPERUNIT, REGIONALCOST) VALUES (?,?,?,?,?,?,?,?,?,?) ''', tupleToEnter)
    
ardentMillFileReader.close()
db.commit()

#Extracting Distribution Centres and Ardent Mills Data from Database tables
tempObject = sqlTableObject.execute('''SELECT DCENTER, LATITUDE, LONGITUDE FROM distCentersTable''')
distCentToDistStoreData = tempObject.fetchall()
tempObject = sqlTableObject.execute('''SELECT * FROM ardentMillsTable''')
ardentMillsData = tempObject.fetchall()

tempDict2 = []
for millData in ardentMillsData:
    millID = millData[1]
    millLatLng = (millData[6],millData[7])
    regionalCostPerMill = millData[10]
    millCostPerUnit = millData[9]
    for EachDC in distCentToDistStoreData:
        distCenterID = EachDC[0]
        distCenterLatLng = (EachDC[1],EachDC[2])
        millToDistCentDistance = vincenty(millLatLng,distCenterLatLng).miles
        travelCost = round(millToDistCentDistance*regionalCostPerMill,2)
        doughCost = round(millCostPerUnit/63,2)        #There are 63 pizza doughs in one unit
        tupleToEnter = (millID,distCenterID,millToDistCentDistance,travelCost,doughCost)
        tempDict2.append(tupleToEnter)

tempObject = sqlTableObject.executemany('''INSERT INTO milltoDistCentTable (MillID,DCenterID,Distance,TravelCost,CostPerDough) VALUES (?,?,?,?,?)''', tempDict2)
db.commit()


#Building Data/Dictionaries
travelCostDict = {}
for rowData in tempDict2:
    millID = rowData[0]
    distCentID = rowData[1]
    travelCost = rowData[3]
    travelCostDict[millID,distCentID] = float(travelCost)
costPerDoughArdent = {}
for rowData in tempDict2:
    millID = rowData[0]
    distCentID = rowData[1]
    PerDoughCost = rowData[4]
    costPerDoughArdent[millID,distCentID] = float(PerDoughCost)
millSupplyCapDict = {}
for rowData in ardentMillsData:
    millID = rowData[1]
    temp = rowData[8]*63
    millCapacity = math.ceil((temp/7)*4)
    millSupplyCapDict[millID] = int(millCapacity) 
distCentSupply2 = {}
for rowData in distCentersDict:
    DCenterID = rowData[1]
    SupCap = math.ceil((rowData[8]/7)*4)
    distCentSupply2[DCenterID] = float(SupCap)

#Minimizing the Model    
ardentFlourModel = Model('ArdentFlourMillsToDistCentModel')
ardentFlourModel.ModelSense = GRB.MINIMIZE
ardentFlourModel.update()


#Objective Function
myVars2 = {}
for Mill,DC in costPerDoughArdent:
    myVars2[Mill,DC] = ardentFlourModel.addVar(obj=(costPerDoughArdent[Mill,DC]*distCentSupply2[DC])+travelCostDict[Mill,DC],vtype=GRB.BINARY, name = 'x_'+Mill+'_'+DC)
ardentFlourModel.update()

operationalCost = 3000000
operationalCostsDict = {}
for Mill in millSupplyCapDict:
    operationalCostsDict[Mill] = ardentFlourModel.addVar(obj=operationalCost,vtype=GRB.BINARY,name='operationCharges_'+Mill)
ardentFlourModel.update()

#Constraints
myConstrs = {}
for EachDC in distCentSupply2: 
    constrName = 'Demand_' + EachDC
    MinDemand = float(distCentSupply2[EachDC])
    myConstrs[constrName] = ardentFlourModel.addConstr(quicksum(MinDemand*myVars2[millData,EachDC] for millData in millSupplyCapDict) >= MinDemand, name = constrName)
ardentFlourModel.update()

for millData in millSupplyCapDict:
    constrName = 'Supply_' + millData 
    Sup = float(millSupplyCapDict[millData])
    myConstrs[constrName] = ardentFlourModel.addConstr(quicksum(myVars2[millData,EachDC]*distCentSupply2[EachDC] for EachDC in distCentSupply2) <= Sup, name = constrName)

#Optimizing Model
ardentFlourModel.update()
ardentFlourModel.optimize()
ardentFlourModel.write('ardentFlourMill.lp')

#printing Q2 on Console
if ardentFlourModel.status == GRB.OPTIMAL:
    for temp in myVars2:
        if myVars2[temp].x > 0:
            print (temp, myVars2[temp].x)

#Writing Q2 in Database
if ardentFlourModel.status == GRB.OPTIMAL:
    solutionDict2 = []
    for temp2 in myVars2:
        if myVars2[temp2].x > 0.0:
            tempVarName = myVars2[temp2].varName.split('_')
            millID = tempVarName[1]
            DistCent = tempVarName[2]
            outputBinValue = myVars2[temp2].x
            tupleToEnter = (millID,DistCent,int(outputBinValue))
            solutionDict2.append(tupleToEnter)
            
sqlTableObject.executemany('''INSERT INTO ardentResultTable (Mill,DC,outputBinValue) VALUES (?,?,?)''',solutionDict2)
db.commit()

