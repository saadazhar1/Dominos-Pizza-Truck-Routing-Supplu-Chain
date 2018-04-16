# -*- coding: utf-8 -*-
"""
Created on Mon Oct 09 20:38:19 2017

@author: Saad
"""

import sqlite3,math,datetime
import pandas,csv
from gurobipy import *
from geopy.distance import vincenty


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
                                                            
sqlQueryStoresModelResultTable = '''DROP TABLE IF EXISTS resultTable;CREATE TABLE IF NOT EXISTS resultTable (ID INTEGER PRIMARY KEY AUTOINCREMENT, DistributionCenter TEXT,DominoStore TEXT, Doughs INT)'''
                                
                                
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
DomPizzaModel.update()

# Defining Variables for Objective Function
myVars = {}
for DC,DS in costPerDough:
    myVars[DC,DS] = DomPizzaModel.addVar(obj=costPerDough[DC,DS]*demandPerStore[DS], vtype=GRB.CONTINUOUS, name = 'x_'+DC+'_'+DS)
DomPizzaModel.update()

#Contraints for Model
myConstrs = {}
for EachStore in demandPerStore:
    constrName = 'Demand_' + EachStore 
    MinDemand = float(demandPerStore[EachStore])
    myConstrs[constrName] = DomPizzaModel.addConstr(quicksum(myVars[EachDC,EachStore] for EachDC in distCentSupply) >= MinDemand, name = constrName)

DomPizzaModel.update()


for EachDC in distCentSupply:
    constrName = 'Supply_' + EachDC 
    Sup = float(distCentSupply[EachDC])
    myConstrs[constrName] = DomPizzaModel.addConstr(quicksum(myVars[EachDC,EachStore] for EachStore in demandPerStore) <= Sup, name = constrName)

DomPizzaModel.update()

#Writting in LP file
DomPizzaModel.write('DomPizzaModel.lp')
DomPizzaModel.optimize()

#%% print the solution to the screen
if DomPizzaModel.status == GRB.OPTIMAL:
    for temp in myVars:
        if myVars[temp].x > 0:
            print (temp, myVars[temp].x)

count=[]
if DomPizzaModel.status == GRB.OPTIMAL:
    objectiveFuctionResults = DomPizzaModel.objVal
    DateTime = datetime.datetime.now()
    solutionDict = []
    for v in myVars:
        if myVars[v].x > 0.0:
            VarName = myVars[v].varName.split('_')
            DistributionCenter = VarName[1]
            DStore = VarName[2]
            doughs = myVars[v].x
            DateTime = datetime.datetime.now()
            tupleToEnter = (DistributionCenter,DStore,int(doughs))
            solutionDict.append(tupleToEnter)
            
            
            

sqlTableObject.executemany('''INSERT INTO resultTable (DistributionCenter,DominoStore,Doughs) VALUES (?,?,?)''',solutionDict)
db.commit()