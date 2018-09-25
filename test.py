from datetime import datetime, timedelta, date
import pandas as pd
import pyodbc
from io import StringIO #enables to put this into memory
import sys
import pymongo as pym
from bson.objectid import ObjectId
import csv

class Test:
    def __init__(self, extid):
        self.extid = extid
        self.endcount = 0
        self.changecount= 0
        self.newcount = 0
        self.prev = {}
        self.curr = {}
        self.parIndex = 0
        self.benIndex = 0
        self.headers = ''
    
    def getObjectIDs(self):
        cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER=seaeb-hwdvsql;DATABASE=DevProcessing;trusted_connection=yes')
        cnxn.autocommit = True
        cursor=cnxn.cursor()

        sqlcmd = 'execute pr_getObjectIDs ' + str(self.extid)

        print("running sql statement")
        data = cursor.execute(sqlcmd)
        objids = data.fetchone() 
        print("running complete")
        cnxn.close()
        return objids

    # We will need this if ParticipantIDs and BenefitIDs are always in the same place
    def findPartBenefitID(self, rawdata):
        #rawdata = 'ParticipantID|RandomID|BenefitID\n1000|20|16\n'
        data = rawdata.split('|')
        for i, d in enumerate(data):
            if d == 'ParticipantID':
                self.parIndex = i
                #print('ParticipantID index:', i)
            if d == 'BenefitID':
                self.benIndex = i
                #print('BenefitID index:', i)

    #pull data and sets it in dictionary
    def pulldata(self, objectid, curr):     
        client = pym.MongoClient()
        db = client['extractdb']
        collection = db['extracts']
        data = collection.find_one({'_id': ObjectId(objectid) })
        rawdata = data['gendata']
        #myreader = csv.reader(rawdata.splitlines(), delimiter="|")
        myreader = rawdata.splitlines()
        self.headers = myreader[0]
        self.findPartBenefitID(myreader[0])
        parInd = self.parIndex
        benInd = self.benIndex
        #iterate through it all to set up current and previous dictionarys
        for row in myreader[1:]:
            key = row.split('|')[parInd] + '-' + row.split('|')[benInd]
            if curr:
                self.curr[key] = row
            else:
                self.prev[key] = row

        client.close()

    def compare(self, objectid):
        new = set(self.curr.keys()) - set(self.prev.keys())
        ended = set(self.prev.keys()) - set(self.curr.keys())
        same = list( set(self.prev.keys()) & set(self.curr.keys()) )

        client = pym.MongoClient()
        db = client['extractdb']
        collection = db['extracts']

        #string io saves data to memory
        #new data
        with StringIO() as f:
            f.write(self.headers + '\n')
            for n in new:
                f.write(self.curr[n] + '\n')
                #print('data:',n, file=f)
            f.seek(0)
            print('saving to MongoDB...')
            #write out update
            data = collection.update_one({'_id': ObjectId(objectid) },
            {'$set':{
                'new': f.read()
                }
            }, upsert = False)

        #write to end to Mongodb
        with StringIO() as e:
            e.write(self.headers + '\n')
            for ed in ended:
                e.write(self.prev[ed] + '\n')
                #print('data:',n, file=f)
            e.seek(0)
            print('saving to MongoDB...')
            #write out update
            data = collection.update_one({'_id': ObjectId(objectid) },
            {'$set':{
                'end': e.read()
                }
            }, upsert = False)
        
        self.newcount = len(new)
        self.endcount = len(ended)

        #changes to MongoDB
        with StringIO() as c:
            c.write(self.headers + '\n')
            #loop through same keys
            for s in same:
                #check for changes
                if self.curr[s] != self.prev[s]: 
                    # write to change file
                    self.changecount += 1

                    print('current:',self.curr[s] , file=c)
                    print('old:',self.prev[s] , file=c)
            #go back to begining        
            c.seek(0)
            print('saving to MongoDB...')
            #write out update
            data = collection.update_one({'_id': ObjectId(objectid) },
            {'$set':{
                'changes': c.read()
                }
            }, upsert = False )
                
mytest = Test(1)

m_objects = mytest.getObjectIDs()
mytest.pulldata(m_objects[0], True)
mytest.pulldata(m_objects[1], False)
mytest.compare(m_objects[0])
print('new:', mytest.newcount, 'ends:',mytest.endcount,'changes:', mytest.changecount)
print('complete')





