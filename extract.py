from datetime import datetime, timedelta, date
import pandas as pd
import pyodbc
from io import StringIO #enables to put this into memory
import sys
import pymongo as pym
from bson.objectid import ObjectId


class Extract:
    def __init__(self, extid, gentable, name, proc, clientid, rundate, mindate, historytype, ae, otherparam ):
        self.extid = extid
        self.gentable = gentable
        self.name = name
        self.proc = proc
        self.clientid = clientid
        self.rundate = rundate
        self.mindate = mindate
        self.other = otherparam
        self.historytype = historytype
        self.ae = ae
        self.proc_sql = self.set_proc_sql()

    def set_proc_sql(self):

        #x = datetime.now()
        z = datetime(self.rundate.year, self.rundate.month, self.rundate.day)

        sql = 'execute ' + self.proc + " '"  + str(z) +  "', '" + str(self.mindate) + \
        "', '" + self.historytype +  "', " + str(self.clientid) + ", " + self.other
        return sql

    def execute_sql(self):
        cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER=#####;DATABASE=#######;trusted_connection=yes')
        cnxn.autocommit = True
        cursor=cnxn.cursor()

        print("running sql statement")
        cursor.execute(self.proc_sql)
        print("running complete")
        cnxn.close()

    def current_details(self):
        cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER=#####;DATABASE=######;trusted_connection=yes')
        df_currgen = pd.read_sql('select * from p_TestAutomation order by ParticipantID', cnxn)
        s = StringIO()
        df_currgen.to_csv(s, sep="|", index=False)
        string = s.getvalue()
        return string

    def save_csv(self, string):
        client = pym.MongoClient()
        db = client['extractdb']
        collection = db['extracts']
        #dictonary = {}
        extract = {}
        extract['extid'] = self.extid
        extract['gendata'] = string
        extract['changes'] = ''
        extract['new'] = ''
        extract['end'] = ''
        extract['timestamp'] = datetime.now().timestamp()
        result = collection.insert_one(extract)
        new_id = result.inserted_id
        client.close()
        return str(new_id)
        
    def saveToSQL_Directory(self, mongoObjectid):
        cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER=######;DATABASE=########;trusted_connection=yes')
        cnxn.autocommit = True
        cursor=cnxn.cursor()

        sqlcmd = 'execute pr_ExtDirSave ' + str(self.extid) +  ", '" + mongoObjectid +"'"

        print("running sql statement")
        cursor.execute(sqlcmd)
        print("running complete")
        cnxn.close()


    def find_ids(self, _id):
        client = pym.MongoClient()
        db = client['extractdb']
        collection = db['extracts']
        for data in collection.find({'extid': _id}):
            print(data['_id'])
            print(data['changes'])
        client.close()

    def delete_ids(self, _id):
        client = pym.MongoClient()
        db = client['extractdb']
        collection = db['extracts']
        for data in collection.find({'extid': _id}):
            collection.delete_one({'_id': ObjectId(data['_id'])})
            


ext = Extract(1,'table','test', 'pr_TestAutomation', 13, datetime.now(), datetime(2018, 1, 1), 'c', 0, "88, 16" )
ext.find_ids(1)
### ext.delete_ids(1)  
### ext.find_ids(1)  

# ext = Extract(1,'table','test', 'pr_TestAutomation', 13, datetime.now(), datetime(2018, 1, 1), 'c', 0, "88, 16" )
# print(ext.proc_sql)
# ext.execute_sql()
# print('current details')
# current_details = ext.current_details()
# print(len(current_details))

# print('size:', sys.getsizeof(current_details) /1000, 'kilobytes')
# saved_id = ext.save_csv(current_details)
# print(saved_id)
# ext.saveToSQL_Directory(saved_id)



# print(saved_id)


# 5ba2c5f51663610b3cf3d0a1

#Things to do:
# once data is compared, keep new, changes, end counts, and data saved some place
# then write out the files
    # may need to create a pr_delimiter stored procedure
# once files are written send emails out with success and failures, and counts

#Show Brian, and Sue all the changes made, and explain how this could be an added benefit

#Move to Production:
# I need to update the procedures to take in the default parameters
# I also need to update the ae functionality so we can just pass a one or a zero in
