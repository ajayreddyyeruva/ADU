
import MySQLdb as mdb, MySQLdb.cursors
from subprocess import Popen, PIPE 
class MySQLDBUtil:
    
    def __init__(self, dbHost,dbPort,dbUser,dbPwd,db):
        self.dbHost = dbHost
        self.dbPort = dbPort
        self.dbUser = dbUser
        self.dbPwd = dbPwd
        self.db = db

    def __init__(self, config, configSection):
        self.dbHost = config.get(configSection,"dbHost")
        self.dbPort = int(config.get(configSection,"dbPort"))
        self.dbUser = config.get(configSection,"dbUser")
        self.dbPwd = config.get(configSection,"dbPwd")
        self.db = config.get(configSection,"db")
    
    def recordExistsUsingCountQuery(self, table, whereCondition):    
        
        con = mdb.connect(host=self.dbHost, port=self.dbPort, user=self.dbUser, passwd=self.dbPwd, db = self.db)
        recordExists = 0
        
        with con:
            recordCountQuery = "select count(1) from " + table + " where " + whereCondition
            print ("Executing query %s "% recordCountQuery)
            cur = con.cursor();
            cur.execute(recordCountQuery)
            recordExists = cur.fetchone()
        
        print recordExists[0]
        return recordExists[0]
    
    def insertRecord(self, table, insertQuery):
        con = mdb.connect(host=self.dbHost, port=self.dbPort, user=self.dbUser, passwd=self.dbPwd, db = self.db)
        with con:
            cur = con.cursor();
            sql = "INSERT into %s  %s" % (table, insertQuery)
            print ("Executing insert query %s"% sql)
            cur.execute(sql)

    def getResultAsDict(self, fetchQuery):
        print ("Executing query %s"% fetchQuery)
        con = mdb.connect(host=self.dbHost, port=self.dbPort, user=self.dbUser, passwd=self.dbPwd, db = self.db, cursorclass=mdb.cursors.DictCursor)
        with con:
            cur = con.cursor();
            cur.execute(fetchQuery)
            data = cur.fetchall()
            return data
        
    def getSingleResultAsDict(self, fetchQuery):
        print ("Executing query %s"% fetchQuery)
        con = mdb.connect(host=self.dbHost, port=self.dbPort, user=self.dbUser, passwd=self.dbPwd, db = self.db, cursorclass=mdb.cursors.DictCursor)
        with con:
            cur = con.cursor();
            cur.execute(fetchQuery)
            data = cur.fetchall()
            return data[0]
    
    def executeQuery(self, query):
        print ("Executing query %s"% query)
        con = mdb.connect(host=self.dbHost, port=self.dbPort, user=self.dbUser, passwd=self.dbPwd, db = self.db)
        with con:
            cur = con.cursor();
            cur.execute(query)
    
    def executeFile(self, sqlFilePath):
        process = Popen('mysql -u %s -p%s -h %s %s' % (self.dbUser, self.dbPwd, self.dbHost, self.db),stdout=PIPE, stdin=PIPE, shell=True)
        output = process.communicate('source ' + sqlFilePath)[0]
        print output
        
'''    
mySQLDBUtil = MySQLDBUtil("localhost",3306,"root","root","test")

recordExists = mySQLDBUtil.recordExistsUsingCountQuery("example", "id=1")

if recordExists:
    print "Record exists "
else:
    print "Record doesn't exists"
dataDict = {"name":"script", "version":1}
mySQLDBUtil.insertRecord("script_metadata", "(name, version) values ('name', 1)")

rows = mySQLDBUtil.getResultAsDict("select name, max(version) version from script_metadata where executed=0 group by name")

for row in rows:
    print row['version']
    print row['name']

mySQLDBUtil.executeFile("/home/user/personal/python/database_scripts/release1/script1_1_do.sql");
'''    

