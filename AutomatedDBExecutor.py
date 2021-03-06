import logging
import getopt
import sys
import os
import glob
import ConfigParser
from MySQLDBUtil import MySQLDBUtil

SQL_SCRIPTS_TO_BE_EXECUTED="select name, max(version) version from script_metadata where executed=0 and releas ='%s' and env='%s' and component='%s' group by name"
EXECUTED_SQL_SCRIPTS="select name, max(version) version from script_metadata where executed=1 and name = '%s' and releas ='%s' and env='%s' and component='%s'"
MARK_SCRIPT_AS_EXECUTED="update script_metadata set executed=1 where name='%s' and releas ='%s' and env='%s' and version<=%s and component='%s'"

class AutomatedDBExecutor:
    
    logging.basicConfig(filename='/var/log/db_executor.log',level=logging.INFO)
    
    def __init__(self, baseFolder, release, env, component):
        self.baseFolder = baseFolder
        self.release = release
        self.scriptsDir = os.path.join(self.baseFolder,self.release)
        self.env = env
        self.component = component
        logging.info("I'll update db on %s for release %s", env,release)
        config = ConfigParser.RawConfigParser( )
        config.read("db.properties")
        self.mySQLDBUtil = MySQLDBUtil(config, "common_db")
        self.mySQLEnvDBUtil = MySQLDBUtil(config, self.env)
        os.chdir(self.scriptsDir)
        
    
    #I'll process the scripts and do the entries of scripts that these scripts
    #needs to be executed
    def processReleaseScriptsMetaData(self):
        SCRIPTS_META_FILE = os.path.join(self.baseFolder,self.release,"sql_sequence.txt")
        try:
            with open(SCRIPTS_META_FILE) as scriptsMetaFile:
                scripts = scriptsMetaFile.readlines()
                for script in scripts:
                    self.processReleaseScriptMetaData(script.strip("\n"))
        except IOError:
            logging.error("%s file doesn't exist please verify", SCRIPTS_META_FILE)
            sys.exit(1)

    
    #I'll process the script and do the entries of all the verions of script
    def processReleaseScriptMetaData(self, script):
        logging.info("\n\nUpdating metadata of script %s", script)
        #for file in glob.glob(script+"*_do.sql"):
        for file in glob.glob("do_*"+script+"*.sql"):
            self.addScriptToMetaData(file);
        
    #I'll add the file in meta list if not already stored
    def addScriptToMetaData(self, script):
        logging.info("Verifying if script %s already tracked", script)
        scriptInfo = ScriptInfo(script, self.release, self.env, self.component)
        if not self.mySQLDBUtil.recordExistsUsingCountQuery("script_metadata", scriptInfo.scriptExistsQuery()):
            logging.info("Adding script %s for tracking", script)
            self.mySQLDBUtil.insertRecord("script_metadata", scriptInfo.scriptInsertQuery())
        else:
            logging.info("Script %s is already tracked", script)

    #I'll process the script's,execute the new entrant scripts
    def processReleaseScripts(self):
        logging.info("\n\nVerifying what all script's needs to be executed")
        sqlScriptsToBeExecuted = SQL_SCRIPTS_TO_BE_EXECUTED %(self.release, self.env, self.component)
        scriptsToBeExecutedDict = self.mySQLDBUtil.getResultAsDict(sqlScriptsToBeExecuted)
        for scriptToBeExecutedDict in scriptsToBeExecutedDict:
            scriptToBeExecuted = ScriptInfo.createScriptInfo(scriptToBeExecutedDict, self.release, self.env, self.component)
            self.executeScript(scriptToBeExecuted)

    # Do following things
    # Verify Undo script exists for last executed script & Script is present in the system
    # Execute the script
    # Mark the script for execution    
    def executeScript(self, scriptToBeExecuted):
        logging.info("Working on executing script %s for version %s" % (scriptToBeExecuted.scriptName, scriptToBeExecuted.version))
        if self.undoScriptExecuted(scriptToBeExecuted):
            scriptPath = os.path.join(self.scriptsDir,scriptToBeExecuted.getScriptFileName())
            if os.path.isfile(scriptPath):
                logging.info("Executing the script %s for version %s"%(scriptToBeExecuted.scriptName,str(scriptToBeExecuted.version))) 
                self.mySQLEnvDBUtil.executeFile(scriptPath, logging)
                self.mySQLDBUtil.executeQuery(scriptToBeExecuted.getQueryToMarkScriptAsExecuted())
            else:
                logging.error("Script %s doesn't exists, please check!"%(scriptPath))
                sys.exit(1)

    def undoScriptExecuted(self, scriptToBeExecuted):
        undoScriptExecuted=0
        lastExecutedScriptDict = self.mySQLDBUtil.getSingleResultAsDict(scriptToBeExecuted.getQueryToFetchLastExecutedScript())
        if lastExecutedScriptDict['name']:
            lastExecutedScript = ScriptInfo.createScriptInfo(lastExecutedScriptDict, self.release, self.env)
            logging.info("Last executed script %s version is %s" % (lastExecutedScript.scriptName, lastExecutedScript.version))
            undoScriptPath=os.path.join(self.scriptsDir,lastExecutedScript.getScriptUndoFileName())
            undoScriptExists = os.path.isfile(os.path.join(self.scriptsDir,lastExecutedScript.getScriptUndoFileName()))
            if undoScriptExists:
                logging.info("Undo script exists in system executing it")
                self.mySQLEnvDBUtil.executeFile(undoScriptPath,logging)
                undoScriptExecuted=1
            else:
                logging.error("Undo script %s for %s version %s does not exists. Please check!"%(lastExecutedScript.getScriptUndoFileName(), lastExecutedScript.scriptName, lastExecutedScript.version))
                sys.exit(1)
        else:
            logging.info("This is first entry of script %s, no need to execute undo script" % (scriptToBeExecuted.scriptName))
            undoScriptExecuted = 1
        return undoScriptExecuted

class ScriptInfo:
    def __init__(self, scriptFileName, release, env, component):
        #self.scriptName =  scriptFileName.split('_')[2][:-4]
        self.scriptName =  "_".join(str(word) for word in scriptFileName.split('_')[2:])[:-4]
        self.version =  scriptFileName.split('_')[1]
        self.release = release
        self.env = env
        self.component = component
    
    @classmethod
    def createScriptInfo(cls,scriptDict, release, env, component):
        #scriptFileName="".join((scriptDict['name'],'_',str(scriptDict['version']),'_do.sql' ))
        scriptFileName="".join(('do','_',str(scriptDict['version']),'_',scriptDict['name'],'.sql' ))
        scriptInfo = cls(scriptFileName, release, env, component)
        return scriptInfo
    
    def scriptExistsQuery(self):
        scriptExistQuery = ""
        scriptExistQuery = scriptExistQuery.join((
        " name = '",self.scriptName,"'",
        " and version =", self.version,
        " and releas ='", self.release,"'",
        " and component ='", self.component,"'",
        " and env ='", self.env,"'"
        ))
        return scriptExistQuery 

    def scriptInsertQuery(self):
        scriptInsertQuery = ""
        scriptInsertQuery = scriptInsertQuery.join((
        "( name, version, releas, component, env) values",
        " ( '",self.scriptName,"',",
        self.version,",",
        "'",self.release,"',",
        "'",self.component,"',",
        "'",self.env,"'",
        ")"))
        return scriptInsertQuery
    
    def getScriptFileName(self):
        #return "".join((self.scriptName,'_',str(self.version),'_do.sql' )) 
        return "".join(('do','_',str(self.version),'_',self.scriptName,'.sql' ))

    def getScriptUndoFileName(self):
        #return "".join((self.scriptName,'_',str(self.version),'_undo.sql' )) 
        return "".join(('undo','_',str(self.version),'_',self.scriptName,'.sql' ))
    
    def getQueryToMarkScriptAsExecuted(self):
        return (MARK_SCRIPT_AS_EXECUTED%(self.scriptName, self.release, self.env, self.version, self.component))

    def getQueryToFetchLastExecutedScript(self):
        return (EXECUTED_SQL_SCRIPTS %(self.scriptName, self.release, self.env, self.component))
          
        
'''        
automatedDBExecutor = AutomatedDBExecutor("/home/user/personal/python/database_scripts", "release1", "dev", "ipms")
automatedDBExecutor.processReleaseScriptsMetaData()
automatedDBExecutor.processReleaseScripts()
'''

