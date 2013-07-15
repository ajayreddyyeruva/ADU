import os
import getopt
import sys
from AutomatedDBExecutor import AutomatedDBExecutor

opts, args = getopt.getopt(sys.argv[1:],"hf:r:e",["folder=","release=","env="])
for opt,arg in opts:
  if opt in ['-f','--folder']:
    baseFolder = arg
  elif opt in ['-r','--release']:
    release = arg
  elif opt in ['-e','--env']:
    env = arg
print baseFolder
print release
print env
os.chdir("/home/automation_testing/slave/workspace/AutomatedDBUpdater")
#automatedDBExecutor = AutomatedDBExecutor("/home/automation_testing/slave/workspace/AutomatedDBUpdater/database_scripts", "release1", "dev")
automatedDBExecutor = AutomatedDBExecutor(baseFolder, release, env)

automatedDBExecutor.processReleaseScriptsMetaData()
automatedDBExecutor.processReleaseScripts()

