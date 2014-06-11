#! /usr/bin/python -u

import sys, time, os, commands, datetime, xml.dom.minidom as xml
from pymongo import MongoClient
from seth import Daemon

INTERVAL = 3
OPENCGA_ACCOUNTS = "/httpd/bioinfo/opencga/accounts"

# Mongo configuration
mongoHost = "webmaster"
mongoPort = 27017
mongoDatabase = "opencga"
mongoCollection = "accounts"
mongouser = "opencga_user"
mongopass = "Opencga_Pass"

connection = MongoClient(mongoHost, mongoPort)
db = connection[mongoDatabase]
db.authenticate(mongouser, mongopass);
collection = db[mongoCollection]


def getXML():
    status, output = commands.getstatusoutput("qstat -xml")
    doc = xml.parseString(output)
    return doc.getElementsByTagName("job_list")


def getXMLattr(node, attribute):
    return node.attributes[attribute].value


def getTimeMillis():
    return datetime.datetime.now().strftime("%Y%m%d%H%M%S%f")[:-3]


def getLogTime():
    return datetime.datetime.now().strftime("%Y-%m-%d %a %H:%M:%S.%f")[:-3]


def getXMLtag(node, tag):
    return node.getElementsByTagName(tag)[0].childNodes[0].data


def getSGEJobInfo(JB_name):
    arr = JB_name.split('_', 2)
    if len(arr) < 2:
        return (arr[0], arr[0])
    return (arr[0], arr[1])


def getSGEjobs():
    sgeJobs = {}
    for node in getXML():
        (toolName, jobId) = getSGEJobInfo(getXMLtag(node, "JB_name"))
        sgeJobs[jobId] = {
            "toolName": toolName,
            "state": getXMLattr(node, "state"),
            "s": getXMLtag(node, "state"),
        }
        # print (getXMLtag(node,"JB_job_number"))
        # print (getXMLtag(node,"JAT_prio"))
        # print (getXMLtag(node,"JB_name"))
        # print (getXMLtag(node,"JB_owner"))
        ##print (getXMLtag(node,"JAT_start_time"))
        ##print (getXMLtag(node,"queue_name"))
        #print (getXMLtag(node,"slots"))
    return sgeJobs


def updateSGEAccountingJob(mongoJob, position):
    id = mongoJob["id"]
    toolName = mongoJob["toolName"].split('.')[0]
    name = toolName + '_' + id

    status = 'unknown'

    commmandStatus, output = commands.getstatusoutput("qacct -j " + name + " | grep exit_status")
    if "error: job name" not in output:
        value = output.split()[1]
        if value != "0":
            status = 'execution_error'
        else:
            status = 'finished'

    commmandStatus, output = commands.getstatusoutput("qacct -j " + name + " | grep failed")
    if "error: job name" not in output:
        value = output.split()[1]
        if value != "0":
            status = 'queue_error'


    if status != "unknown":
        updateJobStatus(mongoJob, position, status)


def checkSGEAccounting(mongoObject):
    name = mongoObject["status"]

    status, output = commands.getstatusoutput("qacct -j " + name + " | grep failed")
    # print output
    if "error: job name" not in output:
        value = output.split()[1]
        if value != "0":
            position = getMongoObjectIndex(mongoObject)
            updateIndexStatus(mongoObject, position, "error")
            return
        status, output = commands.getstatusoutput("qacct -j " + name + " | grep exit_status")
        print output
        value = output.split()[1]
        if value != "0":
            position = getMongoObjectIndex(mongoObject)
            updateIndexStatus(mongoObject, position, "error")
            return
        else:
            position = getMongoObjectIndex(mongoObject)
            updateIndexStatus(mongoObject, position, "ready")


def getMongojobs():
    projects = collection.aggregate(
        [{"$project": {"accountId": 1, "projects.jobs": 1, "_id": 0}}, {"$unwind": "$projects"}, {"$match": {"projects.jobs.visites": {"$lt": 0}}}])["result"]
    jobs = []
    for project in projects:
        for job in project["projects"]["jobs"]:
            if job["visites"] < 0:
                job["accountId"] = project["accountId"]
                jobs.append(job)
    return jobs


def getMongoObjects():
    buckets = collection.aggregate([{"$project": {"accountId": 1, "buckets.objects": 1, "_id": 0}}, {"$unwind": "$buckets"},
                                    {"$match": {"buckets.objects.status": {"$regex": "indexer_*"}}}])["result"]
    objects = []
    for project in buckets:
        for job in project["buckets"]["objects"]:
            if "indexer_" in job["status"]:
                job["accountId"] = project["accountId"]
                objects.append(job)
    return objects


def getMongoJobIndex(job):
    jobs = collection.find_one({"accountId": job["accountId"], "projects.jobs.id": job["id"]}, {"projects.$": 1})["projects"][0]["jobs"]
    position = 0;
    for j in jobs:
        if j["id"] == job["id"]:
            break
        position += 1
    return str(position)


def getMongoObjectIndex(job):
    jobs = collection.find_one({"accountId": job["accountId"], "buckets.objects.id": job["id"]}, {"buckets.$": 1})["buckets"][0]["objects"]
    position = 0;
    for j in jobs:
        if j["id"] == job["id"]:
            break
        position += 1
    return str(position)


def updateRuning(job, position):
    accountId = job["accountId"]
    jobId = job["id"]
    res = collection.update({"accountId": accountId, "projects.jobs.id": jobId}, {
        "$set": {"projects.$.jobs." + position + ".status": "running", "projects.$.jobs." + position + ".visites": -1, "lastActivity": getTimeMillis()}})
    print(res)


def updateEqw(job, position):
    accountId = job["accountId"]
    jobId = job["id"]
    res = collection.update({"accountId": accountId, "projects.jobs.id": jobId}, {
        "$set": {"projects.$.jobs." + position + ".status": "queue_waiting_error", "projects.$.jobs." + position + ".visites": 0,
                 "lastActivity": getTimeMillis()}})
    print(res)


# def updateFinished(job, position):
# accountId = job["accountId"]
#     jobId = job["id"]
#     jobOutdir = job["outdir"]
#     outdir = OPENCGA_ACCOUNTS + "/" + accountId + "/" + jobOutdir
#     status, output = commands.getstatusoutput("ls -r " + outdir + " | grep -v result.xml | grep -v sge_err.log | grep -v sge_out.log")
#
#     sys.stdout.write(getLogTime() + "\t" + jobId + "\t" + accountId + "\t")
#     res = collection.update({"accountId": accountId, "projects.jobs.id": jobId}, {
#         "$set": {"projects.$.jobs." + position + ".status": "finished", "projects.$.jobs." + position + ".outputData": output.split("\n"),
#                  "projects.$.jobs." + position + ".visites": 0, "lastActivity": getTimeMillis()}})
#     print(res)
#     sys.stdout.write(outdir + "\t")
#     print("")


def updateJobStatus(job, position, status):
    accountId = job["accountId"]
    jobId = job["id"]
    jobOutdir = job["outdir"]
    outdir = OPENCGA_ACCOUNTS + "/" + accountId + "/" + jobOutdir

    print(status)
    if status == "finished":
        statusCommand, outputFiles = commands.getstatusoutput("ls -r " + outdir + " | grep -v result.xml | grep -v sge_err.log | grep -v sge_out.log")
    elif status == "queue_error" or status == "execution_error":
        outputFiles = "sge_err.log\nsge_out.log"

    sys.stdout.write(getLogTime() + "\t" + jobId + "\t" + accountId + "\t")
    res = collection.update({
                                "accountId": accountId,
                                "projects.jobs.id": jobId
                            }, {
                                "$set": {
                                    "projects.$.jobs." + position + ".status": status,
                                    "projects.$.jobs." + position + ".outputData": outputFiles.split("\n"),
                                    "projects.$.jobs." + position + ".visites": 0,
                                    "lastActivity": getTimeMillis()
                                }
                            })
    print(res)
    sys.stdout.write(outdir + "\t")
    print("")


def updateIndexStatus(object, position, status):
    accountId = object["accountId"]
    objectId = object["id"]
    res = collection.update({"accountId": accountId, "buckets.objects.id": objectId},
                            {"$set": {"buckets.$.objects." + position + ".status": status, "lastActivity": getTimeMillis()}})


def task():
    sgeJobs = getSGEjobs()
    mongoJobs = getMongojobs()
    for mongoJob in mongoJobs:
        position = getMongoJobIndex(mongoJob)
        mongoAccountId = mongoJob["accountId"]
        print(mongoJob["id"])
        if mongoJob["id"] in sgeJobs:
            sgeJob = sgeJobs[mongoJob["id"]];
            sys.stdout.write(getLogTime() + "\t" + mongoJob["id"] + "\t" + sgeJob["s"] + "\t" + mongoAccountId + "\t")
            if sgeJob["s"] == "r" and mongoJob["status"] != "running":
                updateRuning(mongoJob, position);
            elif sgeJob["s"] == "Eqw":
                updateEqw(mongoJob, position)
                # elif sgeJob["s"] == "qw":
                # print("qw")
            else:
                print("")
        else:
            updateSGEAccountingJob(mongoJob, position)
            # actualizar mongodb, pq el job no esta en la sge, marcar como terminado, y ok o error  ----> esto no se sabe
            # updateFinished(mongoJob,position)

    mongoObjects = getMongoObjects()
    for mongoObject in mongoObjects:
        print mongoObject["status"]
        checkSGEAccounting(mongoObject)


def mongoDisconnect():
    connection.disconnect()


# #########################################################
# ################Daemon implementation####################
# #########################################################
class MyDaemon(Daemon):
    def __init__(self, pid, log):
        Daemon.__init__(self, pid, stdout=log, stderr=log)

    def run(self):
        """Overrides Daemon().run() with actions you want to daemonize.
        MyDaemon.run() is then called within MyDaemon().start()"""
        print('Starting Deamon!')  # message issued on self.stdout
        while True:
            task()
            time.sleep(INTERVAL)
            #sys.stderr.write('error: unicode write test to stderr\n')
            #sys.stdout.write('write test to stdout\n')

    def shutdown(self):
        mongoDisconnect()
        """Overrides Daemon().shutdown() with some clean up"""
        print("Stopping Daemon!")  # message issued on self.stdout


if __name__ == '__main__':
    daemon = MyDaemon('/opt/opencga/opencga-daemon.pid', '/opt/opencga/opencga-daemon.log')
    if len(sys.argv) == 2:
        if 'start' == sys.argv[1]:
            daemon.start()
        elif 'stop' == sys.argv[1]:
            daemon.stop()
        elif 'restart' == sys.argv[1]:
            daemon.restart()
        else:
            print('Unknown command')
            sys.exit(2)
            sys.exit(0)
    else:
        print("usage: {} start|stop|restart".format(sys.argv[0]))
        sys.exit(2)
