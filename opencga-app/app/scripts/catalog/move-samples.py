#!/usr/bin/env python

from pymongo import MongoClient
from pyCGA.opencgarestclients import OpenCGAClient
import os.path
import argparse

parser = argparse.ArgumentParser(description='Move samples across studies. The studies need to belong to the same '
                                             'project.')
parser.add_argument('host', metavar='host', help='Mongo host and port. Example: localhost:27017.')
parser.add_argument('database', metavar='database', help='Mongo database name. Example: opencga_catalog.')
parser.add_argument('sourceStudy', metavar='sourceStudy', type=long,
                    help='Study id where the samples will be moved from.')
parser.add_argument('targetStudy', metavar='targetStudy', type=long,
                    help='Study id where the samples will be moved to.')
parser.add_argument('samples', metavar='sample', nargs='+',
                    help='Sample names that need to be moved across studies.')
args = parser.parse_args()

mongoHost = "mongodb://" + args.host
databaseName = args.database
sourceStudyId = args.sourceStudy
targetStudyId = args.targetStudy

print ("Main parameters received:"
       + "\n\thost: " + args.host
       + "\n\tdatabase: " + args.database
       + "\n\tsource study id: " + str(sourceStudyId)
       + "\n\ttarget study id: " + str(targetStudyId))

db = MongoClient(mongoHost)
client = db[databaseName]

configuration = {
    "version": "v1",
    "rest": {
        "hosts": ["http://localhost:8080/opencga"]
    }
}
opencga_client = OpenCGAClient(configuration=configuration, user='xx', pwd='xx')
# opencga_client = OpenCGAClient(configuration=configuration, session_id='xx')

# 0. Check that the studies belong to the same projectId
studies = []
for study in client['study'].find(
    {
        'id': {
            '$in': [sourceStudyId, targetStudyId]
        }
    }, {
        '_projectId': 1
    }):
    studies.append(study)

if len(studies) < 2:
    print("Some studies were not found.")
    exit(1)
if studies[0]['_projectId'] != studies[1]['_projectId']:
    print("The studies do not belong to the same project")
    exit(1)

for sampleName in args.samples:
    # 1. Look for the sample in both studies
    sampleSource = None
    sampleTarget = None
    for sample in client['sample'].find({
        'name': sampleName,
        '_studyId': {
            '$in': [sourceStudyId, targetStudyId]
        }}):
        if sample['_studyId'] == sourceStudyId:
            sampleSource = sample
        else:
            sampleTarget = sample

    if sampleSource == None:
        print("The sample " + sampleName + " could not be found in the source study. Moving on to the next sample.")
        continue

    # 2. Look for the files associated to the sample
    files = []
    fileIds = []
    filePaths = []
    for file in client['file'].find({
        'samples.id': sampleSource['id'],
        '_studyId': sourceStudyId
    }):
        files.append(file)
        fileIds.append(file['id'])
        filePaths.append(file['path'])

    # 3. Check that none of the files exist in the target study
    for file in client['file'].find({
        '_studyId': targetStudyId,
        'path': {
            '$in': filePaths
        }}):
        print("At least, one file related with sample " + sampleName + " exists in the target study. File "
              + file['id'] + ". Moving on to the next sample.")
        continue

    # 4. Move files to target study
    # 4.1. First, we create a set of parent paths to avoid doing unnecessary calls to REST
    path_set = set()
    for path in filePaths:
        path_set.add(os.path.dirname(path) + "/")

    # 4.2. Create the parents using via REST
    print('Recreating parent directories in target study')
    for path in path_set:
        body = {
            'path': path,
            'parents': True,
            'directory': True
        }
        opencga_client.files.create(body, study=targetStudyId)

    if len(fileIds) > 0:
        print('Moving ' + str(len(fileIds)) + ' files to target study')
        client['file'].update({
            '_studyId': sourceStudyId,
            'id': {
                '$in': fileIds
            }
        }, {
            '$set': {'_studyId': targetStudyId}
        }, multi=True)

    # 5. sampleTarget exist?
    if sampleTarget == None:
        # Move sampleSource to studyTarget
        client['sample'].update({
            'id': sampleSource['id'],
            '_studyId': sourceStudyId
        }, {
            '$set': {'_studyId': targetStudyId}
        })
    else:
        # Remove sample from source study
        client['sample'].remove({
            'id': sampleSource['id'],
            '_studyId': sourceStudyId
        })

        # Update file references to point to the sample id from the target study
        client['file'].update({
            'samples.id': sampleSource['id'],
            '_studyId': targetStudyId
        }, {
            '$set': {
                'samples': [{
                    'id': sampleTarget['id']
                }]
            }
        }, multi=True)
