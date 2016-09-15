from unittest import TestCase
import os

from pyCGA.CatalogWS import Users, Files, Samples, Individuals, Jobs, Variables

file_to_register = '/home/pfurio/Documents/projects/opencb/hpg-bigdata/build/data/test.vcf'
folder_to_register = '/home/pfurio/Documents/projects/opencb/hpg-bigdata/build/data'
sample_to_register = 'LP2000268-DNA_A06'
test_folder = 'testFolder/'
model_to_create_annotation_set = ''
data_to_register = ''
host = 'http://localhost:8080'
catalog_instance = 'opencga'
pwd = 'pfurio'
study_id = 'study'
individual_to_register = '500000521'

## Testing login
unregistered_session = {"host": host, "sid": ""}
user = Users(unregistered_session, instance=catalog_instance)

## Testing file registration

class WorkflowTestCase(TestCase):

    def setUp(self):
        unregistered_session = {"host": host, "sid": ""}
        self.user = Users(unregistered_session, instance=catalog_instance)
        sid = self.user.login_method("pfurio", pwd)[0]["sessionId"]
        self.token = {"user": "pfurio", "host": host, "sid": str(sid), "instance": catalog_instance, "debug": True}
        self.file_connector = Files(token=self.token)
        self.sample_connector = Samples(token=self.token)
        self.individual_connector = Individuals(token=self.token)
        self.job_connector = Jobs(token=self.token)
        self.variable_connector = Variables(token=self.token)


    def test_variables(self):
        self.variable_connector.create(study_id, "myVariableSet", json_file="/home/pfurio/test/variable.json")
        print "hola"

    # def tearDown(self):
    #     self.sample_connector.delete(sample_to_register, force='true')



    # def test_register_file(self):

        # create_folder = self.file_connector.create_folder(studyId=study_id, folder=test_folder)
        # registered_files = self.file_connector.link(studyId=study_id, uri=file_to_register,
        #                                             path=test_folder, parents=True, createFolder=False)
        # print registered_files
        # self.assertEqual(len(registered_files), 1)
        # self.assertEqual(registered_files[0]['uri'], 'file://' + file_to_register)
        #
        # files = self.file_connector.search(study_id, name="test.vcf")
        #
        # file = self.file_connector.update("test.vcf", name="test1.vcf", description="This is the new description")
        #
        # # self.file_connector.unlink("test.vcf")
        # for line in self.file_connector.general_method(ws_category1='files',
        #                                                action='download',
        #                                                item_id1=os.path.basename(file_to_register)):
        #     print(line)


    # def test_create_sample(self):
    #     created_sample = self.sample_connector.create(studyId=study_id, name=sample_to_register,
    #                                                   description=sample_to_register, source='Manual')
    #     sample = self.sample_connector.search(study_id, name=sample_to_register)
    #
    #     # sample = self.sample_connector.delete(sample_to_register)
    #     # sample = self.sample_connector.search(study_id, name=sample_to_register)
    #     newSampleName = "newSampleName"
    #     self.sample_connector.update(sample_to_register, name=newSampleName, description=newSampleName,
    #                                  source="Automatic")
    #     self.assertEqual(created_sample[0]['name'], sample_to_register)


    def test_create_jobs(self):
        data = {
            "name": "jobName",
            "toolName": "toolName",
            "description": "This is my job description",
            "execution": "tophat.sh ",
            "params": {},
            "startTime": 123123,
            "endTime": 24124124,
            "commandLine": "samtools view acceptedhits.bam",
            "status": "TRASHED",
            "statusMessage": "job has been sent to trash",
            "outDirId": "4",
            "input": [
                0
            ],
            "output": [
                0
            ],
            "attributes": {},
            "resourceManagerAttributes": {}
        }
        job = self.job_connector.create_post(study_id, data=data)
        print 5

    # def test_create_individual(self):
    #     created_individual = self.sample_connector.create(studyId=study_id, name=individual_to_register)



















