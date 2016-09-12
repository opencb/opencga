import json
import logging
import os
import datetime
from pyCGA.RestExecutor import WS

__author__ = 'antonior, mparker'


# HOST EXAMPLE
# host = 'http://XX.XX.XX.XXX:XXXX/'


class Users(WS):
    """
    This class contains method for users ws (i.e, login, logout, create new user...)
    """

    def login_method(self, userId, pwd, **options):
        """
        This is the method for login

        :rtype : list of dict
        :param userId: user id
        :param pwd: password for the user
        """
        data = {"password": pwd}

        return self.general_method("users", "login", item_id1=userId, data=data, **options)

    def logout_method(self, userId, **options):
        """
        This method logout the user

        :param userId: user id
        """

        return self.general_method("users", "logout", item_id1=userId, **options)

    def change_password(self, userId, password, new_password, **options):
        """

        method to change password

        :param userId: user ID
        :param password: Old password
        :param new_password: New password
        """
        return self.general_method("users", "change-password", item_id1=userId, password=password,
                                   npassword=new_password, **options)

    def reset_password(self, userId, email, **options):
        """

        method to reset password - CURRENTLY MAIL SERVER IS NOT WORKING

        :param userId: user ID
        :param email: User email to receive the new password
        """

        return self.general_method("users", "reset-password", item_id1=userId, email=email, **options)

    def create_user(self, userId, name, email, organization, password, **options):
        """

        method to create a user

        :param userId: user id
        :param name: name
        :param email: email
        :param organization: organization name
        :param password: user password
        """

        return self.general_method("users", "create", email=email, userId=userId, name=name, organization=organization,
                                   password=password, **options)

    def change_email(self, userId, nemail, **options):
        """

        method to change email

        :param sid: session id
        :param nemail: new email
        :param userId: user id
        """

        return self.general_method("users", "change-email", item_id1=userId, nemail=nemail, **options)

    def delete(self, userId, **options):
        """

        method to delete user

        :param userId: user id
        """
        return self.general_method("users", "delete", item_id1=userId, **options)


class Files(WS):
    """
    This class contains method for files ws (i.e, link, create)
    """

    def share(self, userId, fileId, read=True, write=False, delete=False, unshare=False, **options):

        """

        Method to share files

        :param userId: id of the user this file will be shared
        :param fileId: File id - Notice this is the internal id in Catalog
        :param read: True/False - If True the user could read the file
        :param write: True/False - If True the user could write the file
        :param delete: True/False - If True the user could delete the file
        :param unshare: True/False - If True the file will be unshared for this user
        """
        return self.general_method("files", "share", item_id1=fileId, unshare=str(unshare).lower(), userId=userId,
                                   read=str(read).lower(), write=str(write).lower(), delete=str(delete).lower(),
                                   **options)

    def update(self, fileId, **options):
        """

        Method to update the a file.

        :param fileId: id of file
        :param options: Kargs where the keys are the name of the file properties and the values the new values for those
        properties.
        """

        return self.general_method("files", "update", item_id1=fileId, **options)

    def search(self, studyId, **options):
        """

        Method to search files based in a dictionary "options"

        :param studyId: study id
        :param options: Kargs where the keys are the name of the file properties used to search.
        """
        return self.general_method("files", "search", studyId=studyId, **options)

    def create_folder(self, studyId, folder, **options):
        """

        This is the method create a folder in the DB

        :param studyId: study to associate file to
        :param folder: "path in the DB"
        """
        return self.general_method(ws_category1="files", action="create-folder", studyId=studyId, folders=folder,
                                   **options)

    def link(self, studyId, uri, path, description="", parents=False, calculateChecksum=False, createFolder=True,
             **options):
        """
        This is the method for linking files and folders

        :param studyId: study to associate file to
        :param uri: full path to file on file system
        :param path: path in the DB
        :param description: Folder description
        :param parents: True/False
        :param calculateChecksum: True/Flase
        :param createFolder: True/False If true will create the folder before to link the file
        """

        if createFolder:
            Files.create_folder(self, studyId, path)

        return self.general_method(ws_category1="files", action="link", path=path, uri=uri, studyId=studyId,
                                   description=description, parents=str(parents).lower(),
                                   calculateChecksum=str(calculateChecksum).lower(), **options)

    def info(self, fileId, **options):
        """
        Method to get information of a particular file

        :param fileId: file Id
        """
        return self.general_method(ws_category1="files", action="info", item_id1=fileId, **options)

    def delete(self, fileId, **options):
        """
        Method to delete a particular file/foler

        :param fileId: file Id
        """
        return self.general_method(ws_category1="files", action="delete", item_id1=fileId, **options)

    def unlink(self, fileId, **options):
        """
        Method to unlink a particular file/foler

        :param fileId: file Id
        """
        return self.general_method(ws_category1="files", action="unlink", fileId=fileId, **options)

    def update_file_post(self, fileId, json_file=None, data=None, **options):
        """
        Method to update the sampleId of a file

        :param json_file:
        :param fileId: id of file
        :param sampleId: id os sample
        """
        if data is None and json_file is None:
            raise Exception("please provide a json file or a data")

        if data is None:
            fd = open(json_file)
            data = json.load(fd)
            fd.close()

        return self.general_method(ws_category1="files", action="update", item_id1=fileId, data=data, **options)

    def relink(self, fileId, uri, calculateChecksum=False, **options):
        """
        method to relink (move a file)

        :param fileId: Id of file
        :param uri: new path to file on filesystem
        """

        return self.general_method(ws_category1="files", action="relink", item_id1=fileId, uri=uri,
                                   calculateChecksum=str(calculateChecksum).lower(), **options)

    def index(self, fileId, outdirId, annotate, **options):
        """

        index a file

        :param annotate: True/False
        :param fileId: file Id
        :param outdirId: Output directory of the indexed
        """

        return self.general_method(ws_category1="files", action="index", item_id1=fileId, outdirId=outdirId,
                                   annotate=str(annotate).lower(), **options)

    def refresh(self, fileId, **options):
        """
        refresh metatadata from a file or folder - returns updated files

        :param fileId: File If
        """

        return self.general_method(ws_category1="files", action="refresh", item_id1=fileId, **options)

    def variants(self, fileId, **options):
        """
        get variants from a vcf or gvcf file

        :param options: Options to select the variants
        :param fileId: file Id
        """

        return self.general_method(ws_category1="files", action="variants", item_id1=fileId, **options)

    def alignments(self, fileId, **options):
        """
        get alignments from a bam file

        :param options: Options to select the variants
        :param fileId: file Id
        """

        return self.general_method(ws_category1="files", action="alignments", item_id1=fileId, **options)

    def list(self, fileId, **options):
        """

        list a file

        :param fileId: file Id
        """

        return self.general_method(ws_category1="files", action="list", item_id1=fileId, **options)

    def set_header(self, fileId, header, **options):
        """

        Set the header of a file

        :param fileId: file Id
        :param header: new header
        """

        return self.general_method(ws_category1="files", action="set_header", item_id1=fileId, header=header,
                                   **options)

    def grep(self, fileId, pattern, ignorecase=False, multi=True, **options):
        """

        grep the contents of a file

        :param pattern: Pattern to search
        :param ignorecase: Boolean. Ignore case in grep acction
        :param multi:  Boolean. If it is false will return only the first one
        :param fileId: File id
        """

        return self.general_method(ws_category1="files", action="grep", item_id1=fileId, pattern=pattern,
                                   ignorecase=str(ignorecase).lower(), multi=str(multi).lower(), **options
                                   )


class Variables(WS):
    """

    Class to query variables
    """
    def create(self, studyId, name, json_file=None, data=None, **options):
        """

        This create a VariableSet using a json file, with the format is defined by opencga (this is a post method)

        :param studyId:
        :param json_file:
        """
        if data is None and json_file is None:
            raise Exception("please provide a json file or a data")

        if data is None:
            fd = open(json_file)
            data = json.load(fd)
            fd.close()

        return self.general_method(ws_category1="variableSet", action="create", data=data, studyId=studyId, name=name,
                                   **options)

    def delete(self, variable_set_id, **options):
        """

        This delete one VariableSet given a variableSetId

        :param variable_set_id: Variable Set Id
        """

        return self.general_method(ws_category1="variableSet", action="delete", item_id1=variable_set_id, **options)

    def search(self, studyId, **options):
        """

        Method to search Variable Sets based in a dictionary "options"

        :param variable_set_id: Variable Set Id
        """

        return self.general_method(ws_category1="variableSet", action="search", studyId=studyId, **options)


class Samples(WS):
    def create(self, studyId, name, source, description, **options):
        """

        method to create a sample

        :param source: source
        :param description: description
        :param studyId: studyId
        :param name: name
        :param sampleId:
        :param individualId:
        """

        return self.general_method(ws_category1="samples", action="create", studyId=studyId, name=name,
                                   source=source, description=description, **options)

    def update(self, sampleId, **options):
        """

        method to do simple update of sample via get method

        :param sampleId: Sample Id
        :param options: Options will be updated
        """

        return self.general_method(ws_category1="samples", action="update", item_id1=sampleId, **options)

    def update_post(self, sampleId, data=None, **options):
        """

        method to do simple update of sample via get method

        :param sampleId: Sample Id
        :param options: Options will be updated
        """

        return self.general_method(ws_category1="samples", action="update", item_id1=sampleId, data=data, **options)

    def search(self, studyId, **options):
        """

        Method to search Samples based in a dictionary "options"

        :param studyId: study id
        :param options: Kargs where the keys are the name of the file properties used to search.
        """
        return self.general_method(ws_category1="samples", action="search", studyId=studyId, **options)

    def search_by_annotation(self, studyId, variableSetName, *queries):
        """

        This method search across samples using the annotation

        :param queries: A list of queries for the annotation sets, <VariableId>:<operator><value>,<value>...
        (the queries are built as ANDs and the list of values as ORs). Examples: NAME:=Luke,Leia,Vader AGE:>20
        :param studyId: Study Id
        """

        variable = Variables()
        v_id = variable.search(studyId=studyId, name=variableSetName)[0]["id"]

        return self.search(studyId=studyId, variableSetId=str(v_id), annotation=";".join(queries))

    def annotate(self, sample_id, variableSetName, annotationSetName, studyId, json_file=None, data=None, update=True,
                 **options):
        """
        This annotate a sample using a json file (this is a post method)

        :param sample_id:
        :param variable_set_id:
        :param json_file:
        """
        if data is None and json_file is None:
            raise Exception("please provide a json file or a data")

        if data is None:
            fd = open(json_file)
            data = json.load(fd)
            fd.close()

        variable = Variables()
        variableSetId = str(variable.search(studyId=studyId, name=variableSetName)[0]["id"])

        if update:
            for annt_set in self.info(str(sample_id))[0]["annotationSets"]:
                if annt_set["variableSetId"] == int(variableSetId):
                    annotationSetName = annt_set["name"]

                    return self.general_method(ws_category1="samples", action="update",
                                               item_id1=str(sample_id), ws_category2="annotationSets",
                                               item_id2=annotationSetName, data=data)

        annotateSetName = annotationSetName + "_" + str(datetime.datetime.now()).replace(" ", "_").replace(":", "_").replace(".","_")

        return self.general_method(ws_category1="samples", action="create", item_id1=str(sample_id),
                                   ws_category2="annotationSets", variableSetId=variableSetId,
                                   annotateSetName=annotateSetName, data=data, **options)

    def info(self, sampleId, **options):
        """

        Method to get the sample information

        :param sampleId: Sample Id
        """

        return self.general_method(ws_category1="samples", action="info", item_id1=sampleId, **options)

    def delete(self, sampleId, **options):
        """

        method to delete an sample

        :param sampleId: Sample Id
        """

        return self.general_method(ws_category1="samples", action="delete", item_id1=sampleId, **options)

    def share(self, userId, sampleId, read=True, write=False, delete=False, unshare=False, **options):

        """

        Method to share files

        :param userId: id of the user this file will be shared
        :param sampleId: File id - Notice this is the internal id in Catalog
        :param read: True/False - If True the user could read the file
        :param write: True/False - If True the user could write the file
        :param delete: True/False - If True the user could delete the file
        :param unshare: True/False - If True the file will be unshared for this user
        """
        return self.general_method("samples", "share", item_id1=sampleId, unshare=str(unshare).lower(), userId=userId,
                                   read=str(read).lower(), write=str(write).lower(), delete=str(delete).lower(),
                                   **options)


class Individuals(WS):
    def create(self, studyId, name, family, fatherId, motherId, gender, **options):
        """
        method for creating an individual

        :param studyId: studyid
        :param name: name of individual
        :param family: specify 0 if no family
        :param fatherId: specify as 0 if no fatherid
        :param motherId: specify as 0 if no motehrid
        :param gender: MALE, FEMALE or UNKNOWN
        """
        if fatherId is None:
            fatherId = "0"
        if motherId is None:
            motherId = "0"

        gender = gender.upper()
        if gender != "MALE" and gender != "FEMALE":
            gender = "UNKNOWN"

        return self.general_method(ws_category1="individuals", action="create", name=name, family=family, fatherId=fatherId,
                                   motherId=motherId, gender=gender, studyId=studyId, **options)

    def search(self, studyId, **options):
        """

        :param studyId:
        """

        return self.general_method(ws_category1="individuals", action="search", studyId=studyId, **options)

    def info(self, individualId, **options):
        """

        method to get individual information

        :param individualId:
        """

        return self.general_method(ws_category1="individuals", action="info", item_id1=individualId, **options)

    def delete(self, individualId, **options):
        """

        method to delete an individual

        :param individualId:
        :param sid:
        :return:
        """
        return self.general_method(ws_category1="individuals", action="delete", item_id1=individualId, **options)

    def annotate(self, individual_id, variableSetName, annotationSetName, studyId, json_file=None, data=None,
                 update=True):
        """
        This annotate a individual using a json file (this is a post method)

        :param individual_name:
        :param variable_set_id:
        :param json_file:
        """
        if data is None and json_file is None:
            logging.error("please provide a json file or a data")
            raise Exception("please provide a json file or a data")

        if data is None:
            fd = open(json_file)
            data = json.load(fd)
            fd.close()

        variable = Variables()
        variableSetId = str(variable.search(studyId=studyId, name=variableSetName)[0]["id"])

        if update:
            for annt_set in self.info(str(individual_id))[0]["annotationSets"]:
                if annt_set["variableSetId"] == int(variableSetId):
                    annotationSetName = annt_set["name"]

                    return self.general_method(ws_category1="individuals", action="update",
                                               item_id1=str(individual_id), ws_category2="annotationSets",
                                               item_id2=annotationSetName, data=data)

        annotationSetName = annotationSetName + "_" + str(datetime.datetime.now()).replace(" ", "_").replace(":", "_")

        return self.general_method(ws_category1="individuals", action="create", item_id1=str(individual_id),
                                   ws_category2="annotationSets", variableSetId=variableSetId,
                                   annotateSetName=annotationSetName, data=data)

    def search_by_annotation(self, studyId, variableSetName, *queries, **options):
        """

        :param queries: A list of queries for the annotation sets, <VariableId>:<operator><value>,<value>...
        (the queries are built as ANDs and the list of values as ORs). Examples: NAME:=Luke,Leia,Vader AGE:>20
        :param studyId:
        """

        variable = Variables()
        v_id = variable.search(studyId=studyId, name=variableSetName)[0]["id"]

        return self.search(studyId=studyId, variableSetId=str(v_id), annotation=";".join(queries), **options)


class Projects(WS):
    """
    This class contains method for projects ws (i.e, create, files, info)
    """

    def create(self, userId, name, alias, description, organization, **options):
        """

        :param userId:
        :param name:
        :param alias:
        :param description:
        :param organization:
        """

        return self.general_method(ws_category1="projects", action="create", userId=userId,  name=name, alias=alias,
                                   description=description, organization=organization, **options
                                   )

    def info(self, projectId, **options):
        """
        method to get project information

        :param projectId:
        """

        return self.general_method(ws_category1="projects", action="info", item_id1=projectId, **options)

    def update(self, projectId, **options):
        """
        updates a project

        :param name:
        :param description:
        :param organization:
        :param status:
        :param attributes:
        :param projectId:
        :param sid:
        """

        return self.general_method(ws_category1="projects", action="update", item_id1=projectId, **options)

    def delete(self, projectId, **options):
        """
        deletes a project

        :param projectId:
        """

        return self.general_method(ws_category1="projects", action="delete", item_id1=projectId, **options)

    def studies(self, projectId, **options):
        """
        Returns information on studies contained in the project

        :param projectId:
        """

        return self.general_method(ws_category1="projects", action="studies", item_id1=projectId, **options)


class Studies(WS):
    """
    This class contains method for studies ws (i.e, status, files, info)
    """

    def search(self, **options):
        """

        Method to search studies based in a dictionary "options"

        :param options: Kargs where the keys are the name of the file properties used to search.
        """
        return self.general_method(ws_category1="studies", action="search", **options)

    def create(self, projectId, name, alias, **options):
        """

        :param projectId:
        :param name:
        :param alias:
        """

        return self.general_method(ws_category1="studies", action="create", projectId=projectId, name=name,
                                   alias=alias, **options)

    def info(self, studyId, **options):
        """

        method to get study info

        :param studyId:
        """

        return self.general_method(ws_category1="studies", action="info", item_id1=studyId, **options)

    def files(self, studyId, **options):
        """

        method to get study files

        :param studyId:
        """

        return self.general_method(ws_category1="studies", action="files", item_id1=studyId, **options)

    def jobs(self, studyId, **options):
        """

        method to get study jobs

        :param studyId:
        """

        return self.general_method(ws_category1="studies", action="jobs", item_id1=studyId, **options)

    def samples(self, studyId, **options):
        """

        method to get study samples

        :param studyId:
        """

        return self.general_method(ws_category1="studies", action="samples", item_id1=studyId, **options)

    def variants(self, studyId, **filters):
        """

        method to get study variants


        :param filters: All the filters will be applied to the variant fetch
        :param studyId: StudyID
        """

        return self.general_method(ws_category1="studies", action="variants", item_id1=studyId, **filters)

    def alignments(self, studyId, **filters):
        """

        method to get study alignments


        :param filters: All the filters will be applied to the variant fetch
        :param studyId: StudyID
        """

        return self.general_method(ws_category1="studies", action="alignments", item_id1=studyId, **filters)

    def status(self, studyId, **options):
        """

        method to get study status

        :param studyId:
        """

        return self.general_method(ws_category1="studies", action="status", item_id1=studyId, **options)

    def update(self, projectId, **options):
        """
        updates a study

        :param projectId:
        :param options:
        """

        return self.general_method(ws_category1="studies", action="update", item_id1=projectId, **options)

    def delete(self, studyId, **options):
        """
        method to get delete study

        :param studyId:
        """

        return self.general_method(ws_category1="studies", action="delete", item_id1=studyId, **options)


class Jobs(WS):
    """
    This class contains method for jobs ws (i.e, create, info)
    """

    def create(self, studyId, name, toolId, jobId, **options):
        """
        create a job

        :param studyId:
        :param name:
        :param toolId:
        """

        return self.general_method(ws_category1="jobs", action="create", name=name, studyId=studyId,
                                   toolId=toolId, jobId=jobId, **options)

    def info(self, jobId, **options):
        """
        delete a job

        :param jobId:
        """
        return self.general_method(ws_category1="jobs", action="info", item_id1=jobId, **options)

    def visit(self, jobId, **options):
        """
        visit a job

        :param jobId:
        """
        return self.general_method(ws_category1="jobs", action="visit", item_id1=jobId, **options)

    def delete(self, jobId, **options):
        """
        delete a job

        :param jobId:
        """

        return self.general_method(ws_category1="jobs", action="delete", item_id1=jobId, **options)

    def create_post(self, studyId, json_file=None, data=None, **options):
        """

        post method - so needs a json input file or data

        :return:
        """
        if data is None and json_file is None:
            raise Exception("please provide a json file or a data")

        if data is None:
            fd = open(json_file)
            data = json.load(fd)
            fd.close()

        return self.general_method(ws_category1="jobs", action="create", studyId=studyId, data=data, **options)

# class Cohorts(WS):
#     """
#     This class contains method for cohorts ws (i.e, link, create)
#     """
#     # TODO: Check this method 2 variables unused
#     def create(self, studyId, name, variableSetId, type, sampleIds):
#         """
#
#         :param studyId:
#         :param sid:
#         :param name:
#         :param variableSetId:
#         :param type: can be PAIRED, CASE_CONTROL, CASE_SET, CONTROL_SET, PAIRED_TUMOR, FAMILY, TRIO, COLLECTION
#         :param sampleIds:
#         """
#
#         url = os.path.join(self.pre_url, "cohorts",
#                            "create?sid=" + self.session_id + "&name=" + name + "&type=" + type + "&sampleIds=" + sampleIds)
#         result = self.run_ws(url)
#         return result["id"]
#
#     def samples(self, cohortId):
#         """
#
#         method to get samples that are part of a cohort
#
#         :param cohortId:
#         :return: full result which can be looped over
#         """
#
#         url = os.path.join(self.pre_url, "cohorts", cohortId, "samples?sid=" + self.session_id)
#         result = self.run_ws(url)
#         return result
#
#     def update(self, cohortId, sampleIds):
#         """
#         This will be to add or remove samples from a cohort
#
#         :param cohortId:
#         :param sampleIds: full list of sampleIds, i.e. old ones and new ones (comma separated)
#         """
#
#         url = os.path.join(self.pre_url, "cohorts", cohortId, "update?sid=" + self.session_id + "&samples=" + sampleIds)
#         result = self.run_ws(url)
#         return result
#
#     def info(self, cohortId):
#         """
#         This will be to add or remove samples from a cohort
#
#         :param cohortId:
#
#         """
#
#         url = os.path.join(self.pre_url, "cohorts", cohortId, "info?sid=" + self.session_id)
#         result = self.run_ws(url)
#         return result
#
#     def delete(self, cohortId):
#         """
#
#         method to get samples that are part of a cohort
#
#         :param cohortId:
#         :return: full result which can be looped over
#         """
#
#         url = os.path.join(self.pre_url, "cohorts", cohortId, "delete?sid=" + self.session_id)
#         result = self.run_ws(url)
#         return result
