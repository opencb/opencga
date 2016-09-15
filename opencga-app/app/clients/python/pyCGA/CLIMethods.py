from __future__ import print_function

import getpass
import json
import sys
from hashlib import sha1
from pyCGA.CatalogWS import Users, Variables, Files, Individuals, Samples
from pyCGA.Exceptions import ServerResponseException
from pyCGA.ExpandedMethods import check_user_acls, link_file_and_update_sample, AnnotationSet
from pyCGA.Utils.AvroSchema import AvroSchemaFile

__author__ = 'antonior'


class Methods:
    def __init__(self):
        pass

    # USERS METHODS
    @staticmethod
    def users_login(args):
        """
        Call: python pyCGA [--host hostname] notSid users login --user user --pwd password

        :param args:
        """
        user = Users({"host": args.host, "sid": ""}, instance=args.instance)
        try:
            if args.pwd:
                pwd = sha1(args.pwd).hexdigest()
            else:
                pwd = sha1(getpass.getpass()).hexdigest()
            sid = user.login_method(args.user, pwd)[0].get("sessionId")
            print(sid)
            return sid
        except ServerResponseException as e:
            print(str(e), file=sys.stderr)

    @staticmethod
    def users_acl(args):
        """
        Call: python pyCGA [--host hostname] notSid users login --user user --pwd password

        :param args:
        """
        acls = check_user_acls(args.userId)

        for acl in acls:
            print("You have been granted access in the study: " + str(acl) + ", and samples: ")
            for sample in acls[acl]["samples"]:
                print("\t" + str(sample))

    @staticmethod
    def user_logout(args):
        """
        Call: python pyCGA [--host hostname] notSid users login --user user --pwd password

        :param args:
        """
        user = Users()
        user.logout_method(args.user)
        print("Successfully Log out")

    @staticmethod
    def users_create(args):
        """
        Call: python pyCGA [--host hostname] sid users create

        :param args:
        """
        user = Users()
        try:
            user_id = user.create_user(args.user, args.name, args.email, args.org, args.pwd)[0].get("id")
            print(user_id)
        except ServerResponseException as e:
            print(str(e), file=sys.stderr)

    @staticmethod
    def users_delete(args):
        """
        Call: python pyCGA [--host hostname] sid users delete --userId username

        :param args:
        """
        user = Users()
        try:
            userId = user.delete(args.userId)
            print(userId)
        except ServerResponseException as e:
            print(str(e), file=sys.stderr)

    @staticmethod
    def users_change_password(args):
        pass

    @staticmethod
    def users_reset_password(args):
        pass

    @staticmethod
    def users_change_email(args):
        pass

    ##FILES METHODS
    @staticmethod
    def files_link_file(args):
        
        if args.sampleId:
            sample_id = args.sampleId
        else:
            sample_id = None
             
        try:
            link_file_and_update_sample(args.uri, args.path, args.studyId, sample_id)
            
        except ServerResponseException as e:
            print(e, file=sys.stderr)

    @staticmethod
    def files_query_stats(args):
        """
        Method to query stats stored with files.... generic so user has to specify certain aspects

        Call: python pyCGA query stats --fileType (ALIGNMENT or VARIANTS) --query (e.g BAM_HEADER_MACHINE.MACHINE)

        :param args:
        """
        file_instance = Files()
        # TODO: move to expanded methods
        try:

            results = file_instance.search(studyId=str(args.studyId), bioformat=args.fileType,
                                                include="projects.studies.files.name,projects.studies.files.stats." +args.query)
            # print(json.dumps(result, indent=4))

            levels = args.query.split('.', 1)
            level1 = levels[0]
            level2 = levels[1]
            print("FILENAME\t" + level2)
            for result in results:
                if "stats" in result:
                    try:
                        if isinstance(result["stats"][level1], list) and result["stats"][level1]!=[]:
                            print(result["name"] + "\t" + ",".join(map(str, [term[level2] for term in result["stats"][level1]])))
                        elif isinstance(result["stats"][level1], dict):
                            print(result["name"] + "\t" + str(result["stats"][level1][level2]))
                        else:
                            print(result["name"] + "\t" + "ERROR")
                    except:
                        print(result["name"] + "\t" + "ERROR")
                else:
                    print("ERROR")
        except ServerResponseException as e:
            print(str(e), file=sys.stderr)

    @staticmethod
    def files_search_by_sample(args):

        file_instance = Files()
        sample = Samples()
        for sampleName in args.sampleName:
            try:
                sampleId = sample.search(args.studyId, name=sampleName)[0]["id"]
                results = file_instance.search(sampleIds=str(sampleId), studyId=args.studyId,
                               bioformat=args.fileType,
                               include="projects.studies.files.path,projects.studies.files.sampleIds",
                               path=args.path
                               )

                for r in results:
                    print(sampleName +"\t"+ r["path"])

            except ServerResponseException as e:
                print(str(e), file=sys.stderr)

    @staticmethod
    def files_search_by_type(args):
        pass

    @staticmethod
    def files_search_by_path(args):
        pass

    @staticmethod
    def files_info(args):
        file_instance = Files()
        try:
            result = file_instance.info(args.fileId)
            print(result["stats"][args.subset][args.field])
            # print(result[args.field])
        except ServerResponseException as e:
            print(str(e), file=sys.stderr)

    @staticmethod
    def files_index(args):
        # this has two parts, first get fileId given filename and 2nd run indexing
        file_instance = Files()
        # TODO: check outdir parameter empty
        try:
            result = file_instance.index(str(args.fileId), args.outdir, args.annotate)
            print(result)
        except ServerResponseException as e:
            print(str(e), file=sys.stderr)

    @staticmethod
    def files_variants(args):
        pass

    @staticmethod
    def files_alignemnts(args):
        pass

    @staticmethod
    def files_list(args):
        pass

    @staticmethod
    def files_update_sample_id(args):
        pass

    @staticmethod
    def files_set_header(args):
        pass

    @staticmethod
    def files_grep_content(args):
        pass

    # INDIVIDUALS METHODS
    @staticmethod
    def individuals_create(args):
        # 1st create individual
        # TODO: Change this method to load the individuals from phenotips format or ped
        """
        Call: python pyCGA [--host hostname] sid individuals create --studyId studyId --name paticipantId --gender --fatherId --motherId --family --wellIds

        :param args:
        """
        individual = Individuals()
        sample = Samples()
        try:
            individualId = individual.create(args.studyId, args.name, args.family, args.fatherId,
                                             args.motherId, args.gender)[0]["id"]
            well_ids = args.wellIds.split(",")
            for well in well_ids:
                try:
                    sampleId = sample.search(args.studyId, name=well)[0]["id"]
                    print(sampleId)
                    try:
                        sample.update(str(sampleId), individualId=str(individualId))
                    except ServerResponseException as e:
                        print(str(e), file=sys.stderr)
                except ServerResponseException as e:
                    print(str(e), file=sys.stderr)
        except ServerResponseException as e:
            print(str(e), file=sys.stderr)

    @staticmethod
    def individuals_search(args):
        """
        Call: python pyCGA [--host hostname] sid individuals search --studyId studyId --name particiantId

        :param args:
        """
        individual = Individuals()
        sample = Samples()

        individual_results = individual.search(studyId=args.studyId, name=args.name)
        for individual_result in individual_results:
            gender = individual_result["gender"]
            individualId = individual_result["id"]
            print("ParticipantId in Catalog: "+ str(individualId))
            try:
                sample_result = sample.search(studyId=args.studyId, individualId=str(individualId))

                for sample in sample_result:
                        print("Sample Name: " + sample["name"])

            except ServerResponseException as e:
                print(str(e), file=sys.stderr)



    @staticmethod
    def individuals_search_by_annotation(args):
        individual = Individuals()
        pedigree = []
        try:
            result = individual.search_by_annotation(args.studyId, args.variableSetName, *args.queries)
            for r in result:
                pedigree.append(AnnotationSet(*[annotation for annotation in r["annotationSets"] if args.variableSetName in annotation["attributes"]["annotateSetName"]][0]["annotations"]).get_json())

        except ServerResponseException as e:
            print(str(e), file=sys.stderr)

        fdw = open(args.outfile, "w")
        json.dump(pedigree, fdw, indent=True)
        fdw.close()

    @staticmethod
    def individuals_annotate(args):
        try:
            individual = Individuals()
            individual_id = individual.search(args.studyId, name=args.individualName)[0]["id"]
            result = individual.annotate(str(individual_id), args.variableSetName, args.annotationSetName, args.studyId,
                                         args.jsonFile)
            print(result[0]["id"])

        except ServerResponseException as e:
            print(str(e), file=sys.stderr)

    # SAMPLES METHODS
    @staticmethod
    def samples_search_by_name(args):
        sample = Samples()
        try:
            result = sample.search(args.studyId, name=args.name)
            print(result)
        except ServerResponseException as e:
            print(str(e), file=sys.stderr)

    @staticmethod
    def samples_share(args):
        sample = Samples()
        files = Files()
        ids_sample = [s.rstrip("\n").split("\t")[0] for s in open(args.samples).readlines()]
        for id in ids_sample:
            # This line return the root folder
            root = str(files.search(studyId=args.studyId, sampleIds=id, description="INTAKE")[0]["id"])
            files.share(args.user, root)

            try:
                sample.share(args.user, id, include="projects.studies.samples.name")
            except ServerResponseException as e:
                print(str(e), file=sys.stderr)

    @staticmethod
    def samples_search_by_annotation(args):
        sample = Samples()
        try:
            result = sample.search_by_annotation(args.studyId, args.variableSetName, *args.queries)
            for r in result:
                # print(int(AnnotationSet(*[annotation for annotation in r["annotationSets"] if args.variableSetName in annotation["attributes"]["annotateSetName"]][0]["annotations"]).get_json()["id"]))
                print(AnnotationSet(*[annotation for annotation in r["annotationSets"] if args.variableSetName in annotation["attributes"]["annotateSetName"]][0]["annotations"]).get_json()["name"])
        except ServerResponseException as e:
            print(str(e), file=sys.stderr)

    @staticmethod
    def samples_annotate(args):
        try:
            sample = Samples()
            sample_id = sample.search(args.studyId, name=args.sampleName)[0]["id"]
            result = sample.annotate(sample_id, args.variableSetName, args.annotationSetName, args.studyId,
                                         args.jsonFile)
            print(result)

        except ServerResponseException as e:
            print(str(e), file=sys.stderr)

    @staticmethod
    def create_samples(args):
        try:
            sample = Samples()
            result = sample.create(args.studyId, args.name, "Manually_added", "")[0]
            print(result)

        except ServerResponseException as e:
            print(str(e), file=sys.stderr)

    @staticmethod
    def variables_create(args):
        """

        This method create a variable set from a AVRO schema or OPENCGA schema

        :param args:
        """
        try:
            variable = Variables()
            if args.format == "AVRO":
                schema = AvroSchemaFile(args.json)
                data = schema.convert_variable_set(schema.data)
                variable_id = variable.create(args.studyId, args.name, data=data)[0]["id"]

            else:
                variable_id = variable.create(args.studyId, args.name, json_file=args.json)[0]["id"]

            print(variable_id)

        except ServerResponseException as e:
            print(str(e), file=sys.stderr)

    # Inter-classes methods
    @staticmethod
    def id_converter(studyID, inputIDType, outputIDType, IdBatch=None, Id=None):
        if IdBatch is None and Id is None:
            raise Exception("Error, it was not found any id to convert")
        if Id:
            ids = Id
        else:
            ids = [line.split("\t")[0].replace("\n","") for line in open(IdBatch).readlines()]

        for id in ids:
            if inputIDType == "CatalogIndividualId":
                if outputIDType == "IndividualName":
                    individual = Individuals()
                    individual_info = individual.info(id)[0]
                    yield [id, str(individual_info["name"])]
                elif outputIDType == "SampleName":
                    sample = Samples()
                    sample_info = sample.search(studyID, individualId=id)

                    for info in sample_info:
                        yield [id, info["name"]]

                elif outputIDType == "CatalogSampleId":
                    sample = Samples()
                    sample_info = sample.search(studyID, individualId=id)

                    for info in sample_info:
                        yield [id, str(info["id"])]

            if inputIDType == "IndividualName":
                if outputIDType == "CatalogIndividualId":
                    individual = Individuals()
                    individual_info = individual.search(studyID, name=id)[0]
                    yield [id, str(individual_info["id"])]
                if outputIDType == "SampleName":
                    sample = Samples()
                    individual = Individuals()
                    individual_info = individual.search(studyID, name=id)[0]
                    sample_info = sample.search(studyID, individualId=str(individual_info["id"]))

                    for info in sample_info:
                        yield [id, info["name"]]

                if outputIDType == "CatalogSampleId":
                    sample = Samples()
                    individual = Individuals()
                    individual_info = individual.search(studyID, name=id)[0]
                    sample_info = sample.search(studyID, individualId=str(individual_info["id"]))

                    for info in sample_info:
                        yield [id, str(info["id"])]

            if inputIDType == "CatalogSampleId":
                if outputIDType == "CatalogIndividualId":
                    sample = Samples()
                    sample_info = sample.info(id)[0]
                    yield [id, str(sample_info["individualId"])]
                if outputIDType == "SampleName":
                    sample = Samples()
                    sample_info = sample.info(id)[0]
                    yield [id, str(sample_info["name"])]
                if outputIDType == "IndividualName":
                    sample = Samples()
                    sample_info = sample.info(id)[0]
                    individual = Individuals()
                    individual_info = individual.info(str(sample_info["individualId"]))[0]
                    yield [id, str(individual_info["name"])]

            if inputIDType == "SampleName":
                if outputIDType == "CatalogIndividualId":
                    sample = Samples()
                    sample_info = sample.search(studyID, name=id)[0]
                    yield [id, str(sample_info["individualId"])]
                if outputIDType == "CatalogSampleId":
                    sample = Samples()
                    sample_info = sample.search(studyID, name=id)[0]
                    yield [id, str(sample_info["id"])]
                if outputIDType == "IndividualName":
                    sample = Samples()
                    sample_info = sample.search(studyID, name=id)[0]
                    individual = Individuals()
                    individual_info = individual.info(str(sample_info["individualId"]))[0]
                    yield [id, str(individual_info["name"])]


