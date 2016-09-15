from pyCGA.CatalogWS import Studies, Samples, Individuals, Variables, Files
__author__ = 'antonior'


def share_samples(studyId, list_of_samples_file, user_id):
    file_instance = Files()
    sample_instance = Samples()
    summary = {}
    fd = open(list_of_samples_file)
    for line in fd:
        aline = line.rstrip("\n").split("\t")
        if aline:
            sample_name = aline[0]
            root_folder = aline[1]
            print(sample_name)
            sample_objs = sample_instance.search(studyId=studyId, name=sample_name)
            for sample_obj in sample_objs:
                sample_id = str(sample_obj["id"])
                folders_to_share = file_instance.search(studyId=studyId, path=root_folder,
                                                        sampleIds=sample_id
                                                        )
                for folder_to_share in folders_to_share:
                    file_id = str(folder_to_share["id"])
                    file_instance.share(userId=user_id, fileId=file_id)

                sample_instance.share(userId=user_id, sampleId=sample_id)
                summary[sample_name] = [folders_to_share]


def get_all_samples_names(studyId):
    """

    :param study:
    """
    sample = Samples()
    result = sample.search(studyId=studyId, include="projects.studies.samples.name")
    return [r["name"] for r in result]


def check_samples_acl(studyId, userId, group=None):
    """

    Method to check permisions for one user

    :param studyId:
    :param userId:
    """

    if group is not None:
        userId += "," + group
    sample = Samples()
    options = {"acl.userId": userId}
    result = sample.search(studyId=studyId, **options)
    if not result:
        return 'Your user don\'t have permissions in any of the Sample currently available in this Study, please check it with the administrator'
    else:
        return [sample["name"] for sample in result if sample["acl"]]


def check_studies_acl(user_id):
    """

    Method to check permisions for one user

    :param user_id:
    """
    study = Studies()
    search = {"groups.userIds": user_id}
    result = study.search(**search)
    if not result:
        return 'Your user don\'t have permissions in any of the Studies currently available, please check it with the administrator'
    else:
        studies_groups = []
        for study in result:
            groups = []
            read_permission = False
            for group in study["groups"]:
                if user_id in group["userIds"]:
                    groups.append(group["id"])
                    if group["permissions"]["read"]:
                        read_permission = True

            studies_groups.append((study["id"], groups, read_permission))
        return studies_groups


def check_user_acls(user_id):
    studies = check_studies_acl(user_id)
    acls = {}
    for study_acl in studies:
        acls[study_acl[0]] = {}
        acls[study_acl[0]]["samples"] = []
        acls[study_acl[0]]["status"] = {}
        acls[study_acl[0]]["sex"] = {}
        acls[study_acl[0]]["disease"] = {}

        if study_acl[2]:
            samples = get_all_samples_names(str(study_acl[0]))
        else:
            groups = ["@" + group for group in study_acl[1]]
            samples = check_samples_acl(str(study_acl[0]), user_id, ",".join(groups))
        for sample_acl in samples:
            acls[study_acl[0]]["samples"].append(sample_acl)
    return acls


def get_annotation(study_id, individual_id, variable_set_name):
    """
    :param study_id:
    :param individual_id:
    :param variable_set_name:
    :return:

    """
    variable = Variables()
    individual = Individuals()

    individuals_info = individual.search(studyId=study_id, id=individual_id)

    individual_annotations = []
    variableSetId = variable.search(studyId=study_id, name=variable_set_name)[0]["id"]
    for individual_info in individuals_info:
        individual_annotations.append(extract_annotation(individual_info, variableSetId))
    return individual_annotations

def extract_annotation(individual_info, variableSetId):
    individual_annotations = []
    if "annotationSets" in individual_info:

        individual_annotations = [r["annotations"] for r in individual_info["annotationSets"] if
                                       str(r["variableSetId"]) == str(variableSetId)]
    return individual_annotations

def get_annotation_sample(study_id, sample_id, variable_set_name):
    """
    :param study_id:
    :param sample_id:
    :param variable_set_name:
    :return:

    """
    variable = Variables()
    sample = Samples()

    samples_info = sample.search(studyId=study_id, id=sample_id)

    sample_annotations = []
    variableSetId = variable.search(studyId=study_id, name=variable_set_name)[0]["id"]
    for sample_info in samples_info:
        sample_annotations.append(extract_annotation(sample_info, variableSetId))
    return sample_annotations

def extract_annotation(sample_info, variableSetId):
    sample_annotations = []
    if "annotationSets" in sample_info:

        sample_annotations = [r["annotations"] for r in sample_info["annotationSets"] if
                                  str(r["variableSetId"]) == str(variableSetId)]
    return sample_annotations

def link_file_and_update_sample(uri, path, study_id, *sample_ids):
    file = Files()
    file_id = file.link(study_id, uri, path)[0]["id"]
    print(file_id)

    if sample_ids is not None:
        sample_id = ",".join(sample_ids)
        file.update(fileId=str(file_id), sampleIds=str(sample_id))


class AnnotationSet:
    def __init__(self, *annotations):
        self.data = {}
        for annotation in annotations:
            if isinstance(annotation["value"], float):
                try:
                    self.data[annotation["id"]] = int(annotation["value"])
                except:
                    self.data[annotation["id"]] = float(annotation["value"])
            else:
                self.data[annotation["id"]] = annotation["value"]

    def get_json(self):
        return self.data