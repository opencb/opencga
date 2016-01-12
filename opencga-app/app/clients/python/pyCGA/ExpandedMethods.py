from pyCGA.CatalogWS import Studies, Samples, Individuals, Variables, Files

__author__ = 'antonior'


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
    variableSetId = variable.search(studyId=study_id, name=variable_set_name)[0]["id"]
    individuals_info = individual.search(studyId=study_id, id=individual_id)
    individual_annotations = []
    for individual_info in individuals_info:
        if "annotationSets" in individual_info:
            individual_annotations.append([r["annotations"] for r in individual_info["annotationSets"] if r["attributes"]["variableSetId"] == str(variableSetId)])
        else:
            individual_annotations.append([])
    return individual_annotations


def link_file_and_update_sample(uri, path, study_id, sample_id):
    file = Files()
    file_id = file.link(study_id, uri, path)[0]["id"]
    print(file_id)

    if sample_id is not None:
        file.update(fileId=str(file_id), sampleIds=str(sample_id))


