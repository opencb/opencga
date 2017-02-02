import json

from pyCGA.Utils.AvroSchema import AvroSchemaFile

from pyCGA.commons import execute, OpenCGAResponse, OpenCGAResponseList
from pyCGA.opencgaconfig import ConfigClient


class _ParentRestClient(object):
    """Queries the REST service given the different query params"""

    def __init__(self, configuration, category, session_id=None):
        self._configuration = configuration
        self._category = category
        self.session_id = session_id

    def _get(self, resource, query_id=None, subcategory=None, second_query_id=None, **options):
        """Queries the REST service and returns the result"""

        if isinstance(query_id, list):
            query_id = ','.join(query_id)


        response = execute(host=self._configuration.host,
                           version=self._configuration.version,
                           sid=self.session_id,
                           category=self._category,
                           subcategory=subcategory,
                           method='get',
                           query_id=query_id,
                           second_query_id=second_query_id,
                           resource=resource,
                           options=options)

        return OpenCGAResponseList(response, query_id)

    def _post(self, resource, data, query_id=None, subcategory=None, second_query_id=None, **options):
        """Queries the REST service and returns the result"""

        if isinstance(query_id, list):
            query_id = ','.join(query_id)

        response = execute(host=self._configuration.host,
                           version=self._configuration.version,
                           sid=self.session_id,
                           category=self._category,
                           method='post',
                           data=data,
                           subcategory=subcategory,
                           query_id=query_id,
                           second_query_id=second_query_id,
                           resource=resource,
                           options=options)

        return OpenCGAResponseList(response, query_id)


class _ParentBasicCRUDClient(_ParentRestClient):

    def create(self, data, **options):

        return self._post('create', data=data, **options)

    def info(self, query_id, **options):

        return self._get('info', query_id=query_id, **options)

    def update(self, query_id, data, **options):

        return self._post('update', query_id=query_id, data=data, **options)

    def delete(self, query_id, **options):

        return self._get('delete', query_id=query_id, **options)


class _ParentAclRestClient(_ParentRestClient):

    def acl(self, query_id, **options):
        """
        acl info

        :param query_id:
        :param options:
        """

        return self._get('acl', query_id=query_id, **options)

    def acl_create(self, query_id, data, **options):
        """
        create acl

        :param query_id:
        :param options:
        """

        return self._post('acl', query_id=query_id, subcategory='create', data=data, **options)

    def acl_delete(self, query_id, memberId, **options):
        """
        delete acl

        :param query_id:
        :param options:
        """

        return self._get('acl', query_id=query_id, subcategory='delete', second_query_id=memberId, **options)

    def acl_info(self, query_id, memberId, **options):
        """
        info acl

        :param query_id:
        :param options:
        """

        return self._get('acl', query_id=query_id, subcategory='info', second_query_id=memberId, **options)

    def acl_update(self, query_id, memberId, data, **options):
        """
        info acl

        :param query_id:
        :param options:
        """

        return self._post('acl', query_id=query_id, subcategory='update', second_query_id=memberId, data=data, **options)


class _ParentAnnotationSetRestClient(_ParentRestClient):

    def annotationsets_search(self, query_id, study, **options):
        """
        annotationsets search

        :param query_id:
        :param options:
        """

        return self._get('annotationsets', study=study, query_id=query_id, subcategory='search', **options)

    def annotationsets(self, query_id, study, **options):
        """
        annotationsets

        :param query_id:
        :param options:
        """

        return self._get('annotationsets', query_id=query_id, study=study, subcategory='info', **options)

    def annotationsets_delete(self, query_id, study, annotationset_name, **options):
        """
        delete annotationsets

        :param query_id:
        :param options:
        """

        return self._get('annotationsets', query_id=query_id, study=study, subcategory='delete', second_query_id=annotationset_name, **options)

    def annotationsets_info(self, query_id, study, annotationset_name, **options):
        """
        info annotationsets

        :param query_id:
        :param options:
        """

        return self._get('annotationsets', query_id=query_id, study=study, subcategory='info', second_query_id=annotationset_name, **options)

    def annotationsets_create(self, query_id, study, variable_set_id, data, **options):
        """
        create annotationsets

        :param query_id:
        :param options:
        """

        return self._post('annotationsets', study=study, query_id=query_id, subcategory='create', variableSetId=variable_set_id, data=data, **options)

    def annotationsets_update(self, query_id, study, annotationset_name, data, **options):
        """
        update annotationsets

        :param query_id:
        :param options:
        """

        return self._post('annotationsets', study=study, query_id=query_id, subcategory='update', second_query_id=annotationset_name, data=data, **options)


class Users(_ParentBasicCRUDClient):
    """
    This class contains method for users ws (i.e, login, logout, create new user...)
    """

    def __init__(self, configuration, session_id=None):
        _category = "users"
        super(Users, self).__init__(configuration, _category, session_id)

    def login(self, user, pwd, **options):
        """
        This is the method for login

        :rtype : list of dict
        :param user: user id
        :param pwd: password for the user
        """
        data = dict(password=pwd)

        return self._post('login', data=data, query_id=user, **options)


    def logout(self, userId, **options):
        """
        This method logout the user

        :param userId: user id
        """

        return self._get('logout', query_id=userId, **options)


class Projects(_ParentBasicCRUDClient):
    """
    This class contains method for projects ws (i.e, create, files, info)
    """

    def __init__(self, configuration, session_id=None):
        _category = "projects"
        super(Projects, self).__init__(configuration, _category, session_id)

    def studies(self, projectId, **options):
        """
        Returns information on studies contained in the project

        :param projectId:
        """

        return self._get("studies", query_id=projectId, **options)


class Studies(_ParentBasicCRUDClient, _ParentAclRestClient):
    """
    This class contains method for studies ws (i.e, state, files, info)
    """

    def __init__(self, configuration, session_id=None):
        _category = 'studies'
        super(Studies, self).__init__(configuration, _category, session_id)

    def groups(self, studyId, **options):
        """

        Method to check groups in a studies

        :param options: Kargs where the keys are the name of the file properties used to search.
        """
        return self._get('groups', query_id=studyId, **options)

    def search(self, method='post', data=None, **options):
        """

        Method to search studies based in a dictionary "options"

        :param options: Kargs where the keys are the name of the file properties used to search.
        """

        if method == 'post' and data:
            return self._post('search', data=data, **options)
        else:
            options.update(data)
            return self._get('search', **options)

    def files(self, studyId, **options):
        """

        method to get study files

        :param studyId:
        """

        return self._get('files', query_id=studyId, **options)

    def summary(self, studyId, **options):
        """

        method to get study summary

        :param studyId:
        """

        return self._get('summary', query_id=studyId, **options)

    def groups_create(self, study, data, **options):
        """
        create group

        :param study:
        :param options:
        """

        return self._post('groups', query_id=study, subcategory='create', data=data, **options)

    def groups_delete(self, studyId, groupId, **options):
        """
        delete groups acl

        :param studyId:
        :param options:
        """

        return self._get('groups', query_id=studyId, subcategory='delete', second_query_id=groupId, **options)

    def groups_info(self, studyId, groupId, **options):
        """
        groups acl info

        :param studyId:
        :param options:
        """

        return self._get('groups', query_id=studyId, subcategory='info', second_query_id=groupId, **options)

    def groups_update(self, studyId, groupId, data, **options):
        """
        groups acl update

        :param studyId:
        :param options:
        """

        return self._post('groups', query_id=studyId, subcategory='update', second_query_id=groupId, data=data, **options)


    def jobs(self, studyId, **options):
        """

        method to get study jobs

        :param studyId:
        """

        return self._get('jobs', query_id=studyId, **options)

    def rsync_files(self, studyId, **options):
        """

        Scan the study folder to find untracked or missing files and update their state

        :param studyId:
        """

        return self._get('resyncFiles', query_id=studyId, **options)

    def samples(self, studyId, **options):
        """

        method to get study samples

        :param studyId:
        """

        return self._get('samples', query_id=studyId, **options)

    def scan_files(self, studyId, **options):
        """

        Scans the study folder to find untracked or missing files

        :param studyId:
        """

        return self._get('scanFiles', query_id=studyId, **options)

    def create(self, data, **options):

        return self._post('create', data=[data], **options)


class Files(_ParentBasicCRUDClient, _ParentAclRestClient):
    """
    This class contains method for files ws (i.e, link, create)
    """

    def __init__(self, configuration, session_id=None):
        _category = "files"
        super(Files, self).__init__(configuration, _category, session_id)

    def bioformats(self, **options):
        """

        List of accepted file bioformats

        """
        return self._get('bioformats', **options)

    def formats(self, **options):
        """

        List of accepted file formats

        """
        return self._get('formats', **options)

    def create_folder(self, studyId, folders, path, **options):
        """

        This is the method create a folder in the DB

        :param studyId: study to associate file to
        :param folder: "path in the DB"
        """
        return self._get('create-folder', studyId=studyId, folders=folders, path=path, **options)

    def group_by(self, fields, study, **options):
        """

        group by for files

        """
        return self._get('groupBy', fields=fields, study=study, **options)

    def search(self, study, **options):
        """

        Method to search files based in a dictionary "options"

        :param study: study id
        :param options: Kargs where the keys are the name of the file properties used to search.
        """
        return self._get('search', study=study, **options)

    def unlink(self, fileId, **options):
        """
        Method to unlink a particular file/foler

        :param fileId: file Id
        """
        return self._get('unlink', query_id=fileId, **options)

    def link(self, study, path, uri, **options):
        """
        Method to link a particular file/foler

        :param fileId: file Id
        """
        return self._get('link', study=study, path=path, uri=uri, **options)

    def scan_folder(self, folder, **options):
        """
        Scans a folder

        :param folder: folder
        """
        return self._get('scan', query_id=folder, **options)

    def list(self, folder, **options):
        """

        list files in folder

        :param folder: folder id, uri, name
        """

        return self._get('list', query_id=folder, **options)

    def content(self, file, **options):
        """

        content of a file

        :param file: file id, uri, name
        """

        return self._get('content', query_id=file, **options)


    def grep(self, file, **options):
        """

        grep the contents of a file

        :param file: File id
        """

        return self._get('grep', query_id=file, **options)

    def index(self, fileId, **options):
        """

        index a file

        :param fileId: file Id
        """

        return self._get('index', query_id=fileId, **options)

    def refresh(self, fileId, **options):
        """
        refresh metatadata from a file or folder - returns updated files

        :param fileId: File If
        """

        return self._get('refresh', query_id=fileId, **options)


    def tree(self, fileId, **options):
        """

        simulate tree command

        :param fileId: id of file
        """

        return self._get('tree', query_id=fileId, **options)


class Jobs(_ParentBasicCRUDClient, _ParentAclRestClient):
    """
    This class contains method for jobs ws
    """

    def __init__(self, configuration, session_id=None):
        _category = "jobs"
        super(Jobs, self).__init__(configuration, _category, session_id)

    def group_by(self, fields, study, **options):
        """

        group by for jobs

        """
        return self._get('groupBy', fields=fields, study=study, **options)

    def search(self, study, **options):
        """

        Method to search jobs based in a dictionary "options"

        :param study: study id
        """
        return self._get('search', study=study, **options)

    def visit(self, job_id, **options):
        """

        Increment job visits

        :param job_id: job_id
        """
        return self._get('visit', query_id=job_id, **options)

    def update(self, query_id, **options):
        raise NotImplemented('Update method is not implemented for jobs')


class Individuals(_ParentBasicCRUDClient, _ParentAclRestClient, _ParentAnnotationSetRestClient):
    """
    This class contains method for Individuals ws (i.e, update, create)
    """

    def __init__(self, configuration, session_id=None):
        _category = "individuals"
        super(Individuals, self).__init__(configuration, _category, session_id)


    def group_by(self, fields, study, **options):
        """

        group by for Individuals

        """
        return self._get('groupBy', fields=fields, study=study, **options)

    def search(self, study, **options):
        """

        Method to search Individuals based in a dictionary "options"

        :param study: study id
        :param options: Kargs where the keys are the name of the Individuals properties used to search.
        """
        return self._get('search', study=study, **options)


class Samples(_ParentBasicCRUDClient, _ParentAclRestClient, _ParentAnnotationSetRestClient):
    """
    This class contains method for Samples ws (i.e, update, create)
    """

    def __init__(self, configuration, session_id=None):
        _category = "samples"
        super(Samples, self).__init__(configuration, _category, session_id)

    def group_by(self, fields, study, **options):
        """

        group by for Samples

        """
        return self._get('groupBy', fields=fields, study=study, **options)

    def search(self, study, **options):
        """

        Method to search Samples based in a dictionary "options"

        :param study: study id
        :param options: Kargs where the keys are the name of the Samples properties used to search.
        """
        return self._get('search', study=study, **options)


class Cohorts(_ParentBasicCRUDClient, _ParentAclRestClient, _ParentAnnotationSetRestClient):
    """
    This class contains method for Cohorts ws (i.e, update, create)
    """

    def __init__(self, configuration, session_id=None):
        _category = "cohorts"
        super(Cohorts, self).__init__(configuration, _category, session_id)

    def group_by(self, fields, study, **options):
        """

        group by for Cohorts

        """
        return self._get('groupBy', fields=fields, study=study, **options)

    def search(self, study, **options):
        """

        Method to search Cohorts based in a dictionary "options"

        :param study: study id
        :param options: Kargs where the keys are the name of the Cohorts properties used to search.
        """
        return self._get('search', study=study, **options)

    def samples(self, cohorts, **options):
        """
        Method to get sample information of a particular Cohorts
        """
        return self._get('samples', query_id=cohorts, **options)


class VariableSets(_ParentBasicCRUDClient, _ParentRestClient):
    """
    This class contains method for VariableSets ws (i.e, update, create)
    """

    def __init__(self, configuration, session_id=None):
        _category = "variableset"
        super(VariableSets, self).__init__(configuration, _category, session_id)

    def search(self, study, **options):
        """

        Method to search VariableSets based in a dictionary "options"

        :param study: study id
        :param options: Kargs where the keys are the name of the VariableSets properties used to search.
        """
        return self._get('search', study=study, **options)

    def summary(self, variableset, **options):
        """
        Method to get summary of a particular VariableSets
        """
        return self._get('summary', query_id=variableset, **options)

    def field_rename(self, variableset, old_name, new_name, **options):
        """

        Method to rename a field in a VariableSet.
        properties.
        """

        return self._get('field', subcategory='rename', query_id=variableset, oldName=old_name,
                         newName=new_name, **options)

    def field_delete(self, variableset, name, **options):
        """

        Method to delete a field in a VariableSet.
        properties.
        """

        return self._get('field', subcategory='rename', query_id=variableset, name=name, **options)

    def field_add(self, variableset, data, **options):
        """

        Method to delete a field in a VariableSet.
        properties.
        """

        return self._post('field', subcategory='add', query_id=variableset, data=data, **options)


class AnalysisAlignment(_ParentRestClient):
    """
    This class contains method for AnalysisAlignment ws
    """

    def __init__(self, configuration, session_id=None):
        _category = "analysis"
        super(AnalysisAlignment, self).__init__(configuration, _category, session_id)

    def coverage(self, file, study, **options):
        return self._get('alignment', subcategory='coverage', file=file, study=study, **options)

    def index(self, file, study, **options):
        return self._get('alignment', subcategory='index', file=file, study=study, **options)

    def query(self, file, study, **options):
        return self._get('alignment', subcategory='query', file=file, study=study, **options)

    def stats(self, file, study, **options):
        return self._get('alignment', subcategory='stats', file=file, study=study, **options)


class AnalysisVariant(_ParentRestClient):
    """
    This class contains method for AnalysisVariant ws
    """

    def __init__(self, configuration, session_id=None):
        _category = "analysis"
        super(AnalysisVariant, self).__init__(configuration, _category, session_id)

    def index(self, file, study, **options):
        return self._get('variant', subcategory='index', file=file, study=study, **options)

    def query(self, pag_size, data, skip=0, **options):
        skip = skip
        limit = pag_size
        next_page = True
        while next_page:
            page_result = self._post('variant', subcategory='query', data=data, skipCount=True, skip=skip,
                                     limit=limit, **options)
            skip += pag_size
            if len(page_result.get()) < pag_size:
                next_page = False
            if page_result:
                yield page_result


class GA4GH(_ParentRestClient):
    """
    This class contains method for GA4GH ws
    """

    def __init__(self, configuration, session_id=None):
        _category = "ga4gh"
        super(GA4GH, self).__init__(configuration, _category, session_id)

    def read_search(self, data, **options):
        return self._post('read', subcategory='search', data=data, **options)

    def variant_search(self, data, **options):
        return self._post('variant', subcategory='search', data=data, **options)


class OpenCGAClient(object):
    
    def __init__(self, configuration, user=None, pwd=None, session_id=None):
        self.configuration = ConfigClient(configuration)
        if user and pwd:
            self.users = Users(self.configuration)
            self.user_id = user
            self.session_id = self._login(user, pwd)
        else:
            self.users = Users(self.configuration, session_id)
            self.user_id = self.users
            self.session_id = session_id
        self.projects = Projects(self.configuration, self.session_id)
        self.studies = Studies(self.configuration, self.session_id)
        self.files = Files(self.configuration, self.session_id)
        self.samples = Samples(self.configuration, self.session_id)
        self.cohorts = Cohorts(self.configuration, self.session_id)
        self.jobs = Jobs(self.configuration, self.session_id)
        self.individuals = Individuals(self.configuration, self.session_id)
        self.variable_sets = VariableSets(self.configuration, self.session_id)
        self.analysis_alignment = AnalysisAlignment(self.configuration, self.session_id)
        self.analysis_variant = AnalysisVariant(self.configuration, self.session_id)
        self.ga4gh = GA4GH(self.configuration, self.session_id)

    def _login(self, user, pwd):
        session_id = self.users.login(user=user, pwd=pwd).get().sessionId
        self.users = Users(self.configuration, session_id)
        return session_id

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.user_id:
            self.users.logout(userId=self.user_id)
