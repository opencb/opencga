from time import sleep

from pyCGA.commons import execute, OpenCGAResponseList
from pyCGA.opencgaconfig import ConfigClient
from pyCGA.retry import retry


class _ParentRestClient(object):
    """Queries the REST service given the different query params"""

    def __init__(self, configuration, category, session_id=None, login_handler=None, auto_refresh=True):
        """
        :param login_handler: a parameterless method that can log in this connector
        and return a session id
        """
        self.auto_refresh = auto_refresh
        self._cfg = configuration
        self._category = category
        self.session_id = session_id
        self.login_handler = login_handler
        self.on_retry = None

    def _client_login_handler(self):
        if self.login_handler:
            self.session_id = self.login_handler()

    def _refresh_token_client(self):
        if self.login_handler:
            self.session_id = self.login_handler(refresh=True)

    @staticmethod
    def _get_query_id_str(query_ids):
        if query_ids is None:
            return None
        elif isinstance(query_ids, list):
            return ','.join(map(str, query_ids))
        else:
            return str(query_ids)

    def _rest_retry(self, method, resource, query_id=None, subcategory=None,
                    second_query_id=None, data=None, dont_retry=None, **options):
        """Invokes the specified HTTP method, with retries if they are specified in the configuration
        :return: an instance of OpenCGAResponseList"""

        query_ids_str = self._get_query_id_str(query_id)

        def exec_retry():
            return execute(host=self._cfg.host,
                           version=self._cfg.version,
                           sid=self.session_id,
                           category=self._category,
                           subcategory=subcategory,
                           method=method,
                           query_id=query_ids_str,
                           second_query_id=second_query_id,
                           resource=resource,
                           data=data,
                           options=options)

        def notify_retry(exc_type, exc_val, exc_tb):
            if self.on_retry:
                self.on_retry(self, exc_type, exc_val, exc_tb, dict(
                    method=method, resource=resource, query_id=query_id,
                    category=self._category, subcategory=subcategory,
                    second_query_id=second_query_id, data=data,
                    options=options
                ))

        response = retry(
            exec_retry, self._cfg.max_attempts, self._cfg.min_retry_secs, self._cfg.max_retry_secs,
            login_handler=self._client_login_handler if self.login_handler else None,
            on_retry=notify_retry, dont_retry=dont_retry)

        if self.auto_refresh:
            self._refresh_token_client()
        return OpenCGAResponseList(response, query_ids_str)

    def _get(self, resource, query_id=None, subcategory=None, second_query_id=None, **options):
        """Queries the REST service and returns the result"""
        return self._rest_retry('get', resource, query_id, subcategory, second_query_id, **options)

    def _post(self, resource, data, query_id=None, subcategory=None, second_query_id=None, **options):
        """Queries the REST service and returns the result"""
        return self._rest_retry('post', resource, query_id, subcategory, second_query_id, data=data, **options)


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

    def acl_update(self, memberId, data, **options):
        """
        update acl

        :param query_id:
        :param options:
        """

        return self._post('acl', subcategory='update', second_query_id=memberId, data=data, **options)


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

        return self._get('annotationsets', query_id=query_id, study=study, subcategory='delete',
                         second_query_id=annotationset_name, **options)

    def annotationsets_info(self, query_id, study, annotationset_name, **options):
        """
        info annotationsets

        :param query_id:
        :param options:
        """

        return self._get('annotationsets', query_id=query_id, study=study, subcategory='info',
                         second_query_id=annotationset_name, **options)

    def annotationsets_create(self, query_id, study, variable_set_id, data, **options):
        """
        create annotationsets

        :param query_id:
        :param options:
        """

        return self._post('annotationsets', study=study, query_id=query_id, subcategory='create',
                          variableSetId=variable_set_id, data=data, **options)

    def annotationsets_update(self, query_id, study, annotationset_name, data, **options):
        """
        update annotationsets

        :param query_id:
        :param options:
        """

        return self._post('annotationsets', study=study, query_id=query_id, subcategory='update',
                          second_query_id=annotationset_name, data=data, **options)


class Users(_ParentBasicCRUDClient):
    """
    This class contains method for users ws (i.e, login, logout, create new user...)
    """

    def __init__(self, configuration, session_id=None, login_handler=None, *args, **kwargs):
        _category = "users"
        super(Users, self).__init__(configuration, _category, session_id, login_handler, *args, **kwargs)

    def login(self, user, pwd, **options):
        """
        This is the method for login

        :rtype : list of dict
        :param user: user id
        :param pwd: password for the user
        """
        data = dict(password=pwd)

        return self._post('login', data=data, query_id=user, **options)

    def refresh_token(self, user, **options):
        return self._post('login', data={}, query_id=user, **options)

    def logout(self, **options):
        """
        This method logout the user
        """
        self.session_id = None


class Projects(_ParentBasicCRUDClient):
    """
    This class contains method for projects ws (i.e, create, files, info)
    """

    def __init__(self, configuration, session_id=None, login_handler=None, *args, **kwargs):
        _category = "projects"
        super(Projects, self).__init__(configuration, _category, session_id, login_handler, *args, **kwargs)

    def studies(self, projectId, **options):
        """
        Returns information on studies contained in the project

        :param projectId:
        """

        return self._get("studies", query_id=projectId, **options)

    def search(self, **options):
        """

        Method to search files based in a dictionary "options"

        :param study: study id
        :param options: Kargs where the keys are the name of the project properties used to search.
        """
        return self._get('search', **options)


class Studies(_ParentBasicCRUDClient, _ParentAclRestClient):
    """
    This class contains method for studies ws (i.e, state, files, info)
    """

    def __init__(self, configuration, session_id=None, login_handler=None, *args, **kwargs):
        _category = 'studies'
        super(Studies, self).__init__(configuration, _category, session_id, login_handler, *args, **kwargs)

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
            if data:
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

        return self._post('groups', query_id=studyId, subcategory='update', second_query_id=groupId, data=data,
                          **options)

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


class Files(_ParentBasicCRUDClient, _ParentAclRestClient):
    """
    This class contains method for files ws (i.e, link, create)
    """

    def __init__(self, configuration, session_id=None, login_handler=None, *args, **kwargs):
        _category = "files"
        super(Files, self).__init__(configuration, _category, session_id, login_handler, *args, **kwargs)

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
        options_with_file_id = dict(fileId=fileId)
        options_with_file_id.update(options)
        return self._get('unlink', **options_with_file_id)

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

    def __init__(self, configuration, session_id=None, login_handler=None, *args, **kwargs):
        _category = "jobs"
        super(Jobs, self).__init__(configuration, _category, session_id, login_handler, *args, **kwargs)

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

    def __init__(self, configuration, session_id=None, login_handler=None, *args, **kwargs):
        _category = "individuals"
        super(Individuals, self).__init__(configuration, _category, session_id, login_handler, *args, **kwargs)

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

    def __init__(self, configuration, session_id=None, login_handler=None, *args, **kwargs):
        _category = "samples"
        super(Samples, self).__init__(configuration, _category, session_id, login_handler, *args, **kwargs)

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
        # For compatibility: catalog 0.6 fails if "study" parameter supplied
        if study is not None:
            options['study'] = study

        return self._get('search', **options)


class Cohorts(_ParentBasicCRUDClient, _ParentAclRestClient, _ParentAnnotationSetRestClient):
    """
    This class contains method for Cohorts ws (i.e, update, create)
    """

    def __init__(self, configuration, session_id=None, login_handler=None, *args, **kwargs):
        _category = "cohorts"
        super(Cohorts, self).__init__(configuration, _category, session_id, login_handler, *args, **kwargs)

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


class Families(_ParentBasicCRUDClient, _ParentAclRestClient, _ParentAnnotationSetRestClient):
    """
    This class contains method for Families ws (i.e, update, create)
    """

    def __init__(self, configuration, session_id=None, login_handler=None, *args, **kwargs):
        _category = "families"
        super(Families, self).__init__(configuration, _category, session_id, login_handler, *args, **kwargs)

    def search(self, study, **options):
        """

        Method to search Families based in a dictionary "options"

        :param study: study id
        :param options: Kargs where the keys are the name of the Families properties used to search.
        """
        return self._get('search', study=study, **options)


class VariableSets(_ParentBasicCRUDClient, _ParentRestClient):
    """
    This class contains method for VariableSets ws (i.e, update, create)
    """

    def __init__(self, configuration, session_id=None, login_handler=None, *args, **kwargs):
        _category = "variableset"
        super(VariableSets, self).__init__(configuration, _category, session_id, login_handler, *args, **kwargs)

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

    def __init__(self, configuration, session_id=None, login_handler=None, *args, **kwargs):
        _category = "analysis"
        super(AnalysisAlignment, self).__init__(configuration, _category, session_id, login_handler, *args, **kwargs)

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

    def __init__(self, configuration, session_id=None, login_handler=None, *args, **kwargs):
        _category = "analysis"
        super(AnalysisVariant, self).__init__(configuration, _category, session_id, login_handler, *args, **kwargs)

    def index(self, file, study, **options):
        return self._get('variant', subcategory='index', file=file, study=study, **options)

    def query(self, pag_size, data, skip=0, **options):
        max = None
        if 'limit' in options:
            max = options['limit']
            del options['limit']

        skip = skip
        limit = pag_size
        next_page = True
        count = 0
        while next_page:
            if max is not None:
                if count + limit >= max:
                    limit -= (count + limit) - max
                    next_page = False
            page_result = self._post('variant', subcategory='query', data=data, skipCount=True, skip=skip,
                                     limit=limit, **options)
            skip += pag_size
            num_res = len(page_result.get())
            count += num_res
            if max is not None:
                if num_res == 0:
                    break
            else:
                if len(page_result.get()) < pag_size:
                    next_page = False
            if page_result:
                yield page_result


class GA4GH(_ParentRestClient):
    """
    This class contains method for GA4GH ws
    """

    def __init__(self, configuration, session_id=None, login_handler=None, *args, **kwargs):
        _category = "ga4gh"
        super(GA4GH, self).__init__(configuration, _category, session_id, login_handler, *args, **kwargs)

    def read_search(self, data, **options):
        return self._post('read', subcategory='search', data=data, **options)

    def variant_search(self, data, **options):
        return self._post('variant', subcategory='search', data=data, **options)


class Meta(_ParentRestClient):
    """
    This class contains method for Meta ws
    """

    def __init__(self, configuration, session_id=None, login_handler=None, *args, **kwargs):
        _category = "meta"
        super(Meta, self).__init__(configuration, _category, session_id, login_handler, *args, **kwargs)

    def about(self, **options):
        """
        Get OpenCGA instance info (e.g. version)
        """
        return self._get('about', **options)

    def ping(self, **options):
        """
        Ping OpenCGA instance
        """
        return self._get('ping', **options)

    def status(self, **options):
        """
        Get OpenCGA instance status
        """
        return self._get('status', **options)


class OpenCGAClient(object):
    def __init__(self, configuration, user=None, pwd=None, session_id=None,
                 anonymous=False, on_retry=None, auto_refresh=True):
        """
        :param on_retry: callback to be called with client retries an operation.
            It must accept parameters: client, exc_type, exc_val, exc_tb, call
        """
        self.auto_refresh = auto_refresh
        self.configuration = ConfigClient(configuration, on_retry)
        self.on_retry = on_retry
        self.clients = []
        self.user_id = user  # if user and session_id are supplied, we can log out
        if anonymous:
            self._login_handler = None
            self.session_id = None
        else:
            if user and pwd:
                self._login_handler = self._make_login_handler(user, pwd)
                self._login()
            else:
                if session_id is None:
                    msg = "User/password or session_id must be supplied"
                    raise Exception(msg)
                self._login_handler = None
                self.session_id = session_id
        self._create_clients()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.logout()

    def _create_clients(self):
        self.users = Users(self.configuration, self.session_id, self._login_handler, auto_refresh=self.auto_refresh)
        self.projects = Projects(self.configuration, self.session_id, self._login_handler, auto_refresh=self.auto_refresh)
        self.studies = Studies(self.configuration, self.session_id, self._login_handler, auto_refresh=self.auto_refresh)
        self.files = Files(self.configuration, self.session_id, self._login_handler, auto_refresh=self.auto_refresh)
        self.samples = Samples(self.configuration, self.session_id, self._login_handler, auto_refresh=self.auto_refresh)
        self.cohorts = Cohorts(self.configuration, self.session_id, self._login_handler, auto_refresh=self.auto_refresh)
        self.families = Families(self.configuration, self.session_id, self._login_handler, auto_refresh=self.auto_refresh)
        self.jobs = Jobs(self.configuration, self.session_id, self._login_handler, auto_refresh=self.auto_refresh)
        self.individuals = Individuals(self.configuration, self.session_id, self._login_handler, auto_refresh=self.auto_refresh)
        self.variable_sets = VariableSets(self.configuration, self.session_id, self._login_handler, auto_refresh=self.auto_refresh)
        self.analysis_alignment = AnalysisAlignment(self.configuration, self.session_id, self._login_handler, auto_refresh=self.auto_refresh)
        self.analysis_variant = AnalysisVariant(self.configuration, self.session_id, self._login_handler, auto_refresh=self.auto_refresh)
        self.ga4gh = GA4GH(self.configuration, self.session_id, self._login_handler, auto_refresh=self.auto_refresh)
        self.meta = Meta(self.configuration, self.session_id, self._login_handler, auto_refresh=self.auto_refresh)

        self.clients = [self.users, self.projects, self.studies, self.files,
                        self.samples, self.cohorts, self.families, self.jobs,
                        self.individuals, self.variable_sets,
                        self.analysis_alignment, self.analysis_variant,
                        self.ga4gh, self.meta]

        for client in self.clients:
            client.on_retry = self.on_retry

    def _make_login_handler(self, user, pwd):
        """
        Returns a closure that performs the log-in. This will be called on retries
        if the current session ever expires.
        The reason for using a closure and not a normal function is that a normal
        function would require storing the password in a field. It is more secure
        not to do so. This way, the password stored in the closure is inaccessible
        to other code
        """

        def login_handler(refresh=False):
            self.user_id = user
            if refresh:
                self.session_id = Users(self.configuration, session_id=self.session_id).refresh_token(user=user).get().sessionId
            else:
                self.session_id = Users(self.configuration).login(user=user, pwd=pwd).get().sessionId

            for client in self.clients:
                client.session_id = self.session_id  # renew the client's session id
            return self.session_id

        return login_handler

    def _login(self):
        assert self._login_handler, "Can't login without username and password provided"
        self._login_handler()

    def logout(self):
        self.session_id = None
