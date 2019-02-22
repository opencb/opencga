from time import sleep

from pyopencga.commons import execute, OpenCGAResponseList
from pyopencga.opencga_config import ConfigClient
from pyopencga.retry import retry
from pyopencga.rest_clients._parent_rest_clients import _ParentRestClient, _ParentBasicCRUDClient, _ParentAclRestClient,  _ParentAnnotationSetRestClient  

class Studies(_ParentBasicCRUDClient, _ParentAclRestClient):
    """
    This class contains method for studies ws (i.e, state, files, info)
    """

    def __init__(self, configuration, session_id=None, login_handler=None, *args, **kwargs):
        _category = 'studies'
        super(Studies, self).__init__(configuration, _category, session_id, login_handler, *args, **kwargs)

    def get_groups(self, study, **options):
        """
        Return the groups present in the studies
        URL: /{apiVersion}/studies/{studies}/groups

        :param study: study id
        """
        return self._get('groups', query_id=study, **options)

    def search(self, **options):
        """
        Search studies
        URL: /{apiVersion}/studies/search
        """
        return self._get('search', **options)

    def get_files(self, study, **options):
        """
        Scan the study folder to find untracked or missing files
        URL: /{apiVersion}/studies/{study}/scanFiles

        :param study: study id
        """
        return self._get('scanFiles', query_id=study, **options)

    def check_files(self, study, **options):
        """
        Intended to keep the consistency between the database and the file system.
        Tracks new and/or removed files from a study. Files not available in the
        file system are tagged in their status as 'MISSING'
        URL: /{apiVersion}/studies/{study}/resyncFiles

        :param study: study id
        """
        return self._get('resyncFiles', query_id=study, **options)

    def get_stats(self, study, **options):
        """
        Fetch catalog study stats
        URL: /{apiVersion}/studies/{studies}/stats

        :param study: study id
        """

        return self._get('stats', query_id=study, **options)

        ## Here ==========>>>

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

    def samples(self, studyId, **options):
        """

        method to get study samples

        :param studyId:
        """

        return self._get('samples', query_id=studyId, **options)


