from pyopencga.rest_clients._parent_rest_clients import _ParentRestClient


class Jobs(_ParentRestClient):
    """
    This class contains methods for the 'Jobs' webservices
    Client version: 2.0.0
    PATH: /{apiVersion}/jobs
    """

    def __init__(self, configuration, token=None, login_handler=None, *args, **kwargs):
        _category = 'jobs'
        super(Jobs, self).__init__(configuration, _category, token, login_handler, *args, **kwargs)

    def update(self, jobs, data=None, **options):
        """
        Update some job attributes
        PATH: /{apiVersion}/jobs/{jobs}/update

        :param str jobs: Comma separated list of job IDs or UUIDs up to a maximum of 100
        :param str study: Study [[user@]project:]study where study and project can be either the ID or UUID
        :param dict data: params
        """

        return self._post('update', query_id=jobs, data=data, **options)

    def create(self, data, **options):
        """
        Register an executed job with POST method
        PATH: /{apiVersion}/jobs/create

        :param str study: Study [[user@]project:]study where study and project can be either the ID or UUID
        :param dict data: job
        """

        return self._post('create', data=data, **options)

    def acl(self, jobs, **options):
        """
        Return the acl of the job. If member is provided, it will only return the acl for the member.
        PATH: /{apiVersion}/jobs/{jobs}/acl

        :param str jobs: Comma separated list of job IDs or UUIDs up to a maximum of 100
        :param str member: User or group id
        :param bool silent: Boolean to retrieve all possible entries that are queried for, false to raise an exception whenever one of the entries looked for cannot be shown for whichever reason
        """

        return self._get('acl', query_id=jobs, **options)

    def update_acl(self, members, data, **options):
        """
        Update the set of permissions granted for the member
        PATH: /{apiVersion}/jobs/acl/{members}/update

        :param str members: Comma separated list of user or group ids
        :param dict data: JSON containing the parameters to add ACLs
        """

        return self._post('update', query_id=members, data=data, **options)

    def info(self, jobs, **options):
        """
        Get job information
        PATH: /{apiVersion}/jobs/{jobs}/info

        :param str include: Fields included in the response, whole JSON path must be provided
        :param str exclude: Fields excluded in the response, whole JSON path must be provided
        :param str jobs: Comma separated list of job IDs or UUIDs up to a maximum of 100
        :param str study: Study [[user@]project:]study where study and project can be either the ID or UUID
        :param bool deleted: Boolean to retrieve deleted jobs
        """

        return self._get('info', query_id=jobs, **options)

    def delete(self, jobs, **options):
        """
        Delete existing jobs
        PATH: /{apiVersion}/jobs/{jobs}/delete

        :param str study: Study [[user@]project:]study where study and project can be either the ID or UUID
        :param str jobs: Comma separated list of job ids
        """

        return self._delete('delete', query_id=jobs, **options)

    def search(self, **options):
        """
        Job search method
        PATH: /{apiVersion}/jobs/search

        :param str include: Fields included in the response, whole JSON path must be provided
        :param str exclude: Fields excluded in the response, whole JSON path must be provided
        :param int limit: Number of results to be returned
        :param int skip: Number of results to skip
        :param bool count: Get the total number of results matching the query. Deactivated by default.
        :param str study: Study [[user@]project:]study where study and project can be either the ID or UUID
        :param str name: Job name
        :param str tool: Tool executed by the job
        :param str user: User that created the job
        :param str priority: Priority of the job
        :param str status: Job status
        :param str creation_date: Creation date. Format: yyyyMMddHHmmss. Examples: >2018, 2017-2018, <201805
        :param str modification_date: Modification date. Format: yyyyMMddHHmmss. Examples: >2018, 2017-2018, <201805
        :param bool visited: Visited status of job
        :param str tags: Job tags
        :param str input: Comma separated list of file ids used as input.
        :param str output: Comma separated list of file ids used as output.
        :param str release: Release when it was created
        :param bool deleted: Boolean to retrieve deleted jobs
        """

        return self._get('search', **options)
