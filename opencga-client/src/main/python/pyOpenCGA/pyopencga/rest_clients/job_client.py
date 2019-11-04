from pyopencga.rest_clients._parent_rest_clients import _ParentBasicCRUDClient, _ParentAclRestClient

class Jobs(_ParentBasicCRUDClient, _ParentAclRestClient):
    """
    This class cotains methods for the Jobs webservice
    """

    def __init__(self, configuration, session_id=None, login_handler=None, *args, **kwargs):
        _category = 'jobs'
        super(Jobs, self).__init__(configuration, _category, session_id, login_handler, *args, **kwargs)


    def search(self, **options):
        """
        Job search method
        URL: /{apiVersion}/jobs/search
        
        :param study: study [[user@]project:]study where study and project can be either the id or alias
        :param name: name
        :param toolName: tool name    
        :param status: status
        :param creationDate: creation date (Format: yyyyMMddHHmmss. Examples: >2018, 2017-2018, <201805...)
        :param modificationDate: modification date (Format: yyyyMMddHHmmss. Examples: >2018, 2017-2018, <201805...)
        :param ownerId: owner id
        :param date: date
        :param inputFiles: comma separated list of input file ids
        :param outputFiles: comma separated list of output file ids
        :param release: release value
        :param skipCount: skip count (bool -> default=false)       
        :param include: fields included in the response, whole JSON path must be provided
        :param exclude: fields excluded in the response, whole JSON path must be provided
        :param limit: number of results to be returned in the queries
        :param skip: number of results to skip in the queries
        :param count: total number of results
        """

        return self._get('search', **options)

    def visit(self, job, **options):
        """
        Increment job visits
        URL: /{apiVersion}/jobs/{jobId}/visit

        :param job: jobId
        :param study: Study [[user@]project:]study where study and project can be either the id or alias
        """

        return self._get('visit', query_id=job, **options)
