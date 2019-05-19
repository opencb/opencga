from pyopencga.rest_clients._parent_rest_clients import _ParentBasicCRUDClient


class Projects(_ParentBasicCRUDClient):
    """
    This class contains method for Projects webservices
    """

    def __init__(self, configuration, session_id=None, login_handler=None, *args, **kwargs):
        _category = "projects"
        super(Projects, self).__init__(configuration, _category, session_id, login_handler, *args, **kwargs)


    def aggregation_stats(self, project, **options):
        """
        Fetch catalog project stats
        URL: /{apiVersion}/projects/{projects}/aggregationStats

        :param project: project id
        :param default: calculate default stats (bool)
        :param fileFields: list of file fields separated by semicolons,
            e.g.: studies;type. For nested fields use >>, e.g.: studies>>biotype;type
        :param individualFields: list of individual fields separated by semicolons,
            e.g.: studies;type. For nested fields use >>, e.g.: studies>>biotype;type
        :param familyFields: list of family fields separated by semicolons,
            e.g.: studies;type. For nested fields use >>, e.g.: studies>>biotype;type
        :param sampleFields: list of sample fields separated by semicolons,
            e.g.: studies;type. For nested fields use >>, e.g.: studies>>biotype;type
        :param cohortFields: list of cohort fields separated by semicolons,
            e.g.: studies;type. For nested fields use >>, e.g.: studies>>biotype;type
        """

        return self._get('aggregationStats', query_id=project, **options)

    def search(self, **options):
        """
        Method to search projects
        URL: /{apiVersion}/projects/search

        :param owner: owner of the project
        :param id: project id
        :param name: project name
        :param fqn: project fqn
        :param organization: project organization
        :param description: project description
        :param study: study id or alias
        :param creationDate: creation date (Format: yyyyMMddHHmmss. Examples: >2018, 2017-2018, <201805...)
        :param modificationDate: modification date (Format: yyyyMMddHHmmss. Examples: >2018, 2017-2018, <201805...)
        :param status: status
        :param attributes: attributes
        :param include: set which fields are included in the response, e.g.: name,alias...
        :param exclude: set which fields are excluded in the response, e.g.: name,alias...
        :param limit: max number of results to be returned.
        :param skip: number of results to be skipped.
        """

        return self._get('search', **options)

    def studies(self, project, **options):
        """
        Fetch all the studies from the given project ID
        URL: /{apiVersion}/projects/{projects}/studies

        :param project: project id
        :param silent: boolean to retrieve all possible entries that are queried for, false
            to raise an exception whenever one of the entries looked for cannot be shown for whichever reason
        :param include: set which fields are included in the response, e.g.: name,alias...
        :param exclude: set which fields are excluded in the response, e.g.: name,alias...
        :param limit: max number of results to be returned.
        :param skip: number of results to be skipped.
        """
        return self._get("studies", query_id=project, **options)

    def increment_release(self, project, **options):
        """
        Increment current release number in the project
        URL: /{apiVersion}/projects/{project}/incRelease

        :param project: project id
        """
        return self._post('incRelease', query_id=project, **options)
