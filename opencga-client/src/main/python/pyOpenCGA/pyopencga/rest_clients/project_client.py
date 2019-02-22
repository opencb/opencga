from time import sleep

from pyopencga.commons import execute, OpenCGAResponseList
from pyopencga.opencga_config import ConfigClient
from pyopencga.retry import retry
from pyopencga.rest_clients._parent_rest_clients import _ParentBasicCRUDClient 


class Projects(_ParentBasicCRUDClient):
        """
         This class contains method for projects ws (i.e, create, files, info)
        """

        def __init__(self, configuration, session_id=None, login_handler=None, *args, **kwargs):
            _category = "projects"
            super(Projects, self).__init__(configuration, _category, session_id, login_handler, *args, **kwargs)
            
        def get_studies(self, project, **options):
            """
            Fetch all the studies contained in the projects
            URL: /{apiVersion}/projects/{projects}/studies
            
            :param project: project id
            """
            return self._get("studies", query_id=project, **options)

        def search(self, **options):
            """
            Method to search projects
            URL: /{apiVersion}/projects/search
            """
            return self._get('search', **options)

        def get_stats(self, project, **options):
            """
            Fetch catalog project stats
            URL: /{apiVersion}/projects/{projects}/stats

            :param project: project id
            """
            return self._get('stats', query_id=project, **options)

        def increment_release_number(self, project, **options):
            """
            Increment current release number in the project
            URL: /{apiVersion}/projects/{project}/incRelease

            :param project: project id
            """
            return self._post('incRelease', query_id=project, **options)

