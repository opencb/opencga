from pyopencga.rest_clients._parent_rest_clients import _ParentRestClient

class Tool(_ParentRestClient):
    """
    This class contains methods for the Analysis - Tool webservices
    """

    def __init__(self, configuration, session_id=None, login_handler=None, *args, **kwargs):
        _category = 'analysis/tool'
        super(Tool, self).__init__(configuration, _category, session_id, login_handler, *args, **kwargs)


    def execute(self, data, **options):
        """
        Execute an analysis using an internal or external tool
        URL: /{apiVersion}/analysis/tool/execute
        """

        return self._post('execute', data=data, **options)

