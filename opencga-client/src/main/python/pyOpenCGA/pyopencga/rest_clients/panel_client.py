from pyopencga.rest_clients._parent_rest_clients import _ParentBasicCRUDClient, _ParentAclRestClient

class Panels(_ParentBasicCRUDClient, _ParentAclRestClient):
    """
    This class contains the methods for Panels webservices
    """

    def __init__(self, configuration, session_id=None, login_handler=None, *args, **kwargs):
        _category = 'panels'
        super(Panels, self).__init__(configuration, _category, session_id, login_handler, *args, **kwargs)


    def search(self, **options):
        """
        Panel search
        URL: /{apiVersion}/panels/search

        :param study: Study [[user@]project:]study
        :param name: Panel name
        :param phenotypes: Panel phenotypes
        :param variants: Panel variants
        :param genes: Panel genes
        :param regions: Panel regions
        :param categories: Panel categories
        :param tags: Panel tags
        :param description: Panel description
        :param author: Panel author
        :param creationDate: Creation date (Format: yyyyMMddHHmmss. Examples: >2018, 2017-2018, <201805...)
        :param modificationDate: Modification date (Format: yyyyMMddHHmmss. Examples: >2018, 2017-2018, <201805...)
        :param global: Boolean (deafult False) indicating which panels are queried (installation or study specific panels)
        :param skipCount: Skip count (Boolean, default False)
        :param release: Release value (Current release from the moment the samples were first created)
        :param snapshot: Snapshot value (Latest version of samples in the specified release)
        :param include: Fields included in the response, whole JSON path must be provided
        :param exclude: Fields excluded in the response, whole JSON path must be provided
        :param limit: Number of results to be returned in the queries
        :param skip: Number of results to skip in the queries
        :param count: Total number of results (Boolean, default False)
        """

        return self._get('search', **options)
