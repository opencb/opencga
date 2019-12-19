from pyopencga.rest_clients._parent_rest_clients import _ParentRestClient


class Operation(_ParentRestClient):
    """
    This class contains methods for the Operations webservice
    """

    def __init__(self, configuration, token=None, login_handler=None, *args, **kwargs):
        _category = "operation/variant"
        super(Operation, self).__init__(configuration, _category, token, login_handler, *args, **kwargs)

    def index_sample_genotype(self, data, **options):
        """
        Build and annotate the sample index.
        URL: /{apiVersion}/operation/variant/sample/genotype/index
        """

        return self._post('sample/genotype/index', data=data, **options)

    def index_family_genotype(self, data, **options):
        """
        Build the family index
        URL: /{apiVersion}/operation/variant/family/genotype/index
        """

        return self._post('family/genotype/index', data=data, **options)

    def aggregate_family(self, data, **options):
        """
        Find variants where not all the samples are present, and fill the empty values.
        URL: /{apiVersion}/operation/variant/family/aggregate
        """

        return self._post('family/aggregate', data=data, **options)

    def aggregate(self, data, **options):
        """
        Find variants where not all the samples are present, and fill the empty values, excluding HOM-REF (0/0) values.
        URL: /{apiVersion}/operation/variant/aggregate
        """

        return self._post('aggregate', data=data, **options)

    def create_secondary_index(self, data, **options):
        """
        Creates a secondary index using a search engine. If samples are provided, sample data will be added to the secondary index.
        URL: /{apiVersion}/operation/variant/secondaryIndex
        """

        return self._post('secondaryIndex', data=data, **options)

    def save_annotation(self, data, **options):
        """
        Save a copy of the current variant annotation at the database
        URL: /{apiVersion}/operation/variant/annotation/save
        """

        return self._post('annotation/save', data=data, **options)

    def index_score(self, data, **options):
        """
        Index a variant score in the database.
        URL: /{apiVersion}/operation/variant/score/index
        """

        return self._post('score/index', data=data, **options)

    def index_annotation(self, data, **options):
        """
        Create and load variant annotations into the database
        URL: /{apiVersion}/operation/variant/annotation/index
        """

        return self._post('annotation/index', data=data, **options)

    def delete_annotation(self, **options):
        """
        Deletes a saved copy of variant annotation
        URL: /{apiVersion}/operation/variant/annotation/delete
        """

        return self._delete('annotation/delete', **options)

    def delete_secondary_index(self, **options):
        """
        Remove a secondary index from the search engine for a specific set of samples.
        URL: /{apiVersion}/operation/variant/secondaryIndex/delete
        """

        return self._delete('secondaryIndex/delete', **options)

    def delete_score(self, **options):
        """
        Remove a variant score in the database
        URL: /{apiVersion}/operation/variant/score/delete
        """

        return self._delete('score/delete', **options)
