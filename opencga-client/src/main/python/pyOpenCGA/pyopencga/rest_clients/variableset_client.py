from pyopencga.rest_clients._parent_rest_clients import _ParentBasicCRUDClient

## [DEPRECATED]

class VariableSets(_ParentBasicCRUDClient): 
    """
    This class contains methods for the VariableSets webservices
    """

    def __init__(self, configuration, session_id=None, login_handler=None, *args, **kwargs):
        _category = 'variableset'
        super(VariableSets, self).__init__(configuration, _category, session_id, login_handler, *args, **kwargs)

    def search(self, **options):
        """
        Get VariableSet info [DEPRECATED]
        URL: /{apiVersion}/variableset/search
        """

        return self._get('search', **options)

    def summary(self, variableset, **options):
        """
        Get VariableSet summary [DEPRECATED]
        URL: /{apiVersion}/variableset/{variableset}/summary
        """

        return self._get('summary', query_id=variableset, **options)

    def rename_field(self, variableset, old_name, new_name, **options):
        """
        Rename the field id of a field in a variable set [DEPRECATED]
        URL: /{apiVersion}/variableset/{variableset}/field/rename
        """

        options['oldName'] = old_name
        options['newName'] = new_name

        return self._get('field', subcategory='rename', query_id=variableset, **options)

    def delete_field(self, variableset, name, **options):
        """
        Delete one field from a variable set [DEPRECATED]
        URL: /{apiVersion}/variableset/{variableset}/field/delete
        """

        options['name'] = name

        return self._get('field', subcategory='delete', query_id=variableset, **options)

    def add_field(self, variableset, data, **options):
        """
        Add a new field in a variable set [DEPRECATED]
        URL: /{apiVersion}/variableset/{variableset}/field/add
        """

        return self._post('field', subcategory='add', query_id=variableset, data=data, **options)

