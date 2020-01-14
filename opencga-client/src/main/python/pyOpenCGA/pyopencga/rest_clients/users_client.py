from pyopencga.rest_clients._parent_rest_clients import _ParentRestClient


class Users(_ParentRestClient):
    """
    This class contains methods for the 'Users' webservices
    Client version: 2.0.0
    PATH: /{apiVersion}/users
    """

    def __init__(self, configuration, token=None, login_handler=None, *args, **kwargs):
        _category = 'users'
        super(Users, self).__init__(configuration, _category, token, login_handler, *args, **kwargs)

    def create(self, data, **options):
        """
        Create a new user
        PATH: /{apiVersion}/users/create

        :param dict data: JSON containing the parameters
        """

        return self._post('create', data=data, **options)

    def info(self, user, **options):
        """
        Return the user information including its projects and studies
        PATH: /{apiVersion}/users/{user}/info

        :param str include: Fields included in the response, whole JSON path must be provided
        :param str exclude: Fields excluded in the response, whole JSON path must be provided
        :param str user: User id
        """

        return self._get('info', query_id=user, **options)

    def configs(self, user, **options):
        """
        Fetch a user configuration
        PATH: /{apiVersion}/users/{user}/configs

        :param str user: User id
        :param str name: Unique name (typically the name of the application).
        """

        return self._get('configs', query_id=user, **options)

    def password(self, user, data, **options):
        """
        Change the password of a user
        PATH: /{apiVersion}/users/{user}/password

        :param str user: User id
        :param dict data: JSON containing the params 'password' (old password) and 'newPassword' (new password)
        """

        return self._post('password', query_id=user, data=data, **options)

    def projects(self, user, **options):
        """
        Retrieve the projects of the user
        PATH: /{apiVersion}/users/{user}/projects

        :param str include: Fields included in the response, whole JSON path must be provided
        :param str exclude: Fields excluded in the response, whole JSON path must be provided
        :param int limit: Number of results to be returned
        :param int skip: Number of results to skip
        :param str user: User id
        """

        return self._get('projects', query_id=user, **options)

    def update(self, user, data, **options):
        """
        Update some user attributes
        PATH: /{apiVersion}/users/{user}/update

        :param str user: User id
        :param dict data: JSON containing the params to be updated.
        """

        return self._post('update', query_id=user, data=data, **options)

    def update_configs(self, user, data, **options):
        """
        Add or remove a custom user configuration
        PATH: /{apiVersion}/users/{user}/configs/update

        :param str user: User id
        :param str action: Action to be performed: ADD or REMOVE a group
        :param dict data: JSON containing anything useful for the application such as user or default preferences. When removing, only the id will be necessary.
        """

        return self._post('configs/update', query_id=user, data=data, **options)

    def update_filters(self, user, data, **options):
        """
        Add or remove a custom user filter
        PATH: /{apiVersion}/users/{user}/configs/filters/update

        :param str user: User id
        :param str action: Action to be performed: ADD or REMOVE a group
        :param dict data: Filter parameters. When removing, only the 'name' of the filter will be necessary
        """

        return self._post('configs/filters/update', query_id=user, data=data, **options)

    def update_filter(self, user, name, data, **options):
        """
        Update a custom filter
        PATH: /{apiVersion}/users/{user}/configs/filters/{name}/update

        :param str user: User id
        :param str name: Filter name
        :param dict data: Filter parameters
        """

        return self._post('configs/filters', query_id=user, subcategory='update', second_query_id=name, data=data, **options)

    def filters_configs(self, user, **options):
        """
        Fetch user filters
        PATH: /{apiVersion}/users/{user}/configs/filters

        :param str user: User id
        :param str name: Filter name. If provided, it will only fetch the specified filter
        """

        return self._get('configs/filters', query_id=user, **options)

    def login(self, user, data=None, **options):
        """
        Get identified and gain access to the system
        PATH: /{apiVersion}/users/{user}/login

        :param str user: User id
        :param dict data: JSON containing the parameter 'password'
        """

        return self._post('login', query_id=user, data=data, **options)
