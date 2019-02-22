from time import sleep

from pyopencga.commons import execute, OpenCGAResponseList
from pyopencga.opencga_config import ConfigClient
from pyopencga.retry import retry
from pyopencga.rest_clients._parent_rest_clients import _ParentBasicCRUDClient 

## [DEBUG]
print("## loaded user_client.py")

class Users(_ParentBasicCRUDClient):
    """
    This class contains method for users ws (i.e, login, logout, create new user...)
    """

    def __init__(self, configuration, session_id=None, login_handler=None, *args, **kwargs):
        _category = "users"
        super(Users, self).__init__(configuration, _category, session_id, login_handler, *args, **kwargs)

    def login(self, user, pwd, **options):
        """
        This is the method for login
        URL: /{apiVersion}/users/{user}/login

        :rtype: list of dict
        :param user: user id
        :param pwd: password for the user

        LoginModel {
            password (string)
        }
        """
        data = dict(password=pwd)

        return self._post('login', data=data, query_id=user, **options)

    def refresh_token(self, user, **options):
        """
        This is a method to refresh token to maintain the conection with the server
        URL: /{apiVersion}/users/{user}/login
        
        :rtype: list or dict
        :param user: user id
        """
        return self._post('login', data={}, query_id=user, **options)

    def logout(self, **options):
        """
        This method logout the user by disabling the session id
        """
        self.session_id = None

    def get_projects(self, user, **options):
        """
        Method to retrieve the projects of the user
        URL: /{apiVersion}/users/{user}/projects

        :param user: user id
        """
        return self._get('projects', query_id=user, **options)

    def change_password(self, user, pwd, newpwd, **options):
        """
        Change the password of a user
        URL: /{apiVersion}/users/{user}/password

        :param user: user id
        :param data: dict
        
        ChangePasswordModel {
            password (string),
            npassword (string, optional), ## --> Deprecated (old)?
            newPassword (string)
        }
        """
        data = dict(password=pwd, newPassword=newpwd)
        
        return self._post('password', query_id=user, data=data, **options)

    def get_configs(self, user, **options): ## Method name may be changed
        """
        Fetch a user configuration
        URL: /{apiVersion}/users/{user}/configs

        :param user: user id
        """
        return self._get('configs', query_id=user, **options)

    def get_filters(self, user, **options): ## Method name may be changed
        """
        Fetch user filters
        URL: /{apiVersion}/users/{user}/configs/filters

        :param user: user id
        """
        return self._get('configs', query_id=user, subcategory='filters', **options)


