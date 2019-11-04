from pyopencga.rest_clients._parent_rest_clients import _ParentRestClient

class Admin(_ParentRestClient):
    """
    This class contains methods for the Admin webservices
    """

    def __init__(self, configuration, session_id=None, login_handler=None, *args, **kwargs):
        _category = 'admin'
        super(Admin, self).__init__(configuration, _category, session_id, login_handler, *args, **kwargs)


    def sync_users(self, data, **options):
        """
        Synchronise groups of users with LDAP groups
        URL: /{apiVersion}/admin/users/sync
        # Mandatory fields:

        - authOriginId: Authentication origin id defined in the main Catalog configuration.
        - study: Study [[user@]project:]study where the group of users will be synced with the LDAP group.
        - from: LDAP group to be synced with a catalog group.
        - to: Catalog group that will be synced with the LDAP group.
        - force: Boolean to force the synchronisation with already existing Catalog 
                 groups that are not yet synchronised with any other group.

        data = {
            "authenticationOriginId": "string",
            "from": "string",
            "to": "string",
            "study": "string",
            "force": true
        }
        """

        return self._post('users', subcategory='sync', data=data, **options)

    def import_users(self, data, **options):
        """
        Import users or a group of users from LDAP
        URL: /{apiVersion}/admin/users/import
        
        data = {
            "authenticationOriginId": "string",
            "users": [
                "string"
                ],
            "group": "string",
            "study": "string",
            "studyGroup": "string",
            "account": "string"
        }
        """

        return self._post('users', subcategory='import', data=data, **options)

    def create_user(self, data, **options):
        """
        Create a new user
        URL: /{apiVersion}/admin/users/create

        data = {
        "id": "string",
        "name": "string",
        "email": "string",
        "password": "string",
        "organization": "string",
        "account": "string"
        }
        """
        
        return self._post('users', subcategory='create', data=data, **options)

    def handle_global_panels(self, data, **options):
        """
        Handle global panels
        URL: /{apiVersion}/admin/catalog/panel
        """

        return self._post('catalog', sucategory='panel', data=data, **options)

    def update_jwt(self, data, **options):
        """
        Change JSON Web Token (JWT) secret key
        URL: /{apiVersion}/admin/catalog/jwt

        data = {
            "secretKey": "string"
            }
        """

        return self._post('catalog', subcategory='jwt', data=data, **options)

    def install_opencga_database(self, data, **options):
        """
        Install OpenCGA database. Creates and initialises the OpenCGA database.
        URL: /{apiVersion}/admin/catalog/install

        # Mandatory fields:
        - secretKey: Secret key needed to authenticate through OpenCGA (JWT)
        - password: Password that will be set to perform future administrative operations over OpenCGA
        
        data = {
            "password": "string",
            "secretKey": "string"
        }
        """
        
        return self-_post('catalog', subcategory='install', data=data, **options)

    def sync_catalog(self, data, **options):
        """
        Sync Catalog into the Solr
        URL: /{apiVersion}/admin/catalog/indexStats
        """

        return self._post('catalog', subcategory='indexStats', data=data, **options)

    def group_by(self, **options): ## Improve method later
        """
        Group by operation
        URL: /{apiVersion}/admin/audit/groupBy
        """

        return self._get('audit', subcategory='groupBy', **options)
