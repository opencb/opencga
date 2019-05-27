from pyopencga.rest_clients._parent_rest_clients import _ParentBasicCRUDClient 

class Users(_ParentBasicCRUDClient):
    """
    This class contains method for the Users webservices
    """

    def __init__(self, configuration, session_id=None, login_handler=None, *args, **kwargs):
        _category = 'users'
        super(Users, self).__init__(configuration, _category, session_id, login_handler, *args, **kwargs)


    def login(self, user, pwd, **options):
        """
        This is the method for login
        URL: /{apiVersion}/users/{user}/login

        :param user: user id
        :param pwd: user password 
        """
        
        data = dict(password=pwd)

        return self._post('login', data=data, query_id=user, **options)

    def refresh_token(self, user, **options):
        """
        This is a method to refresh token to maintain the connection with the server
        URL: /{apiVersion}/users/{user}/login
        
        :param user: user id
        """
        
        return self._post('login', data={}, query_id=user, **options)

    def logout(self):
        """
        This method logout the user by deleting the session id, no server call is needed
        """
        
        self.session_id = None

    def projects(self, user, **options):
        """
        Method to retrieve the projects of the user
        URL: /{apiVersion}/users/{user}/projects

        :param user: user id
        :param include: set which fields are included in the response, e.g.: name,alias...
        :param exclude: set which fields are excluded in the response, e.g.: name,alias...
        :param limit: max number of results to be returned.
        :param skip: number of results to be skipped.
        """
        
        return self._get('projects', query_id=user, **options)

    def update_password(self, user, pwd, newpwd, **options):
        """
        Change the password of a user
        URL: /{apiVersion}/users/{user}/password

        :param user: user id
        :param pwd: current password
        :param newpwd: new password
        """
        
        data = dict(password=pwd, newPassword=newpwd)
        
        return self._post('password', query_id=user, data=data, **options)

    def configs(self, user, **options):
        """
        Fetch a user configuration
        URL: /{apiVersion}/users/{user}/configs

        :param user: user id
        :param name: unique name (typically the name of the application)
        """
        
        return self._get('configs', query_id=user, **options)

    def update_configs(self, user, data, action, **options):
        """
        Add or remove a custom user configuration. Some applications might want to store
        some configuration parameters containing the preferences of the user. The aim of 
        this is to provide a place to store this things for every user.
        URL:/{apiVersion}/users/{user}/configs/update

        :param user: user id
        :param action: Action to be performed: ADD or REMOVE a group
        :param data: dict with the following Model:

	{
	    "id": "string",
	    "configuration": {}
	}
        """
        
        return self._post('configs', query_id=user, subcategory='update', data=data, action=action, **options)

    def filters(self, user, **options): ## Method name may be changed
        """
        Fetch user filters
        URL: /{apiVersion}/users/{user}/configs/filters

        :param user: user id
        :param name: filter name. If provided, it will only fetch the specified filter
        """
        
        return self._get('configs', query_id=user, subcategory='filters', **options)

    def update_filters(self, user, data, action, **options):
        """
        Add or remove a custom filter
        URL: /{apiVersion}/users/{user}/configs/filters/update

        :param user: user id
        :param acton: action to be performed: ADD or REMOVE
        :param data: dict with the following Model:
        
	{
            "name": "string",
            "description": "string",
            "bioformat": "string",
            "query": {},
            "options": {}
        }

        ## Available bioformat values:

        'MICROARRAY_EXPRESSION_ONECHANNEL_AGILENT',
        'MICROARRAY_EXPRESSION_ONECHANNEL_AFFYMETRIX',
        'MICROARRAY_EXPRESSION_ONECHANNEL_GENEPIX',
        'MICROARRAY_EXPRESSION_TWOCHANNELS_AGILENT',
        'MICROARRAY_EXPRESSION_TWOCHANNELS_GENEPIX',
        'DATAMATRIX_EXPRESSION',
        'IDLIST',
        'IDLIST_RANKED',
        'ANNOTATION_GENEVSANNOTATION',
        'OTHER_NEWICK',
        'OTHER_BLAST',
        'OTHER_INTERACTION',
        'OTHER_GENOTYPE',
        'OTHER_PLINK',
        'OTHER_VCF',
        'OTHER_PED',
        'VCF4',
        'VARIANT',
        'ALIGNMENT',
        'COVERAGE',
        'SEQUENCE',
        'PEDIGREE',
        'NONE',
        'UNKNOWN'
       """

        return self._post('configs', query_id=user, subcategory='filters/update', data=data, action=action, **options)

    ## [INFO] The implementation of update_filter needs a reimplementation of URL building methods
    def update_filter(self, user, filter_name, data, **options):
        """
        Updates a custom filter
        URL: /{apiVersion}/users/{user}/configs/filters/{name}/update

        :para user: user id
        :param filter: filter id or name
        :param data: dict with the following Model:
        
	{
            "bioformat": "string",
            "description": "string",
            "query": {},
            "options": {}
        }

        ## Available bioformat values:

        'MICROARRAY_EXPRESSION_ONECHANNEL_AGILENT',
        'MICROARRAY_EXPRESSION_ONECHANNEL_AFFYMETRIX',
        'MICROARRAY_EXPRESSION_ONECHANNEL_GENEPIX',
        'MICROARRAY_EXPRESSION_TWOCHANNELS_AGILENT',
        'MICROARRAY_EXPRESSION_TWOCHANNELS_GENEPIX',
        'DATAMATRIX_EXPRESSION',
        'IDLIST',
        'IDLIST_RANKED',
        'ANNOTATION_GENEVSANNOTATION',
        'OTHER_NEWICK',
        'OTHER_BLAST',
        'OTHER_INTERACTION',
        'OTHER_GENOTYPE',
        'OTHER_PLINK',
        'OTHER_VCF',
        'OTHER_PED',
        'VCF4',
        'VARIANT',
        'ALIGNMENT',
        'COVERAGE',
        'SEQUENCE',
        'PEDIGREE',
        'NONE',
        'UNKNOWN'
        """
        
        ## [INFO] Temporal solution to action with embedded secondary_query_id
        subcategory = 'filters/{name}/update'.format(name=filter_name)

        return self._post('configs', query_id=user, subcategory=subcategory, data=data, **options)
