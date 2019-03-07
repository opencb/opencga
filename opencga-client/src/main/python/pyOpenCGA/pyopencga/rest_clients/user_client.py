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
        This is a method to refresh token to maintain the conection with the server
        URL: /{apiVersion}/users/{user}/login
        
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
        :param include: set which fields are included in the response, e.g.: name,alias...
        :param exclude: set which fields are excluded in the response, e.g.: name,alias...
        :param limit: max number of results to be returned.
        :param skip: number of results to be skipped.
        """
        
        return self._get('projects', query_id=user, **options)

    def change_password(self, user, pwd, newpwd, **options):
        """
        Change the password of a user
        URL: /{apiVersion}/users/{user}/password

        :param user: user id
        :param pwd: current password
        :param newpwd: new password
        """
        
        data = dict(password=pwd, newPassword=newpwd)
        
        return self._post('password', query_id=user, data=data, **options)

    def get_configs(self, user, **options):
        """
        Fetch a user configuration
        URL: /{apiVersion}/users/{user}/configs

        :param user: user id
        :param name: unique name (typically the name of the application)
        """
        
        return self._get('configs', query_id=user, **options)

    def add_config(self, user, data, **options):
        """
        Add a custom user configuration. Some applications might want to store
        some configuration parameters containing the preferences of the user. The aim of 
        this is to provide a place to store this things for every user.
        URL:/{apiVersion}/users/{user}/configs/update?action=ADD

        :param user: user id
        :param data: dict with the following Model:
	
	{
	    "id": "string",
	    "configuration": {}
	}
        """

        return self._post('configs', query_id=user, subcategory='update', data=data,
                          action='ADD', **options)

    def remove_config(self, user, data, **options):
        """
        Remove a custom user configuration. Some applications might want to store
        some configuration parameters containing the preferences of the user. The aim of 
        this is to provide a place to store this things for every user.
        URL:/{apiVersion}/users/{user}/configs/update?action=REMOVE

        :param user: user id
        :param data: dict with the following Model:
	
	{
	    "id": "string",
	    "configuration": {}
	}
        """
        
        return self._post('configs', query_id=user, subcategory='update', data=data,
                          action='REMOVE', **options)

    def get_filters(self, user, **options): ## Method name may be changed
        """
        Fetch user filters
        URL: /{apiVersion}/users/{user}/configs/filters

        :param user: user id
        :param name: filter name. If provided, it will only fetch the specified filter
        """
        
        return self._get('configs', query_id=user, subcategory='filters', **options)

    def add_filter(self, user, data, **options):
        """
        Add a custom user filter
        URL: /{apiVersion}/users/{user}/configs/filters/update?action=ADD

        :param user: user id
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
        
        return self._post('configs', query_id=user, subcategory='filters/update',
                          data=data, action='ADD', **options)

    def remove_filter(self, user, data, **options):
        """
        Remove a custom filter
        URL: /{apiVersion}/users/{user}/configs/filters/update?action=REMOVE

        :param user: user id
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

        return self._post('configs', query_id=user, subcategory='filters/update',
                          data=data, action='REMOVE', **options)

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


