from pyopencga.rest_clients._parent_rest_clients import _ParentBasicCRUDClient, _ParentAclRestClient, _ParentAnnotationSetRestClient

class Files(_ParentBasicCRUDClient, _ParentAclRestClient, _ParentAnnotationSetRestClient):
    """
    This class contains methods for the Files webservices
    """

    def __init__(self, configuration, session_id=None, login_handler=None, *args, **kwargs):
        _category = 'files'
        super(Files, self).__init__(configuration, _category, session_id, login_handler, *args, **kwargs)


    def aggregation_stats(self, **options):
        """
        Fetch catalog file stats
        URL: /{apiVersion}/files/aggregationStats

        :param study: study [[user@]project:]study where study and project can be either the id or alias
        :param name: name
        :param type: type
        :param bioformat: comma separated Bioformat values. For existing Bioformats see files/bioformats
        :param format: comma separated Format values. For existing Formats see files/formats
        :param creationYear: creation year
        :param creationMonth: creation month (JANUARY, FEBRUARY...)
        :param creationDay: creation day
        :param creationDayOfWeek: creation day of week (MONDAY, TUESDAY...)
        :param status: status
        :param release: release
        :param external: external (boolean)
        :param size: size
        :param software: software
        :param experiment: experiment
        :param numSamples: number of samples
        :param numRelatedFiles: number of related files
        :param annotation: annotation, e.g: key1=value(;key2=value)
        :param default: calculate default stats (bool -> false (default))
        :param field: list of fields separated by semicolons, e.g.: studies;type.
            For nested fields use >>, e.g.: studies>>biotype;type;numSamples[0..10]:1
        """

        return self._get('aggregationStats', **options)

    def bioformats(self, **options):
        """
        List of accepted file bioformats
        URL: /{apiVersion}/files/bioformats
        """

        return self._get('bioformats', **options)

    def formats(self, **options):
        """
        List of accepted file formats
        URL: /{apiVersion}/files/formats
        """

        return self._get('formats', **options)

    def search(self, **options):
        """
        File search method.
        URL: /{apiVersion}/files/search

        :param study: study [[user@]project:]{study} where study and project can be either the id or alias.
        :param name: comma separated list of file names
        :param path: comma separated list of paths
        :param type: available types (FILE, DIRECTORY)
        :param bioformat: comma separated Bioformat values. For existing Bioformats see files/bioformats
        :param format: comma separated Format values. For existing Formats see files/formats
        :param status: status
        :param directory: directory under which we want to look for files or folders
        :param creationDate: creation date (Format: yyyyMMddHHmmss. Examples: >2018, 2017-2018, <201805...)
        :param modificationDate: modification date (Format: yyyyMMddHHmmss. Examples: >2018, 2017-2018, <201805...)
        :param description: description
        :param tags: tags
        :param size: size
        :param samples: comma separated list of sample ids
        :param job.id: job id that created the file(s) or folder(s)
        :param annotation: annotation, e.g: key1=value(;key2=value)
        :param attributes: text attributes (Format: sex=male,age>20 ...)
        :param nattributes: numerical attributes (Format: sex=male,age>20 ...)
        :param skipCount: skip count (default = false)
        :param release: release value
        :param include: fields included in the response, whole JSON path must be provided
        :param exclude: fields excluded in the response, whole JSON path must be provided
        :param limit: number of results to be returned in the queries
        :param skip: number of results to skip in the queries
        :param count: total number of results (default = false)
        :param lazy: false to return entire job and experiment object (default = true)
        :param flattenAnnotations: flatten the annotations? (default = false)
        """

        return self._get('search', **options)

    def scan_folder(self, folder, **options):
        """
        Scans a folder
        URL: /{apiVersion}/files/{folder}/scan

        :param folder: Folder id, name or path. Paths must be separated by : instead of /
        :param study: Study [[user@]project:]study where study and project can be either the id or alias
        :param calculateChecksum: calculateChecksum (bool -> false (default))
        """

        return self._get('scan', query_id=folder, **options)

    def list_folder(self, folder, **options):
        """
        List all the files inside the folder
        URL: /{apiVersion}/files/{folder}/list

        :param folder: Folder id, name or path
        :param study: study [[user@]project:]study where study and project can be either the id or alias
        :param include: fields included in the response, whole JSON path must be provided
        :param exclude: fields excluded in the response, whole JSON path must be provided
        :param limit: number of results to be returned in the queries
        :param skip: number of results to skip in the queries
        :param count: total number of results
        """

        return self._get('list', query_id=folder, **options)

    def content(self, file, **options):
        """
        Show the content of a file (up to a limit)
        URL: /{apiVersion}/files/{file}/content

        :param file: File id, name or path. Paths must be separated by : instead of /
        :param study: Study [[user@]project:]study where study and project can be either the id or alias
        :param start: start (default: -1 -> All file)
        :param limit: limit (default: -1 -> All file)
        """

        return self._get('content', query_id=file, **options)

    def grep(self, file, **options):
        """
        Filter lines of the file containing a match of the pattern [NOT TESTED]
        URL: /{apiVersion}/files/{file}/grep

        :param file: File id, name or path. Paths must be separated by : instead of /
        :param study: Study [[user@]project:]study where study and project can be either the id or alias
        :param pattern: Pattern (default ".*")
        :param ignoreCase: Do a case insensitive search (default = False)
        :param multi: Return multiple matches (default = True)
        """

        return self._get('grep', query_id=file, **options)

    def refresh(self, file, **options):
        """
        Refresh metadata from the selected file or folder. Return updated files.
        URL: /{apiVersion}/files/{file}/refresh

        :param file: File id, name or path. Paths must be separated by : instead of /
        :param study: Study [[user@]project:]study where study and project can be either the id or alias
        """

        return self._get('refresh', query_id=file, **options)

    def tree_folder(self, folder, **options):
        """
        Obtain a tree view of the files and folders within a folder
        URL: /{apiVersion}/files/{folder}/tree

        :param folder: Folder id, name or path. Paths must be separated by : instead of /
            (default root -> ":")
        :param study: study [[user@]project:]study where study and project can be either the id or alias
        :param maxDepth: maximum depth to get files from (default maxDepth = 5)
        :param include: fields included in the response, whole JSON path must be provided
        :param exclude: fields excluded in the response, whole JSON path must be provided
        :param limit: [TO BE IMPLEMENTED] Number of results to be returned in the queries
        """

        return self._get('tree', query_id=folder, **options)

    def upload(self, data, **options):
        """
        Resource to upload a file by chunks
        URL: /{apiVersion}/files/upload

        :param filename: filename
        :param fileFormat: For existing Bioformats see files/formats
        :param bioformat: For existing Bioformats see files/bioformats
        :param study: study [[user@]project:]study where study and project can be either the id or alias
        :param relativeFilePath: path within catalog where the file will be located
            (default: root folder = ".")
        :param description: description
        :param parents: create the parent directories if they do not exist
        """

        return self._post('upload', data=data, **options)

    def download(self, file, **options):
        """
        Download file. The usage of /{file}/download webservice through Swagger is discouraged.
        An special DOWNLOAD permission is needed to download files from OpenCGA.
        URL: /{apiVersion}/files/{file}/download

        :param file: File id, name or path. Paths must be separated by : instead of /
        :param study: Study [[user@]project:]study where study and project can be either the id or alias
        """

        return self._get('download', query_id=file, **options)
