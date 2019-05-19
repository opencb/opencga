from pyopencga.rest_clients._parent_rest_clients import _ParentRestClient, _ParentBasicCRUDClient, _ParentAclRestClient,  _ParentAnnotationSetRestClient

class GA4GH(_ParentRestClient):
    """
    This class contains method for GA4GH ws
    """

    def __init__(self, configuration, session_id=None, login_handler=None, *args, **kwargs):
        _category = "ga4gh"
        super(GA4GH, self).__init__(configuration, _category, session_id, login_handler, *args, **kwargs)


    def responses(self, chrom, pos, allele, beacon, **options):
        """
        Beacon webservices
        URL: /{apiVersion}/ga4gh/responses

        chrom: Chromosome ID. Accepted values: 1-22, X, Y, MT. Note:
            For compatibility with conventions set by some of the existing
            beacons, an arbitrary prefix is accepted as well (e.g. chr1 is
            equivalent to chrom1 and 1).
        pos: Coordinate within a chromosome. Position is a number and is
            0-based.
        allele: Any string of nucleotides A,C,T,G or D, I for deletion and
            insertion, respectively. Note: For compatibility with conventions
            set by some of the existing beacons, DEL and INS identifiers are
            also accepted.
        ref: Genome ID. If not specified, all the genomes supported by the given
            beacons are queried. Note: For compatibility with conventions set by
            some of the existing beacons, both GRC or HG notation are accepted,
            case insensitive.
        beacon: Beacon IDs. If specified, only beacons with the given IDs are
            queried. Responses from all the supported beacons are obtained
            otherwise. Format: [id1,id2].
        """

        options['chrom'] = chrom
        options['pos'] = pos
        options['allele'] = allele
        options['beacon'] = beacon

        return self._get('responses', **options)

    def search_reads(self, data, **options):
        """
        <PEDNING>
        URL: /{apiVersion}/ga4gh/reads/search

        :param data: dict with the following Model:

        {
          "variantSetId": "string",
          "callSetIds": [
            "string"
          ],
          "referenceName": "string",
          "start": 0,
          "end": 0,
          "pageSize": 0,
          "pageToken": "string",
          "schema": {
            "props": {},
            "type": "RECORD",
            "elementType": {},
            "aliases": [
              "string"
            ],
            "enumSymbols": [
              "string"
            ],
            "types": [
              {}
            ],
            "fixedSize": 0,
            "name": "string",
            "fields": [
              {
                "props": {},
                "jsonProps": {}
              }
            ],
            "error": true,
            "namespace": "string",
            "fullName": "string",
            "valueType": {},
            "doc": "string",
            "jsonProps": {}
          }
        }
        """

        return self._post('read', subcategory='search', data=data, **options)

    def search_variants(self, data, **options):
        """
        <PENDING>
        URL: /{apiVersion}/ga4gh/variants/search

	:param data: dict with the following Model:
        {
          "readGroupIds": [
            "string"
          ],
          "referenceId": "string",
          "start": 0,
          "end": 0,
          "pageSize": 0,
          "pageToken": "string",
          "schema": {
            "props": {},
            "type": "RECORD",
            "elementType": {},
            "aliases": [
              "string"
            ],
            "enumSymbols": [
              "string"
            ],
            "types": [
              {}
            ],
            "fixedSize": 0,
            "name": "string",
            "fields": [
              {
                "props": {},
                "jsonProps": {}
              }
            ],
            "error": true,
            "namespace": "string",
            "fullName": "string",
            "valueType": {},
            "doc": "string",
            "jsonProps": {}
          }
        }
        """

        return self._post('variant', subcategory='search', data=data, **options)
