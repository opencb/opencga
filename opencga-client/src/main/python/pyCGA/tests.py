import json
import re
import unittest
from urlparse import parse_qs

import httpretty
from sure import expect

from pyCGA.opencgarestclients import OpenCGAClient


class OpenCGAClinetTest(unittest.TestCase):
    def create_paginate_response(self, request, uri, headers):
        """

        :type url: re
        :param url:
        :return:
        """

        list_of_file = [{"name": str(i), "id": i} for i in range(0, 10000)]
        response = {"apiVersion": "v1", "warning": "", "error": "",
                    "queryOptions": {"metadata": True, "limit": 2000}, "response": []}

        p = parse_qs(uri.split('?')[1])
        limit = int(p['limit'][0])
        response['limit'] = limit
        skip = int(p['skip'][0])
        response['skip'] = skip
        selected_results = list_of_file[skip: skip + limit]
        response1 = [{"numTotalResults": len(list_of_file), "numResults": len(selected_results),
                      "resultType": "org.opencb.commons.datastore.core.ObjectMap",
                      "time": 0, "id": "You successfully logged in", "dbTime": 5,
                      "result": selected_results}]
        response['response'] = response1

        return 200, headers, json.dumps(response)

    def create_paginate_response_no_count(self, request, uri, headers):
        """

        :type url: re
        :param url:
        :return:
        """

        list_of_variants = [{"name": str(i), "id": i} for i in range(0, 10000)]
        response = {"apiVersion": "v1", "warning": "", "error": "",
                    "queryOptions": {"metadata": True, "limit": 2000}, "response": []}

        p = parse_qs(uri.split('?')[1])
        limit = int(p['limit'][0])
        response['limit'] = limit
        skip = int(p['skip'][0])
        response['skip'] = skip
        selected_results = list_of_variants[skip: skip + limit]
        response1 = [{"numTotalResults": -1, "numResults": len(selected_results),
                      "resultType": "org.opencb.commons.datastore.core.ObjectMap",
                      "time": 0, "id": "You successfully logged in", "dbTime": 5,
                      "result": selected_results}]
        response['response'] = response1

        return 200, headers, json.dumps(response)

    def setUp(self):
        httpretty.enable()
        server = "http://mock-opencga/opencga"
        base = "http://mock-opencga/opencga/webservices/rest/(.+)/"
        response_base = {"apiVersion": "v1", "warning": "", "error": "",
                         "queryOptions": {"metadata": True, "limit": 2000}, "response": []}
        user_results = [{"numTotalResults": 1, "numResults": 1,
                         "resultType": "org.opencb.commons.datastore.core.ObjectMap",
                         "result": [{"date": "20170128014231", "ip": "10.5.0.2", "sessionId": "XOkCfKX09FV0YyPJCBvd",
                                     "id": "XOkCfKX09FV0YyPJCBvd"}],
                         "time": 0, "id": "You successfully logged in", "dbTime": 5}]

        httpretty.register_uri(httpretty.HEAD, server, status=302)
        response_base['response'] = user_results
        httpretty.register_uri(httpretty.POST, re.compile(base + 'users/(.+)/login'), status=200,
                               body=json.dumps(response_base),
                               content_type='application/json',
                               )
        httpretty.register_uri(httpretty.GET, re.compile(base + 'projects/(.+)/info'), status=200,
                               body=json.dumps(response_base),
                               content_type='application/json',
                               )
        httpretty.register_uri(httpretty.GET, re.compile(base + 'files/search'), status=200,
                               body=self.create_paginate_response,
                               content_type='application/json',
                               )
        httpretty.register_uri(httpretty.POST, re.compile(base + 'analysis/variant/query'), status=200,
                               body=self.create_paginate_response_no_count,
                               content_type='application/json',
                               )

        self.configuration = {
            'version': 'v1',
            'rest': {
                'hosts': [
                    'mock-opencga/opencga'
                ]
            }
        }

    def test_sid_generation(self):
        open_cga_client = OpenCGAClient(self.configuration, user='pepe', pwd='pepe')
        expect(open_cga_client.session_id).to.equal('XOkCfKX09FV0YyPJCBvd')
        open_cga_client = OpenCGAClient(self.configuration, session_id='XOkCfKX09FV0YyPJCBvd')
        expect(open_cga_client.session_id).to.equal('XOkCfKX09FV0YyPJCBvd')
        open_cga_client.projects.info('pt')
        expect(httpretty.last_request()).to.have.property(
            "headers").which.have.property('headers').which.should.equal(['Host: mock-opencga\r\n',
                                                                          'Connection: keep-alive\r\n',
                                                                          'Accept-Encoding: gzip\r\n',
                                                                          'Accept: */*\r\n',
                                                                          'User-Agent: python-requests/2.17.3\r\n',
                                                                          'Authorization: Bearer XOkCfKX09FV0YyPJCBvd\r\n']
                                                                         )

    def test_extra_parameters(self):
        open_cga_client = OpenCGAClient(self.configuration, session_id='XOkCfKX09FV0YyPJCBvd')
        open_cga_client.projects.info('pt', extra='as_argument', **{'extra2': 'as_kwargs'})
        expect(httpretty.last_request()).to.have.property(
            "querystring").which.should.have.key('extra').which.should.equal(['as_argument'])
        expect(httpretty.last_request()).to.have.property(
            "querystring").which.should.have.key('extra2').which.should.equal(['as_kwargs'])

    def test_pagination(self):
        open_cga_client = OpenCGAClient(self.configuration, session_id='XOkCfKX09FV0YyPJCBvd')
        r = open_cga_client.files.search(study='st')
        expect(len(r.get())).equal_to(10000)
        r = open_cga_client.files.search(study='st', limit=500)
        expect(len(r.get())).equal_to(500)
        for batch in open_cga_client.analysis_variant.query(5001, data={}):
            expect(len(batch.get())).lower_than_or_equal_to(5001)

    def tearDown(self):
        httpretty.disable()
