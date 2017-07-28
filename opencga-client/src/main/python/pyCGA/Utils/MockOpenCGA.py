import json
import re

import httpretty


class OpenCGAMock(object):
    def __init__(self, enable_login=True):
        httpretty.enable()
        self.server = "http://mock-opencga/opencga"
        self.base = "http://mock-opencga/opencga/webservices/rest/(.+)/"
        self.response_base = {"apiVersion": "v1", "warning": "", "error": "",
                              "queryOptions": {"metadata": True, "limit": 2000}, "response": []}
        httpretty.register_uri(httpretty.HEAD, self.server, status=302)
        if enable_login:
            self.enable_login()

    def create_response(self, raw_results):
        response = self.response_base
        number_of_results = len(raw_results)
        results = [{"numTotalResults": number_of_results, "numResults": number_of_results,
                    "resultType": "org.opencb.commons.type",
                    "result": raw_results, 'time': 0, 'id': 'id', 'dbTime': 0}]
        response['response'] = results
        return response

    @staticmethod
    def create_results(fields):
        len_values = [len(value) for value in fields.values()]
        if len(set(len_values)) != 1:
            raise ValueError('Not all the fields has the same length')
        results = []
        for f in fields:
            for i, value in enumerate(fields[f]):
                if len(results) < i + 1:
                    results.append({})
                results[i][f] = value
        return results

    def enable_login(self):
        result = [{"date": "20170128014231", "ip": "10.5.0.2", "sessionId": "XOkCfKX09FV0YyPJCBvd",
                   "id": "XOkCfKX09FuriV0YyPJCBvd"}]
        user_response = self.create_response(raw_results=result)
        httpretty.register_uri(httpretty.POST, re.compile(self.base + 'users/(.+)/login'), status=200,
                               body=json.dumps(user_response),
                               content_type='application/json',
                               )

    def enable_service(self, regex, method, fields, status=200):

        httpretty.register_uri(method, re.compile(self.base + regex), status=status,
                               body=json.dumps(self.create_response(self.create_results(fields))),
                               content_type='application/json',
                               )
