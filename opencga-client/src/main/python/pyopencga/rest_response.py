import sys
import pandas as pd

from pyopencga.commons import deprecated


class RestResponse:
    def __init__(self, response):
        self.apiVersion = response.get('apiVersion')
        self.time = response.get('time')
        self.events = response.get('events')
        self.params = response.get('params')
        self.responses = response.get('responses')

        # TODO: Remove deprecated response in future release. Added for backwards compatibility
        self.response = response.get('responses')

        # TODO: Remove deprecated result. Added for backwards compatibility
        for query_result in self.responses:
            if 'results' in query_result:
                query_result['result'] = query_result['results']

    @deprecated
    def first(self):
        return self.responses[0]

    def get_results(self, response_pos=0):
        """
        Return the list of results of the response_pos response.
        """
        return self.responses[response_pos]['results']

    def get_result(self, result_pos, response_pos=0):
        """
        Return the result 'result_pos' of the response 'response_pos'.
        """
        return self.responses[response_pos]['results'][result_pos]

    def get_responses(self):
        """
        Return the list of responses
        """
        return self.responses

    def get_response(self, response_pos=0):
        """
        Return the response_pos response.
        """
        return self.responses[response_pos]

    def result_iterator(self, response_pos=None):
        """
        Return all results from all responses as an iterator
        """
        if response_pos is None:
            for response in self.responses:
                for result in response['results']:
                    yield result
        else:
            for result in self.responses[response_pos]['results']:
                yield result

    @staticmethod
    def _get_param_value(result, field):
        items = field.split('.')
        result2 = result
        is_list = False
        for item in items:
            if item in result2:
                if isinstance(result2[item], list):
                    if is_list:
                        return '.'
                    is_list = True
                result2 = result2[item]
            else:
                if is_list:
                    if len(result2) == 0 or isinstance(result2[0][item], list):
                        return '.'
                    result2 = [r[item] for r in result2 if item in r]
                else:
                    return '.'
        if is_list:
            if len(result2) == 0 or isinstance(result2[0], (list, dict)):
                return '.'
            return ','.join(map(str, set(result2)))
        else:
            return result2

    def print_results(self, fields=None, response_pos=None, limit=None, separator='\t', title=None,
                      metadata=True, outfile=None):
        outfhand = sys.stdout if outfile is None else open(outfile, 'w')
        if title is not None:
            outfhand.write(title + '\n' + '-'*(len(title)+5) + '\n')
        if metadata:
            for event_type in ['INFO', 'WARNING', 'ERROR']:
                for event in self.get_response_events(event_type):
                    msg = '#{}: {}'.format(event['type'], event['description'])
                    if 'id' in event:
                        msg += ' ({})'.format(event['id'])
                    outfhand.write(msg + '\n')
            outfhand.write('#Time: {}\n'.format(self.time))
            outfhand.write('#Num matches: {}\n'.format(self.get_num_matches(response_pos)))
            outfhand.write('#Num results: {}\n'.format(self.get_num_results(response_pos)))
            outfhand.write('#Num inserted: {}\n'.format(self.get_num_inserted(response_pos)))
            outfhand.write('#Num updated: {}\n'.format(self.get_num_updated(response_pos)))
            outfhand.write('#Num deleted: {}\n'.format(self.get_num_deleted(response_pos)))

        responses = [self.get_response(response_pos)] if response_pos is not None else self.get_responses()
        for response in responses:
            if fields:
                fields = fields.split(',')
            elif 'include' in self.params:
                fields = self.params['include'].split(',')
            else:
                if response['results']:
                    fields = response['results'][0].keys()

            limit = limit if limit is not None else len(response['results'])
            if limit:
                outfhand.write('#{}\n'.format(separator.join(fields)))
            for result in response['results'][:limit]:
                values = [self._get_param_value(result, field) for field in fields]
                outfhand.write(separator.join(map(str, values)) + '\n')

    def to_data_frame(self, response_pos=None):
        responses = [self.get_response(response_pos)] if response_pos is not None else self.get_responses()
        return pd.concat([pd.json_normalize(result) for response in responses
                          for result in response['results']]).reset_index(drop=True)

    def get_response_events(self, event_type=None):
        """
        Return response events by name
        """
        event_names = ['INFO', 'WARNING', 'ERROR']
        if event_type is None or self.events is None:
            return self.events or []
        elif event_type in event_names:
            return [event for event in self.events if event['type'] == event_type]
        else:
            msg = 'Argument "type" must be one of the following values: "{}"'
            raise ValueError(msg.format(', '.join(event_names)))

    def get_result_events(self, event_type=None, response_pos=0):
        """Return result events by name and position"""
        event_names = ['INFO', 'WARNING', 'ERROR']
        response = self.responses[response_pos]
        if event_type is None:
            return response['events'] \
                if 'events' in response and response['events'] else []
        elif event_type in event_names:
            return [event for event in response['events'] if event['type'] == event_type] \
                if 'events' in response and response['events'] else []
        else:
            msg = 'Argument "type" must be one of the following values: "{}"'
            raise ValueError(msg.format(', '.join(event_names)))

    def get_num_matches(self, response_pos=None):
        """
        Return number of matches
        """
        if response_pos is not None:
            return self.responses[response_pos]['numMatches']
        else:
            num_matches = 0
            for query_result in self.responses:
                if 'numMatches' in query_result:
                    num_matches += query_result['numMatches']
            return num_matches

    def get_num_results(self, response_pos=None):
        """
        Return number of results
        """
        if response_pos is not None:
            return self.responses[response_pos]['numResults']
        else:
            num_results = 0
            for query_result in self.responses:
                if 'numResults' in query_result:
                    num_results += query_result['numResults']
            return num_results

    def get_num_inserted(self, response_pos=None):
        """
        Return number of inserted
        """
        if response_pos is not None:
            return self.responses[response_pos]['numInserted']
        else:
            num_inserted = 0
            for query_result in self.responses:
                if 'numInserted' in query_result:
                    num_inserted += query_result['numInserted']
            return num_inserted

    def get_num_updated(self, response_pos=None):
        """
        Return number of updated
        """
        if response_pos is not None:
            return self.responses[response_pos]['numUpdated']
        else:
            num_updated = 0
            for query_result in self.responses:
                if 'numUpdated' in query_result:
                    num_updated += query_result['numUpdated']
            return num_updated

    def get_num_deleted(self, response_pos=None):
        """
        Return number of deleted
        """
        if response_pos is not None:
            return self.responses[response_pos]['numDeleted']
        else:
            num_deleted = 0
            for query_result in self.responses:
                if 'numDeleted' in query_result:
                    num_deleted += query_result['numDeleted']
            return num_deleted
