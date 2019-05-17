import itertools
import threading
from time import sleep

import requests

from pyopencga.exceptions import OpenCgaInvalidToken, OpenCgaAuthorisationError

try:
    from Queue import Queue
except ImportError:
    from queue import Queue

_CALL_BATCH_SIZE = 2000
_NUM_THREADS_DEFAULT = 4


class QueryResponse:
    def __init__(self, response):
        self.time = response.get('time')
        self.apiVersion = response.get('apiVersion')
        self.queryOptions = response.get('queryOptions')
        self.warning = response.get('warning')
        self.error = response.get('error')
        self.response = response.get('response')

    def first(self):
        return self.response[0]

    def result(self, position=None):
        """
        Return the first result of the QueryResult in the position 'position'.
        If no position is passed, it will return the one from the first
        QueryResult
        """
        pos = 0
        if position is not None:
            pos = position
        if isinstance(self.response[pos]['result'], list):
            return self.response[pos]['result'][0]
        else:
            # This is a special scenario that only happens in AnalysisResults
            # where result is not array
            return self.response[pos]['result']

    def results(self):
        """
        Iterates over all the results of all the QueryResults
        """
        for query_result in self.response:
            if isinstance(query_result['result'], list):
                for result in query_result['result']:
                    yield result
            else:
                # This is a special scenario that only happens in AnalysisResults
                # where result is not array
                yield query_result['result']

    def num_total_results(self):
        """
        Return the total number of results taking into account the whole list of QueryResults
        """
        num_results = 0
        for query_result in self.response:
            num_results += query_result['numResults']
        return num_results


def _create_rest_url(host, version, sid, category, resource, subcategory=None, query_id=None,
                     second_query_id=None, **options):
    """Creates the URL for querying the REST service"""

    # Creating the basic URL
    url = ('/'.join([host,
                     'webservices/rest',
                     version,
                     category
                     ]))

    # If subcategory is queried, query_id can be absent
    if query_id is not None:
        url += '/' + query_id

    url += '/' + resource

    if second_query_id is not None:
        url += '/' + second_query_id

    if subcategory is not None:
        url += '/' + subcategory

    header = {"Accept-Encoding": "gzip"}
    if sid is not None:
        header['Authorization'] = 'Bearer {}'.format(sid)

    # Checking optional params
    if options is not None:
        opts = []
        for k, v in options.items():
            opts.append(k + '=' + str(v))
        if opts:
            url += '?' + '&'.join(opts)

    return url, header


def _fetch(host, version, sid, category, resource, method, subcategory=None, query_id=None,
           second_query_id=None, data=None, **options):
    """Queries the REST service retrieving results until exhaustion or limit"""
    # HERE BE DRAGONS
    final_response = None

    # Setting up skip and limit default parameters
    call_skip = 0
    call_limit = 1000
    max_limit = None
    if options is None:
        opts = {'skip': call_skip, 'limit': call_limit}
    else:
        opts = options.copy()['options']  # Do not modify original data!
        if 'skip' not in opts:
            opts['skip'] = call_skip
        # If 'limit' is specified, a maximum of 'limit' results will be returned
        if 'limit' in opts:
            max_limit = opts['limit']
        # Server must be always queried for results in groups of 1000
        opts['limit'] = call_limit

    # If there is a query_id, the next variables will be used
    total_id_list = []  # All initial ids
    next_id_list = []  # Ids which should be queried again for more results
    next_id_indexes = []  # Ids position in the final response
    if query_id is not None:
        total_id_list = query_id.split(',')

    # If some query has more than 'call_limit' results, the server will be
    # queried again to retrieve the next 'call_limit results'
    call = True
    current_query_id = None  # Current REST query
    current_id_list = None  # Current list of ids
    time_out_counter = 0  # Number of times a query is repeated due to time-out
    while call:
        # Check 'limit' parameter if there is a maximum limit of results
        if max_limit is not None and max_limit <= call_limit:
            opts['limit'] = max_limit

        # Updating query_id and list of ids to query
        if query_id is not None:
            if current_query_id is None:
                current_query_id = query_id
                current_id_list = total_id_list
                current_id_indexes = range(len(total_id_list))
            else:
                current_query_id = ','.join(next_id_list)
                current_id_list = next_id_list
                current_id_indexes = next_id_indexes

        # Retrieving url
        url, header = _create_rest_url(host=host,
                                       version=version,
                                       category=category,
                                       sid=sid,
                                       subcategory=subcategory,
                                       query_id=current_query_id,
                                       second_query_id=second_query_id,
                                       resource=resource,
                                       **opts)
        # print(url)  # DEBUG

        # Getting REST response
        if method == 'get':
            try:
                r = requests.get(url, headers=header)
            except requests.exceptions.ConnectionError:
                sleep(1)
                r = requests.get(url, headers=header)

        elif method == 'post':
            try:
                r = requests.post(url, json=data, headers=header)
            except requests.exceptions.ConnectionError:
                sleep(1)
                r = requests.post(url, json=data, headers=header)

        else:
            raise NotImplementedError('method: ' + method + ' not implemented.')

        if r.status_code == 504:  # Gateway Time-out
            if time_out_counter == 99:
                msg = 'Server not responding in time'
                raise requests.ConnectionError(msg)
            time_out_counter += 1
            continue
        time_out_counter = 0

        if r.status_code == 401:
            raise OpenCgaInvalidToken(r.content)
        elif r.status_code == 403:
            raise OpenCgaAuthorisationError(r.content)

        elif r.status_code != 200:
            raise Exception(r.content)

        try:
            response = r.json()
        except ValueError:
            msg = 'Bad JSON format retrieved from server'
            raise ValueError(msg)

        # Setting up final_response
        if final_response is None:
            final_response = response
        # Concatenating results
        else:
            if query_id is not None:
                for index, res in enumerate(response['response']):
                    id_index = current_id_indexes[index]
                    final_response[id_index]['result'] += res['result']
            else:
                final_response['response'][0]['result'] += response['response'][0]['result']

        if query_id is not None:
            # Checking which ids are completely retrieved
            next_id_list = []
            next_id_indexes = []
            for index, res in enumerate(response['response']):
                if res['numResults'] == call_limit:
                    next_id_list.append(current_id_list[index])
                    next_id_indexes.append(current_id_indexes[index])
            # Ending REST calling when there are no more ids to retrieve
            if not next_id_list:
                call = False
        else:
            # Ending REST calling when there are no more results to retrieve
            if response['response'][0]['numResults'] != call_limit:
                call = False

        # Skipping the first 'limit' results to retrieve the next ones
        opts['skip'] += call_limit

        # Subtracting the number of returned results from the maximum goal
        if max_limit is not None:
            max_limit -= call_limit
            # When 'limit' is 0 returns all the results. So, break the loop if 0
            if max_limit == 0:
                break

    return final_response


def _worker(queue, results, host, version, sid, species, category, resource, method, subcategory=None,
            second_query_id=None, data=None, **options):
    """Manages the queue system for the threads"""
    while True:
        # Fetching new element from the queue
        index, query_id = queue.get()
        response = _fetch(host=host, version=version, sid=sid, species=species, category=category,
                          subcategory=subcategory,
                          resource=resource, method=method, data=data,
                          query_id=query_id, second_query_id=second_query_id, **options)
        # Store data in results at correct index
        results[index] = response
        # Signaling to the queue that task has been processed
        queue.task_done()


def execute(host, version, sid, category, resource, method, subcategory=None, query_id=None,
            second_query_id=None, data=None, **options):
    """Queries the REST service using multiple threads if needed"""

    # If query_id is an array, convert to comma-separated string
    if query_id is not None:
        if isinstance(query_id, list):
            query_id = ','.join([str(item) for item in query_id])
        else:
            query_id = str(query_id)  # convert to string so we can call this method with int ids

    # Multithread if the number of queries is greater than _CALL_BATCH_SIZE
    if query_id is None or len(query_id.split(',')) <= _CALL_BATCH_SIZE:
        response = _fetch(host=host, version=version, sid=sid, category=category, subcategory=subcategory,
                          resource=resource, method=method, data=data,
                          query_id=query_id, second_query_id=second_query_id, **options)
        return response
    else:
        if options is not None and 'num_threads' in options:
            num_threads = options['num_threads']
        else:
            num_threads = _NUM_THREADS_DEFAULT

        # Splitting query_id into batches depending on the call batch size
        id_list = query_id.split(',')
        id_batches = [','.join(id_list[x:x + _CALL_BATCH_SIZE])
                      for x in range(0, len(id_list), _CALL_BATCH_SIZE)]

        # Setting up the queue to hold all the id batches
        q = Queue(maxsize=0)
        # Creating a size defined list to store thread results
        res = [''] * len(id_batches)

        # Setting up the threads
        for thread in range(num_threads):
            t = threading.Thread(target=_worker,
                                 kwargs={'queue': q,
                                         'results': res,
                                         'host': host,
                                         'version': version,
                                         'sid': sid,
                                         'category': category,
                                         'subcategory': subcategory,
                                         'second_query_id': second_query_id,
                                         'resource': resource,
                                         'method': method,
                                         'data': data,
                                         'options': options})
            # Setting threads as "daemon" allows main program to exit eventually
            # even if these do not finish correctly
            t.setDaemon(True)
            t.start()

        # Loading up the queue with index and id batches for each job
        for index, batch in enumerate(id_batches):
            q.put((index, batch))  # Notice this is a tuple

        # Waiting until the queue has been processed
        q.join()

    # Joining all the responses into a one final response
    final_response = list(itertools.chain.from_iterable(res))

    return final_response
