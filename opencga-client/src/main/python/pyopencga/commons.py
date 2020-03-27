import sys
import threading
from time import sleep
import warnings

import requests

from pyopencga.exceptions import OpencgaInvalidToken, OpencgaAuthorisationError

try:
    from Queue import Queue
except ImportError:
    from queue import Queue

_CALL_BATCH_SIZE = 2000
_NUM_THREADS_DEFAULT = 4


def deprecated(func):
    """Prints a warning for functions marked as deprecated"""
    def new_func(*args, **kwargs):
        warnings.simplefilter('always', DeprecationWarning)  # turn off filter
        warnings.warn('Call to deprecated function "{}".'.format(func.__name__),
                      category=DeprecationWarning, stacklevel=2)
        warnings.simplefilter('default', DeprecationWarning)  # reset filter
        return func(*args, **kwargs)
    return new_func


def _create_rest_url(host, version, sid, category, resource, subcategory=None, query_id=None,
                     second_query_id=None, options=None):
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

    if subcategory is not None:
        url += '/' + subcategory

    if second_query_id is not None:
        url += '/' + second_query_id

    url += '/' + resource

    header = {"Accept-Encoding": "gzip"}
    if sid is not None:
        header['Authorization'] = 'Bearer {}'.format(sid)

    # Checking optional params
    if options is not None:
        opts = []
        for k, v in options.items():
            if k == 'debug':
                continue
            if isinstance(v, list):
                opts.append(k + '=' + ','.join(map(str, v)))
            else:
                opts.append(k + '=' + str(v))
        if opts:
            url += '?' + '&'.join(opts)
    return url, header


def _fetch(host, version, sid, category, resource, method, subcategory=None, query_id=None,
           second_query_id=None, data=None, options=None):
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
        opts = options.copy()  # Do not modify original data!
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
                                       options=opts)

        # DEBUG param
        if opts is not None and 'debug' in opts and opts['debug']:
            sys.stderr.write(url + '\n')

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
            raise OpencgaInvalidToken(r.content)
        elif r.status_code == 403:
            raise OpencgaAuthorisationError(r.content)

        elif r.status_code != 200:
            raise Exception(r.content)

        try:
            response = r.json()

            # TODO Remove deprecated response and result in future release. Added for backwards compatibility
            if 'response' in response:
                response['responses'] = response['response']
            for query_result in response['responses']:
                if 'result' in query_result:
                    query_result['results'] = query_result['result']

        except ValueError:
            msg = 'Bad JSON format retrieved from server'
            raise ValueError(msg)

        # Setting up final_response
        if final_response is None:
            final_response = response
        # Concatenating results
        else:
            if query_id is not None:
                for index, res in enumerate(response['responses']):
                    id_index = current_id_indexes[index]
                    final_response[id_index]['results'] += res['results']
            else:
                final_response['responses'][0]['results'] += response['responses'][0]['results']

        if query_id is not None:
            # Checking which ids are completely retrieved
            next_id_list = []
            next_id_indexes = []
            for index, res in enumerate(response['responses']):
                if res['numResults'] == call_limit:
                    next_id_list.append(current_id_list[index])
                    next_id_indexes.append(current_id_indexes[index])
            # Ending REST calling when there are no more ids to retrieve
            if not next_id_list:
                call = False
        else:
            # Ending REST calling when there are no more results to retrieve
            if response['responses'][0]['numResults'] != call_limit:
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


def _worker(queue, results, host, version, sid, category, resource, method, subcategory=None,
            second_query_id=None, data=None, options=None):

    """Manages the queue system for the threads"""
    while True:
        # Fetching new element from the queue
        index, query_id = queue.get()
        response = _fetch(host=host, version=version, sid=sid, category=category, subcategory=subcategory,
                          resource=resource, method=method, data=data, query_id=query_id,
                          second_query_id=second_query_id, options=options)
        # Store data in results at correct index
        results[index] = response
        # Signaling to the queue that task has been processed
        queue.task_done()


def merge_query_responses(query_response_list):
    final_response = query_response_list[0]
    for i, query_response in enumerate(query_response_list):
        if i != 0:
            final_response['events'] += query_response['events']
            final_response['time'] += query_response['time']
            # final_response['responses'] += response['responses']

            for key in query_response['params']:
                if final_response['params'][key] != query_response['params'][key]:
                    final_response['params'][key] += ',' + query_response['params'][key]

            for j, query_result in enumerate(query_response['responses']):
                if len(final_response['responses'])-1 < j:
                    final_response['responses'] += []
                for key in query_result:
                    if key not in final_response['responses'][j]:
                        final_response['responses'][j][key] = query_result[key]
                    else:
                        if isinstance(query_result[key], (int, list)):
                            final_response['responses'][j][key] += query_result[key]
    return final_response


def execute(host, version, sid, category, resource, method, subcategory=None, query_id=None,
            second_query_id=None, data=None, options=None):
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
                          resource=resource, method=method, data=data, query_id=query_id,
                          second_query_id=second_query_id, options=options)
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
    final_query_response = merge_query_responses(res)

    return final_query_response


