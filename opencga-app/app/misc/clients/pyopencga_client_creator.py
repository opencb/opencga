import sys
import os
import re
import requests
import argparse


CATEGORIES = {
    'Users': 'Users',
    'Projects': 'Projects',
    'Studies': 'Studies',
    'Files': 'Files',
    'Jobs': 'Jobs',
    'Samples': 'Samples',
    'Individuals': 'Individuals',
    'Families': 'Families',
    'Cohorts': 'Cohorts',
    'Disease Panels': 'Panels',
    'Analysis - Alignment': 'Alignment',
    'Analysis - Variant': 'Variant',
    'Analysis - Clinical Interpretation': 'Clinical',
    'Operations - Variant Storage': 'VariantOperations',
    'Meta': 'Meta',
    'GA4GH': 'GA4GH',
    'Admin': 'Admin'
}

ENDPOINTS = {
    '/{apiVersion}/users/{user}/configs/filters/update': {'method_name': 'update_filters'},
    '/{apiVersion}/users/{user}/configs/filters/{name}/update': {'method_name': 'update_filter'},
    '/{apiVersion}/analysis/clinical/{clinicalAnalysis}/interpretations/{interpretation}/update': {'method_name': 'update_interpretation'},
    '/{apiVersion}/ga4gh/reads/{study}/{file}': {'method_name': 'fetch_reads'}
}

PARAMS_TYPE = {
    'string': 'str',
    'integer': 'int',
    'map': 'dict',
    'boolean': 'bool',
    'enum': 'str'
}


def _setup_argparse():
    desc = 'This script creates automatically all RestClients files'
    parser = argparse.ArgumentParser(description=desc,
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('server_url', help='server URL')
    parser.add_argument('output_dir', help='output directory')
    args = parser.parse_args()
    return args


def any_arg(items):
    return any([True if '{' in item and '}' in item else False for item in items])


def all_arg(items):
    return all([True if '{' in item and '}' in item else False for item in items])


def get_method_name(endpoint_path, category):
    method_name = ''
    subpath = endpoint_path.replace(category['path'] + '/', '')
    items = subpath.split('/')
    if len(items) == 1:
        method_name = items[0]
    if len(items) == 2:
        # e.g. /{apiVersion}/ga4gh/reads/search
        if not any_arg(items):
            method_name = '_'.join(items[::-1])
        # e.g. /{apiVersion}/users/{user}/info
        elif any_arg([items[0]]) and not any_arg([items[1]]):
            method_name = items[1]
    if len(items) == 3:
        # e.g. /{apiVersion}/analysis/variant/cohort/stats/run
        if not any_arg(items):
            method_name = '_'.join([items[2], items[0], items[1]])
        # e.g. /{apiVersion}/users/{user}/configs/filters
        elif any_arg([items[0]]) and not any_arg([items[1:]]):
            method_name = '_'.join([items[2], items[1]])
        # e.g. /{apiVersion}/studies/acl/{members}/update
        elif any_arg([items[1]]) and not any_arg([items[0], items[2]]):
            method_name = '_'.join([items[2], items[0]])
    if len(items) == 4:
        # e.g. /{apiVersion}/operation/variant/sample/genotype/index
        if not any_arg(items):
            method_name = '_'.join([items[3], items[1], items[2]])
    if len(items) == 5:
        # e.g. /{apiVersion}/files/{file}/annotationSets/{annotationSet}/annotations/update
        if all_arg([items[0], items[2]]) and not any_arg([items[1], items[3], items[4]]):
            method_name = '_'.join([items[4], items[3]])
    if not method_name:
        NotImplementedError('Case not implemented for PATH: "{}"'.format(endpoint_path))
    return re.sub(r'(?<!^)(?=[A-Z])', '_', method_name).lower()


def to_snake_case(text):
    return re.sub(r'(?<!^)(?=[A-Z])', '_', text).lower()


def main():
    # Getting arg parameters
    args = _setup_argparse()
    server_url = args.server_url
    output_dir = args.output_dir

    create_rest_clients(server_url, output_dir)


def create_rest_clients(server_url, output_dir):
    for category in requests.get(server_url + '/webservices/rest/v2/meta/api').json()['responses'][0]['results'][0]:
        version = requests.get(server_url + '/webservices/rest/v2/meta/about').json()['responses'][0]['results'][0]['Version'].split('-')[0]
        text = []
        text.append('from pyopencga.rest_clients._parent_rest_clients import _ParentRestClient')
        text.append('')
        text.append('')
        text.append('class {}(_ParentRestClient):'.format(CATEGORIES[category['name']]))
        text.append('{}"""'.format(' ' * 4))
        text.append('{}This class contains methods for the \'{}\' webservices'.format(' '*4, category['name']))
        text.append('{}Client version: {}'.format(' '*4, version))
        text.append('{}PATH: {}'.format(' ' * 4, category['path']))
        text.append('{}"""'.format(' ' * 4))
        text.append('')
        text.append('{}def __init__(self, configuration, token=None, login_handler=None, *args, **kwargs):'.format(
            ' ' * 4, category['name'], category['path'])
        )
        text.append('{}_category = \'{}\''.format(' '*8, category['path'].replace('/{apiVersion}/', '')))
        text.append('{}super({}, self).__init__(configuration, _category, token, login_handler, *args, **kwargs)'.format(
            ' ' * 8, CATEGORIES[category['name']])
        )
        text.append('')

        for endpoint in category['endpoints']:

            method_name = get_method_name(endpoint['path'], category) if endpoint['path'] not in ENDPOINTS else ENDPOINTS[endpoint['path']]['method_name']

            # Getting queries and subcategories for the method
            endpoint_subpath = endpoint['path'].replace(category['path'] + '/', '')
            queries = re.findall(r'{(.*?)}', endpoint_subpath)
            if queries:
                regex = ''.join(['{' + i + '}(.+)' for i in queries])
                resources = re.findall(regex, endpoint_subpath)
                if resources:
                    resources = resources if type(resources[0]) != tuple else list(resources[0])
            else:
                resources = [endpoint_subpath]
            resources = [i.strip('/') for i in resources]

            # Getting parameters info
            params = [
                '{}:param {} {}: {}'.format(
                    ' '*8,
                    PARAMS_TYPE[parameter['type']] if parameter['type'] in PARAMS_TYPE else parameter['type'],
                    to_snake_case(parameter['name']),
                    parameter['description'].replace('"', '\'').replace('\n', '\n' + ' '*12))
                for parameter in endpoint['parameters'] if parameter['name'] != 'body'
            ]
            params += [
                '{}:param dict data: {}'.format(
                    ' '*8, parameter['description'].replace('"', '\'')
                ).replace('\n', '\n' + ' '*12)
                for parameter in endpoint['parameters'] if parameter['name'] == 'body'
            ]

            # Getting required parameters for the method
            all_parameters = [
                to_snake_case(parameter['name']) if parameter['name'] != 'body' else 'data'
                for parameter in endpoint['parameters']
            ]
            required_parameters = [
                to_snake_case(parameter['name']) if parameter['name'] != 'body' else 'data'
                for parameter in endpoint['parameters']
                if parameter['required'] and parameter['name'] not in queries
            ]
            method_body = ['{i}options[\'{r}\'] = {r}'.format(i=' '*8, r=required_parameter)
                           for required_parameter in required_parameters if required_parameter != 'data']

            # Method arguments
            method_args = ['self']
            for query in queries:
                method_args.append(to_snake_case(query))
            for required_parameter in required_parameters:
                method_args.append(required_parameter)
            if endpoint['method'] == 'POST' and 'data' in all_parameters and 'data' not in required_parameters:
                method_args.append('data=None')
            method_args.append('**options')
            method_args = ', '.join(method_args)

            # Calling arguments
            call_args = []
            if resources:
                call_args = ['\'{}\''.format(resources[0])]
            if queries:
                call_args.append('query_id={}'.format(to_snake_case(queries[0])))
            if len(resources) > 1:
                call_args.append('subcategory=\'{}\''.format(resources[1]))
            if len(queries) > 1:
                call_args.append('second_query_id={}'.format(to_snake_case(queries[1])))
            if endpoint['method'] == 'POST' and 'data' in all_parameters:
                call_args.append('data=data')
            call_args.append('**options')
            call_args = ', '.join(call_args)

            text.append('{}def {}({}):'.format(' ' * 4, method_name, method_args))
            text.append('{}"""'.format(' ' * 8))
            text.append('{}{}'.format(' ' * 8, endpoint['description'].replace('"', '\'')))
            text.append('{}PATH: {}'.format(' ' * 8, endpoint['path']))
            text.append('')
            text += params if params else ['']
            text.append('{}"""'.format(' ' * 8))
            text += method_body if method_body else ['']
            text.append('{}return self.{}({})'.format(' ' * 8, '_' + endpoint['method'].lower(), call_args))
            text.append('')

        file_name = to_snake_case(CATEGORIES[category['name']]) + '_client.py'\
            if CATEGORIES[category['name']] != 'GA4GH' else CATEGORIES[category['name']].lower() + '_client.py'
        sys.stderr.write('Creating ' + os.path.join(output_dir, file_name) + '...\n')
        with open(os.path.join(output_dir, file_name), 'w') as fhand:
            fhand.write('\n'.join(text))
        # print('\n'.join(text))


if __name__ == '__main__':
    sys.exit(main())
