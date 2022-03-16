#!/usr/bin/env python3

import argparse
import sys
import re

from rest_client_generator import RestClientGenerator


class PythonClientGenerator(RestClientGenerator):

    def __init__(self, server_url, output_dir):
        super().__init__(server_url, output_dir)

        self.param_types = {
            'string': 'str',
            'integer': 'int',
            'int': 'int',
            'long': 'int',
            'map': 'dict',
            'boolean': 'bool',
            'enum': 'str',
            'list': 'list',
            'object': 'dict',
            'inputstream': 'inputstream'
        }

    @staticmethod
    def to_snake_case(text):
        return re.sub(r'(?<!^)(?=[A-Z])', '_', text).lower()

    @staticmethod
    def to_camel_case(text):
        components = text.split('_')
        return components[0] + ''.join(x.title() for x in components[1:])

    @staticmethod
    def format_line(line, max_length=79):
        new_lines = []
        new_line = []
        len_line = 0
        indent = len(line) - len(line.lstrip())
        line = line.replace('\"', '\'')
        for word in line.split(' '):
            if len_line + len(word) + 1 <= max_length:
                new_line.append(word)
                len_line += len(word) + 1
            else:
                new_lines.append(' '.join(new_line))
                new_line = [' '*(indent + 4) + word]
                len_line = len(new_line[0])
        new_lines.append(' '.join(new_line))
        return '\n'.join(new_lines)

    def get_imports(self):
        text = '"""\n'
        text += self.get_autogenerated_message()[0] + '\n'
        text += '\n    '.join(self.get_autogenerated_message()[1:]) + '\n'
        text += '"""\n\n'
        text += 'from pyopencga.rest_clients._parent_rest_clients import ' \
                '_ParentRestClient\n\n'
        return text

    def get_class_definition(self, category):
        text = []
        text.append('class {}(_ParentRestClient):'.format(self.categories[self.get_category_name(category)]))
        text.append('{}"""'.format(' ' * 4))
        text.append('{}This class contains methods for the \'{}\' webservices'.format(' ' * 4, self.get_category_name(category)))
        text.append('{}Client version: {}'.format(' ' * 4, self.version))
        text.append('{}PATH: {}'.format(' ' * 4, category['path']))
        text.append('{}"""'.format(' ' * 4))
        text.append('')
        text.append('{}def __init__(self, configuration, token=None, login_handler=None, *args, **kwargs):'.format(
            ' ' * 4, self.get_category_name(category), category['path'])
        )
        text.append('{}super({}, self).__init__(configuration, token, login_handler, *args, **kwargs)'.format(
            ' ' * 8, self.categories[self.get_category_name(category)])
        )
        text.append('')
        return '\n'.join(text)

    def get_class_end(self):
        return ''

    def get_method_definition(self, category, endpoint):

        # Getting parameters
        path_params = list(map(lambda x: self.to_snake_case(x), self.get_path_params(endpoint)))
        mandatory_query_params = list(map(lambda x: self.to_snake_case(x), self.get_mandatory_query_params(endpoint)))

        # Getting parameters description
        params_descriptions = []
        for param in self.parameters:
            desc = self.get_parameter_description(param)
            if self.parameters[param]['allowedValues']:
                desc += ' Allowed values: {}'.format(
                    self.get_parameter_allowed_values(param).split(',')
                )
            line = '{}:param {} {}: {}'.format(
                        ' ' * 8,
                        self.param_types[self.get_parameter_type(param)],
                        self.to_snake_case(param),
                        desc
                )
            if self.is_required(param):
                line += ' (REQUIRED)'
                params_descriptions.insert(0, self.format_line(line))
            else:
                params_descriptions.append(self.format_line(line))
        params_descriptions = '\n'.join(params_descriptions)

        # Method arguments
        method_args = ['self']
        method_args += path_params
        method_args += mandatory_query_params
        if endpoint['method'] == 'POST' and 'data' in self.parameters.keys() and 'data' not in mandatory_query_params:
            method_args.append('data=None')
        method_args += ['**options']
        method_args = ', '.join(method_args)

        # Method body
        method_body = ['{}options[\'{}\'] = {}'.format(' ' * 8, self.to_camel_case(required_parameter), required_parameter)
                       for required_parameter in mandatory_query_params if required_parameter != 'data']

        # Call arguments
        call_args = []
        if self.category:
            call_args.append('category=\'{}\''.format(self.category))
        if self.action:
            call_args.append('resource=\'{}\''.format(self.action))
        if self.id1:
            call_args.append('query_id={}'.format(self.to_snake_case(self.id1)))
        if self.subcategory:
            call_args.append('subcategory=\'{}\''.format(self.subcategory))
        if self.id2:
            call_args.append('second_query_id={}'.format(self.to_snake_case(self.id2)))
        if endpoint['method'] == 'POST' and 'data' in self.parameters:
            call_args.append('data=data')
        call_args.append('**options')
        call_args = ', '.join(call_args)

        # Method text
        text = []
        text.append('{}def {}({}):'.format(' ' * 4, self.get_method_name(endpoint, category), method_args))
        text.append('{}"""'.format(' ' * 8))
        text.append(self.format_line('{}{}'.format(' ' * 8, self.get_endpoint_description(endpoint))))
        text.append('{}PATH: {}'.format(' ' * 8, endpoint['path']))
        if params_descriptions:
            text.append('')
            text.append(params_descriptions)
        text.append('{}"""'.format(' ' * 8))
        text.append('')
        if method_body:
            text += method_body
        text.append('{}return self.{}({})'.format(' ' * 8, '_' + endpoint['method'].lower(), call_args))
        text.append('')
        return '\n'.join(text)

    def get_file_name(self, category):
        return self.to_snake_case(self.categories[self.get_category_name(category)]) + '_client.py' \
            if self.categories[self.get_category_name(category)] != 'GA4GH' \
            else self.categories[self.get_category_name(category)].lower() + '_client.py'


def _setup_argparse():
    desc = 'This script creates automatically all Python RestClients files'
    parser = argparse.ArgumentParser(description=desc)

    parser.add_argument('server_url', help='server URL')
    parser.add_argument('output_dir', help='output directory')
    args = parser.parse_args()
    return args


def main():
    # Getting arg parameters
    args = _setup_argparse()

    client_generator = PythonClientGenerator(args.server_url, args.output_dir)
    client_generator.create_rest_clients()


if __name__ == '__main__':
    sys.exit(main())