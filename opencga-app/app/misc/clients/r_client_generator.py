import argparse
import sys
import re
import os

from rest_client_generator import RestClientGenerator

class RClientGenerator(RestClientGenerator):

    def __init__(self, server_url, output_dir):
        super().__init__(server_url, output_dir)

        self.categories = {
            'Users': 'User',
            'Projects': 'Project',
            'Studies': 'Study',
            'Files': 'File',
            'Jobs': 'Job',
            'Samples': 'Sample',
            'Individuals': 'Individual',
            'Families': 'Family',
            'Cohorts': 'Cohort',
            'Disease Panels': 'Panel',
            'Analysis - Alignment': 'Alignment',
            'Analysis - Variant': 'Variant',
            'Analysis - Clinical': 'Clinical',
            'Operations - Variant Storage': 'Operation',
            'Meta': 'Meta',
            'GA4GH': 'GA4GH',
            'Admin': 'Admin'
        }



    def get_imports(self):
        return ''
        # headers = []
        # headers.append('/*')
        # headers.append('* Copyright 2015-2020 OpenCB')
        # headers.append('*')
        # headers.append('* Licensed under the Apache License, Version 2.0 (the "License");')
        # headers.append('* you may not use this file except in compliance with the License.')
        # headers.append('* You may obtain a copy of the License at')
        # headers.append('*')
        # headers.append('*     http://www.apache.org/licenses/LICENSE-2.0')
        # headers.append('*')
        # headers.append('* Unless required by applicable law or agreed to in writing, software')
        # headers.append('* distributed under the License is distributed on an "AS IS" BASIS,')
        # headers.append('* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.')
        # headers.append('* See the License for the specific language governing permissions and')
        # headers.append('* limitations under the License.')
        # headers.append('*/')
        # headers.append('')
        # headers.append('package org.opencb.opencga.client.rest;')
        # headers.append('')
        #
        # imports = set()
        # imports.add('org.opencb.opencga.client.exceptions.ClientException;')
        # imports.add('org.opencb.opencga.client.config.ClientConfiguration;')
        # # imports.append('import org.opencb.opencga.client.rest.AbstractParentClient;')
        # imports.add('org.opencb.opencga.core.response.RestResponse;')
        #
        # for java_type in self.java_types:
        #     if java_type in self.type_imports and java_type not in self.ignore_types:
        #         imports.add(self.type_imports[java_type])
        #     elif java_type not in self.ignore_types:
        #         raise Exception(java_type + ' not found')
        #
        # imports = remove_redundant_imports(list(imports))
        # imports.sort()
        #
        # return '\n'.join(headers) + '\n' + '\n'.join(['import ' + i for i in imports])

    def get_class_definition(self, category):
        class_path_params = []
        for endpoints in category["endpoints"]:
            if "parameters" in endpoints:
                for param in endpoints["parameters"]:
                    if param["param"] == "path":
                        class_path_params.append(param["name"])
        path_params = set(class_path_params)

        print (path_params)

        # Create AllGenerics
        allgenerics_file = os.path.join(self.output_dir, get_file_name_allgenerics())
        allgenerics = open(allgenerics_file, 'a')
        text_allgenerics = []
        text_allgenerics.append('#' * 80)
        text_allgenerics.append('## {}Client'.format(self.categories[self.get_category_name(category)]))
        text_allgenerics.append('setGeneric("{}Client", function(OpencgaR, {}action, params=NULL, ...)'.format(
            self.categories[self.get_category_name(category)].lower(),
            ', '.join(path_params) + ', ' if len(path_params) > 0 else ''))
        text_allgenerics.append('{}standardGeneric("{}Client"))'.format(
            ' ' * 4, self.categories[self.get_category_name(category)].lower()))
        allgenerics.write('\n'.join(text_allgenerics) + '\n\n')
        allgenerics.close()

        # Print class description
        text = []
        text.append('#' * 80)
        text.append("#' {}Client methods".format(self.categories[self.get_category_name(category)]))
        text.append("#' @include AllClasses.R")
        text.append("#' @include AllGenerics.R")
        text.append("#' @include commons.R\n")
        text.append("#' @description This function implements the OpenCGA calls for managing {}".format(self.categories[self.get_category_name(category)]))
        text.append("#' @param OpencgaR an object OpencgaR generated using initOpencgaR and/or opencgaLogin")
        text.append("#' @seealso \\url{http://docs.opencb.org/display/opencga/Using+OpenCGA} and the RESTful API documentation")
        text.append("#' \\url{http://bioinfo.hpc.cam.ac.uk/opencga-prod/webservices/}")
        text.append("#' @export\n\n")

        # Print method
        text.append('setMethod("{}Client", "OpencgaR", function(OpencgaR, {}action, params=NULL, ...) {{'.format(
            self.categories[self.get_category_name(category)].lower(),
            ', '.join(path_params) + ', ' if len(path_params) > 0 else ''))
        text.append('{}category <- "{}"'.format(' ' * 4, self.get_category_path(category)))
        text.append('{}switch(action,'.format(' ' * 4))
        return '\n'.join(text)

    def get_class_end(self):
        return ' ' * 4 + ")\n" + \
               "})"

    def get_method_definition(self, category, endpoint):

        print("Processing " + self.get_endpoint_path(endpoint))

        # Getting parameters description
        comments_text = '# Endpoint: {}'.format(endpoint['path'])
        params_descriptions = []
        for param in self.parameters:
            desc = self.get_parameter_description(param)
            if self.parameters[param]['allowedValues']:
                desc += ' Allowed values: {}'.format(
                    self.get_parameter_allowed_values(param).split(','))
            params_descriptions.append('{}# @param {}: {}'.format(' ' * 8, param, desc))
        params_descriptions = '\n'.join(params_descriptions)

        # Get query params
        if len(self.get_mandatory_query_params(endpoint)) > 0:
            query_params = 'c({})'.format(','.join('"{0}"'.format(w) for w in self.get_mandatory_query_params(endpoint)))
        else:
            query_params = 'NULL'

        # Method text
        text = ['{}{}'.format(' ' * 8, comments_text)]
        text.append(params_descriptions)
        append_text(text, '{}{}=fetchOpenCGA(object=OpencgaR, category=category, categoryId={}, subcategory={}, '
                          'subcategoryId={}, action="{}", params=params, httpMethod="{}", as.queryParam={}, ...),'.format(
                   ' ' * 8,
                   self.get_method_name(endpoint, category),
                   self.get_endpoint_id1() if self.get_endpoint_id1() else 'NULL',
                   '"{0}"'.format(self.get_endpoint_subcategory()) if self.get_endpoint_subcategory() else 'NULL',
                   self.get_endpoint_id2() if self.get_endpoint_id2() else 'NULL',
                   self.get_endpoint_action() if self.get_endpoint_action() else 'NULL',
                   self.get_endpoint_method(endpoint),
                   query_params), sep=8)


        # text.append('{}"""'.format(' ' * 8))
        # # text.append(self.format_line('{}{}'.format(' ' * 8, self.get_endpoint_description(endpoint))))
        # text.append('{}PATH: {}'.format(' ' * 8, endpoint['path']))
        # if params_descriptions:
        #     text.append('')
        #     text.append(params_descriptions)
        # text.append('{}"""'.format(' ' * 8))
        # text.append('')
        # if method_body:
        #     text += method_body
        # text.append('{}return self.{}({})'.format(' ' * 8, '_' + endpoint['method'].lower(), call_args))
        # text.append('')
        return '\n'.join(text)

    def get_file_name(self, category):
        return self.categories[self.get_category_name(category)] + "-methods.R"

    def get_method_name(self, endpoint, category):
        method_name = super().get_method_name(endpoint, category)
        # Convert to cammel case
        method_name = method_name.replace('_', ' ').title().replace(' ', '')
        return method_name[0].lower() + method_name[1:]

    def get_method_parameters(self, endpoint):
        parameters = []
        parameters.extend(self.get_path_params(endpoint))
        parameters.extend(self.get_mandatory_query_params(endpoint))

        if 'data' in self.parameters:
            parameters.append('data')

        if self.has_optional_params(endpoint):
            parameters.append('params')

        return parameters

    def get_parameter_description(self, parameter):
        if parameter != 'params':
            return super().get_parameter_description(parameter)
        else:
            return 'Map containing any additional optional parameters.'


def get_file_name_allgenerics():
    return "AllGenerics.R"


def append_text(array, string, sep):
    _append_text(array, string, sep, False)


def append_comment_text(array, string, sep):
    _append_text(array, string, sep, True)


def _append_text(array, string, sep, comment):
    if len(string) <= 120:
        array.append(string)
    else:
        my_string = string
        throw_string = None

        if 'throws' in my_string:
            pos = my_string.find('throws')
            throw_string = my_string[pos:]
            my_string = my_string[:pos - 1]

        while len(my_string) > 120:
            max = 0
            for it in re.finditer(' ', my_string):
                pos = it.start()
                if len(string[:pos]) < 120:
                    max = pos
            array.append(my_string[:max])
            text = ' ' * sep
            if comment:
                text += '*'
            text += ' ' * sep + my_string[max + 1:]
            my_string = text

        if throw_string:
            if len(my_string + ' ' + throw_string) <= 120:
                array.append(my_string + ' ' + throw_string)
            else:
                array.append(my_string)
                array.append(' ' * sep * 3 + throw_string)
        else:
            array.append(my_string)



def _setup_argparse():
    desc = 'This script creates automatically all RestClient files'
    parser = argparse.ArgumentParser(description=desc, formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('server_url', help='server URL')
    parser.add_argument('output_dir', help='output directory')
    args = parser.parse_args()
    return args

def main():
    # Getting arg parameters
    args = _setup_argparse()

    client_generator = RClientGenerator(args.server_url, args.output_dir)
    client_generator.create_rest_clients()


if __name__ == '__main__':
    sys.exit(main())
