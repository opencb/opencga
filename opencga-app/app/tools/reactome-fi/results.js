/*
 * Copyright 2015 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

var RESULT = {
    "reactome-fi.default": {
        "layout": {
            "title": "Job results",
            "presentation": "",
            "children": function () {
                return [
                    {
                        'title': 'SIF Network',
                        'file': 'mcl_out.sif',
                        "renderers": [
                            {type: 'file'}
                        ]
                    },
                    {
                        'title': 'Node attributes',
                        'file': 'mcl_node_attributes.txt',
                        "renderers": [
                            {type: 'file'}
                        ]
                    },
                    {
                        'title': 'Edge attributes',
                        'file': 'mcl_edge_attributes.txt',
                        "renderers": [
                            {type: 'file'}
                        ]
                    },
                    {
                        'title': 'Summary',
                        'file': 'mcl_summary.json',
                        "renderers": [
                            {
                                type: 'memory-grid',
                                fields: function (data) {
                                    data.header.pop();
                                    return data.header;
                                },
                                data: function (data) {
                                    return data.content
                                }
                            }
                        ]
                    },
                    {
                        'title': '',
                        'file': '',
                        "renderers": [
                            {type: 'note', html: 'Note: MCL modules are available as node attributes'}
                        ]
                    }
                ]
            }
        }
    }
};
