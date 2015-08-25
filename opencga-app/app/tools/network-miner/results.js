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
    "network-miner.default": {
        "layout": {
            "title": "Job results",
            "presentation": "",
            "children": function () {

                var minimalItems = [];
                var el = this.xml.find('output item[name=significant_value]');
                var text = el.text();
                minimalItems.push({
                    'title': el.attr('title'),
                    'file': text,
                    "renderers": [
                        {type: 'text'}
                    ]
                });
                var el = this.xml.find('output item[name=significant_size]');
                var text = el.text();
                minimalItems.push({
                    'title': el.attr('title'),
                    'file': text,
                    "renderers": [
                        {type: 'text'}
                    ]
                });
                minimalItems.push({
                    'title': 'Size vs. Avg. node per component',
                    'file': 'result_size_pvalue.json',
                    "renderers": [
                        {
                            type: 'scatter',
                            x: {
                                title: 'Size',
                                fields: ['size'],
                                field: 'size'
                            },
                            y: {
                                title: 'Average',
                                fields: ['average'],
                                field: 'average'
                            },
                            config: function (item) {
                                var config = {};
                                if (item['significant']) {
                                    config.fill = '#FF0000';
                                    config.radius = 3;
                                }
                                return config;
                            },
                            fields: ['size', 'average', 'significant'],
                            processData: function (data) {
                                var d = [];
                                for (var i = 0; i < data.length; i++) {
                                    var item = data[i];
                                    d.push({
                                        size: item.nodes.length,
                                        average: item.rawValue,
                                        significant: item.significant
                                    })
                                }
                                return d;
                            }
                        }
                    ]
                });
                minimalItems.push({
                    'title': 'Size vs. Score',
                    'file': 'result_size_pvalue.json',
                    "renderers": [
                        {
                            type: 'scatter',
                            x: {
                                title: 'Size',
                                fields: ['size'],
                                field: 'size'
                            },
                            y: {
                                title: 'Score',
                                fields: ['score'],
                                field: 'score'
                            },
                            config: function (item) {
                                var config = {};
                                if (item['significant']) {
                                    config.fill = '#FF0000';
                                    config.radius = 3;
                                }
                                return config;
                            },
                            fields: ['size', 'score', 'significant'],
                            processData: function (data) {
                                var d = [];
                                var ymax = 0;
                                for (var i = 0; i < data.length; i++) {
                                    var item = data[i];
                                    d.push({
                                        size: item.nodes.length,
                                        score: item.score,
                                        significant: item.significant
                                    })
                                    ymax = (item.score > ymax) ? item.score : ymax;
                                }
                                this.y.max = ymax;
                                return d;
                            }
                        }
                    ]
                });

                var inputListItems = [];
                var el = this.xml.find('output item[name=nodes_file_number]');
                var text = el.text();
                inputListItems.push({
                    'title': el.attr('title'),
                    'file': text,
                    "renderers": [
                        {type: 'text'}
                    ]
                });

                var items = [
                    {
                        title: 'Minimum Connected Network selected',
                        children: minimalItems
                    },
                    {
                        title: 'Input list',
                        children: inputListItems
                    }
                ];

                var el = this.xml.find('output item[name=seed_nodes_file_number]');
                if (el.length > 0) {
                    var inputSeedListItems = [];

                    var text = el.text();
                    inputSeedListItems.push({
                        'title': el.attr('title'),
                        'file': text,
                        "renderers": [
                            {type: 'text'}
                        ]
                    });

                    var el = this.xml.find('output item[name=list_info_not_matched_seed_nodes_number]');
                    var text = el.text();
                    inputSeedListItems.push({
                        'title': el.attr('title'),
                        'file': text,
                        "renderers": [
                            {type: 'text'}
                        ]
                    });

                    items.push({
                        title: 'Input seed list',
                        children: inputSeedListItems
                    });
                }

                items.push({
                    'title': '',
                    'file': '',
                    "renderers": [
                        {type: 'note', html:'Note: MCN topological parameters are available as node attributes'}
                    ]
                });

                return items
            },
            oldXML: 'result.xml'
        }
    }
};
