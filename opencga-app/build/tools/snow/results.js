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
    "snow.default": {
        "layout": {
            "title": "Job results",
            "presentation": "",
            "children": function () {
//
                var minimalItems = [];
                var el = this.xml.find('output item[name=components_number]');
                var textArr = el.text().replace(/ /gi, '').split('[');
                var numberComponents = parseFloat(textArr[0]);
                var intervalArr = textArr[1].replace(']','').split(',');
                var intervalFirst =  parseFloat(intervalArr[0]);
                var intervalLast =  parseFloat(intervalArr[1]);
                if(numberComponents <= intervalFirst || numberComponents >= intervalLast){
                    var text = '<span style="color:red">'+numberComponents+'</span>'+' ['+intervalFirst+', '+intervalLast+']';//5 [8, 25]
                }else{
                    var text = '<span style="color:#7BD148">'+numberComponents+'</span>'+' ['+intervalFirst+', '+intervalLast+']';//5 [8, 25]
                }
                minimalItems.push({
                    'title': el.attr('title'),
                    'file': text,
                    "renderers": [
                        {type: 'text'}
                    ]
                });
                var el = this.xml.find('output item[name=sn_random_kol_param_bet]');
                var text = el.text();
                if(parseFloat(text)<= 0.05){
                    text = '<span style="color:red">'+text+'</span>'
                }
                minimalItems.push({
                    'title': el.attr('title'),
                    'file': text,
                    "renderers": [
                        {type: 'text'}
                    ]
                });
                var el = this.xml.find('output item[name=sn_random_kol_param_conn]');
                var text = el.text();
                if(parseFloat(text)<= 0.05){
                    text = '<span style="color:red">'+text+'</span>'
                }
                minimalItems.push({
                    'title': el.attr('title'),
                    'file': text,
                    "renderers": [
                        {type: 'text'}
                    ]
                });
                var el = this.xml.find('output item[name=sn_random_kol_param_clu]');
                var text = el.text();
                if(parseFloat(text)<= 0.05){
                    text = '<span style="color:red">'+text+'</span>'
                }
                minimalItems.push({
                    'title': el.attr('title'),
                    'file': text,
                    "renderers": [
                        {type: 'text'}
                    ]
                });

                minimalItems.push({
                    'title': '',
                    'file': '',
                    "renderers": [
                        {type: 'note', html:'Note: MCN topological parameters are available as node attributes'}
                    ]
                });

                return [
                    {
                        'title': 'Minimal Connected Network topological evaluation',
                        "children": minimalItems
                    },
//                    {
//                        'title': 'Node attributes',
//                        'file': 'mcl_node_attributes.txt',
//                        "renderers": [
//                            {type: 'file'}
//                        ]
//                    },
//                    {
//                        'title': 'Edge attributes',
//                        'file':'mcl_edge_attributes.txt',
//                        "renderers": [
//                            {type: 'file'}
//                        ]
//                    },
//                    {
//                        'title': 'SIF Network',
//                        'file': 'mcl_summary.txt',
//                        "renderers": [
//                            {type: 'file'}
//                        ]
//                    }
                ]
            },
            oldXML: 'result.xml'
        }
    }
};
