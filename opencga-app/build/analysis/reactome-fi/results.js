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
