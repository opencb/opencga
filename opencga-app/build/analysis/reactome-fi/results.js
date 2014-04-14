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
                        'file':'mcl_edge_attributes.txt',
                        "renderers": [
                            {type: 'file'}
                        ]
                    },
                    {
                        'title': 'SIF Network',
                        'file': 'mcl_summary.txt',
                        "renderers": [
                            {type: 'file'}
                        ]
                    }
                ]
            }
        }
    }
};
