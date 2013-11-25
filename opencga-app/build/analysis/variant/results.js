var RESULT = {
    "variant": {
        "layout": {
            "title": "Job results",
            "children": function () {

                var children = [];
                children.push({title: 'Variant Widget', children: [
                    {
                        "title": 'Variant Widget',
                        "renderers": [
                            {type: 'variant-widget'}
                        ]
                    }
                ]});
                return children;
            },
            "sortOutputItems": function (a, b) {
            }
        }
    }
};
