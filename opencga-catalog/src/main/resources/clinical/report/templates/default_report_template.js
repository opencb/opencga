const getTemplateConfig = () => {
    return {
        title: "Hello world",
        sections: [
            {
                title: "Section 1",
                elements: [
                    {
                        title: "Clinical Analysis ID",
                        field: "id",
                    },
                ],
            },
        ],
    };
};

return {
    title: "Example template",
    version: "v1",
    config: getTemplateConfig(),
};