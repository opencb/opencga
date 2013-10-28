var RESULT = {
    "hpg-variant.effect": {
        "layout": {
            "title": "Job results",
            "children": function () {

                var consequenceTypeVariantsTable = {
                    name: "CONSEQUENCE_TYPE_VARIANTS",
                    colNames: ["Chrom", "Position", "Reference", "Alternative", "Feature ID", "Ext Name", "Feature Type", "Biotype", "8", "9", "10", "11", "12", "13", "14", "15", "16", "17", "18", "Consequence Type", "20", "21", "22", "23", "24", "25"],
                    colTypes: ["number", "number", "text", "text", "text", "text", "text", "text", "text", "text", "text", "text", "text", "text", "text", "text", "text", "text", "text", "text", "text", "text", "text", "number", "text", "text"],
                    colVisibility: [1, 1, 1, 1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0],
                    colOrder: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25]
                };

                var children = [];


                var inputFileNameArr = this.job.command.data['vcf-file'].split('/');
                children.push({title: 'Input', children: [
                    {
                        "title": 'VCF input file',
                        "file": inputFileNameArr[inputFileNameArr.length - 1],
                        "renderers": [
                            {type: 'text'}
                        ]
                    }
                ]});

                this.filteredFile;
                this.variantFilterFiles = {};
                /* SUMMARY */
                var filtered;
                var variantsChildren = [];
                var phenotypicChildren = [];
                for (var i = 0, leni = this.outputItems.length; i < leni; i++) {
                    var outItem = this.outputItems[i];
                    if (outItem.indexOf('.filtered', outItem.length - '.filtered'.length) !== -1) {
                        filtered = {
                            "title": 'Filtered Variants',
                            "file": outItem,
                            "renderers": [
                                {type: 'file'}
                            ]
                        };
                        this.filteredFile = outItem;
                    }
                    if (outItem.indexOf('.txt', outItem.length - '.txt'.length) !== -1 &&
                        outItem != 'summary.txt' &&
                        outItem != 'mutation_phenotypes.txt' &&
                        outItem != 'snp_phenotypes.txt' &&
                        outItem != 'all_variants.txt') {
                        var outTitle = outItem.replace('.txt', '');
                        variantsChildren.push({
                            "title": outTitle,
                            "file": outItem,
                            "renderers": [
                                {type: 'file'}
                            ]
                        });
                        this.variantFilterFiles[outItem] = outTitle;
                    }
                    if (outItem == 'mutation_phenotypes.txt' || outItem == 'snp_phenotypes.txt') {
                        phenotypicChildren.push({
                            "title": outItem.replace('.txt', ''),
                            "file": outItem,
                            "renderers": [
                                {type: 'file'}
                            ]
                        });
                    }

                }
                var summaryChildren = [];
                if (filtered) {
                    summaryChildren.push(filtered);
                }
                summaryChildren.push(
                    {
                        "title": 'Genes with Variants',
                        "file": 'genes_with_variants.txt',
                        "renderers": [
                            {type: 'file'}
                        ]
                    });
                summaryChildren.push({
                    "title": 'Consequence types histogram',
                    "file": 'summary.txt',
                    "renderers": [
                        {type: 'file'},
                        {type: 'piechart'}
                    ]
                });
                children.push({title: 'Summary', children: summaryChildren});
                /* SUMMARY */

                children.push({title: 'Variants by Consequence Type', children: variantsChildren});

                phenotypicChildren.push({
                    "title": 'Genomic Context',
                    "renderers": [
                        {type: 'genome-viewer', tableLayout:consequenceTypeVariantsTable}
                    ]
                });

                children.push({title: 'Variants with phenotypic information', children: phenotypicChildren});

                return children;
            },
            sortOutputItems: function (a, b) {
            }
        }
    }
};
