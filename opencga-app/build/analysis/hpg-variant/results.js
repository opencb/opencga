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
                    if (outItem.indexOf('.json', outItem.length - '.json'.length) !== -1 &&
                        outItem != 'mutation_phenotypes.json' &&
                        outItem != 'snp_phenotypes.json' &&
                        outItem != 'all_variants.json') {
                        var outTitle = outItem.replace('.json', '');
                        variantsChildren.push({
                            "title": outTitle,
                            "file": outItem,
                            "renderers": [
                                {type: 'file'}
                            ]
                        });
                        this.variantFilterFiles[outItem] = outTitle;
                    }
                    if (outItem == 'mutation_phenotypes.json' || outItem == 'snp_phenotypes.json') {
                        phenotypicChildren.push({
                            "title": outItem.replace('.json', ''),
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
            "sortOutputItems": function (a, b) {
            }
        }
    },
    "hpg-variant.vcf-stats": {
        "layout": {
            "title": "Job results",
            "children": function () {
                var stats_samples_table = {
                    name: "Stats Samples Table",
                    colNames: ["Sample", "MissGT", "Mendel.Err."],
                    colTypes: ["string", "int", "int"],
                    colVisibility: [1, 1, 1],
                    colOrder: [0, 1, 2]
                };

                var children = [];
                /*  SUMMARY */
                var summaryChildren = [];
                for (var i = 0, leni = this.outputItems.length; i < leni; i++) {
                    var outItem = this.outputItems[i];
                    if (outItem.indexOf('.stats-samples', outItem.length - '.stats-samples'.length) !== -1) {
                        summaryChildren.push(
                            {
                            "title": "Stats Samples",
                            "file": outItem,
                            "renderers": [
                                {type: 'file'},
                                {type: 'grid', tableLayout:stats_samples_table}
                            ]
                        });

                    }else if (outItem.indexOf('.stats-summary', outItem.length - '.stats-summary'.length) !== -1) {
                        summaryChildren.push(
                            {
                            "title": "Stats Summary",
                            "file": outItem,
                            "renderers": [
                                {type: 'file'},
                                {type: 'table'}
                            ]
                        });

                    }else if (outItem.indexOf('.stats-variants', outItem.length - '.stats-variants'.length) !== -1) {
                        summaryChildren.push(
                            {
                            "title": "Stats Variants",
                            "file": outItem,
                            "renderers": [
                                {type: 'file'}
                            ]
                        });
                    }
                }

                children.push({title: 'Summary', children: summaryChildren});
                children.push({title: 'Variant Stats Widget', children: [
                    {
                        "title": 'Variant Stats Widget',
                        "renderers": [
                            {type: 'variant-stats-widget'}
                        ]
                    }
                ]});
                return children;
            },
            "sortOutputItems": function (a, b) {
            }
        }
    },
    "hpg-variant.vcf-merge": {
        "layout": {
            "title": "Job results",
            "children": function () {



                var children = [];
                /*  SUMMARY */
                var summaryChildren = [];
                for (var i = 0, leni = this.outputItems.length; i < leni; i++) {
                    var outItem = this.outputItems[i];
                        summaryChildren.push(
                            {
                            "title": "Merge",
                            "file": outItem,
                            "renderers": [
                                {type: 'file'},
                                {type: 'vcf-grid'}
                            ]
                        });

                }

                children.push({title: 'Summary', children: summaryChildren});
                return children;
            },
            "sortOutputItems": function (a, b) {
            }
        }
    },
    "hpg-variant.vcf-split": {
        "layout": {
            "title": "Job results",
            "children": function () {



                var children = [];
                /*  SUMMARY */
                var summaryChildren = [];
                for (var i = 0, leni = this.outputItems.length; i < leni; i++) {
                    var outItem = this.outputItems[i];
                        summaryChildren.push(
                            {
                            "title": "Split",
                            "file": outItem,
                            "renderers": [
                                {type: 'file'},
                                {type: 'vcf-grid'}
                            ]
                        });

                }

                children.push({title: 'Summary', children: summaryChildren});
                return children;
            },
            "sortOutputItems": function (a, b) {
            }
        }
    },
    "hpg-variant.vcf-annot": {
        "layout": {
            "title": "Job results",
            "children": function () {



                var children = [];
                /*  SUMMARY */
                var summaryChildren = [];
                for (var i = 0, leni = this.outputItems.length; i < leni; i++) {
                    var outItem = this.outputItems[i];
                        summaryChildren.push(
                            {
                            "title": "Annot",
                            "file": outItem,
                            "renderers": [
                                {type: 'file'},
                                {type: 'vcf-grid'}
                            ]
                        });

                }

                children.push({title: 'Summary', children: summaryChildren});
                return children;
            },
            "sortOutputItems": function (a, b) {
            }
        }
    },
    "hpg-variant.vcf-filter": {
        "layout": {
            "title": "Job results",
            "children": function () {



                var children = [];
                /*  SUMMARY */
                var summaryChildren = [];
                for (var i = 0, leni = this.outputItems.length; i < leni; i++) {
                    var outItem = this.outputItems[i];
                        summaryChildren.push(
                            {
                            "title": "Filter",
                            "file": outItem,
                            "renderers": [
                                {type: 'file'},
                                {type: 'vcf-grid'}
                            ]
                        });

                }

                children.push({title: 'Summary', children: summaryChildren});
                return children;
            },
            "sortOutputItems": function (a, b) {
            }
        }
    },
    "hpg-variant.gwas": {
        "layout": {
            "title": "Job results",
            "children": function () {



                var children = [];
                /*  SUMMARY */
                var summaryChildren = [];
                for (var i = 0, leni = this.outputItems.length; i < leni; i++) {
                    var outItem = this.outputItems[i];
                    summaryChildren.push(
                        {
                            "title": "Filter",
                            "file": outItem,
                            "renderers": [
                                {type: 'file'},
                                {type: 'vcf-grid'}
                            ]
                        });

                }

                children.push({title: 'Summary', children: summaryChildren});
                return children;
            },
            "sortOutputItems": function (a, b) {
            }
        }
    }
};
