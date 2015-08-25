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
    "pathiways": {
        "layout": {
            "title": "Job results",
            "presentation": "tabs",
            "children": function () {
                var children = [];
                var groups = {};
                var summaryTable = {
                    name: "PATHIWAY_SUMMARY_TABLE",
                    colNames: ["Pathway Name", "Mean", "Upper limit", "Lower Limit", "UP/DOWN", "Significance", "SubpathsUP(sign)", "SubpathsDOWN(sign)", "SubpathsNEUTRAL(sign)"],
                    colTypes: ["string", "float", "float", "float", "string", "string", "string", "string", "string"],
                    colVisibility: [1, 1, 1, 1, 1, 1, 1, 1, 1],
                    colOrder: [0, 1, 2, 3, 4, 5, 6, 7, 8]
                };
                var individualTable = {
                    name: "PATHIWAY_INDIVIDUAL_TABLE",
                    colNames: ["Subpathway Name", "Mean", "Upper limit", "Lower Limit", "UP/DOWN", "Significance"],
                    colTypes: ["string", "float", "float", "float", "string", "string"],
                    colVisibility: [1, 1, 1, 1, 1, 1],
                    colOrder: [0, 1, 2, 3, 4, 5]
                };
                var wilcoxonTable = {
                    name: "PATHIWAY_WILCOXON_TABLE",
                    colNames: ["Subpathway Name", "p-value", "FDR p-value", "UP/DOWN"],
                    colTypes: ["string", "string", "string", "string"],
                    colVisibility: [1, 1, 1, 1],
                    colOrder: [0, 1, 2, 3]
                };
                var wilcoxonSumaryTable = {
                    name: "PATHIWAY_WILCOXON_SUMMARY",
                    colNames: ["Pathway Name", "Significant", "SubpathsUP(sign)", "SubpathsDOWN(sign)"],
                    colTypes: ["string", "string", "string", "string"],
                    colVisibility: [1, 1, 1, 1],
                    colOrder: [0, 1, 2, 3]
                };
                var swmFeaturesTable = {
                    name: "SWM_FEATURES_TABLE",
                    colNames: ["Subpathway Name"],
                    colTypes: ["string"],
                    colVisibility: [1],
                    colOrder: [0]
                };
                var pathiwaysProbabilitiesTable = {
                    name: "SWM_FEATURES_TABLE",
                    colNames: ["Subpathway Name"],
                    colTypes: ["string"],
                    colVisibility: [1],
                    colOrder: [0]
                };
                var pathwayDict = {'Summary': 'Summary', 'Normalization': 'Normalization',
                    '03320': 'PPAR SIGNALING PATHWAY',
                    '04010': 'MAPK SIGNALING PATHWAY',
                    '04012': 'ERBB SIGNALING PATHWAY',
                    '04020': 'CALCIUM SIGNALING PATHWAY',
                    '04060': 'CITOKINE-CYTOKINE RECEPTOR INTERACTION',
                    '04062': 'CHEMOKINE SIGNALING PATHWAY',
                    '04080': 'NEUROACTIVE LIGAND-RECEPTOR INTERACTION',
                    '04110': 'CELL CYCLE',
                    '04115': 'p53 SIGNALING PATHWAY',
                    '04150': 'mTOR SIGNALING PATHWAY',
                    '04210': 'APOPTOSIS',
                    '04310': 'WNT SIGNALING PATHWAY',
                    '04330': 'NOTCH SIGNALING PATHWAY',
                    '04340': 'HEDGEHOG SIGNALING PATHWAY',
                    '04350': 'TGF-BETA SIGNALING PATHWAY',
                    '04370': 'VEGF SIGNALING PATHWAY',
                    '04510': 'FOCAL ADHESION',
                    '04512': 'ECM-RECEPTOR INTERACTION',
                    '04514': 'CELL ADHESION MOLECULES',
                    '04520': 'ADHERENS JUNCTION',
                    '04530': 'TIGHT JUNCTION',
                    '04540': 'GAP JUNCTION',
                    '04610': 'COMPLEMENT AND COAGULATION CASCADES',
                    '04612': 'ANTIGEN PROCESING AND PRESENTATION',
                    '04620': 'TOLL-LIKE RECEPTOR SIGNALING PATHWAY',
                    '04630': 'JAK-STAT SIGNALING PATHWAY',
                    '04650': 'NATURAL CELL MEDIATED CYTOTOXICITY',
                    '04660': 'T CELL RECEPTOR SIGNALING PATHWAY',
                    '04662': 'B CELL RECEPTOR SIGNALING PATHWAY',
                    '04664': 'Fc EPSILON RI SIGNALING PATHWAY',
                    '04670': 'LEUKOCYTE TRANSENDOTHELIAL MIGRATION',
                    '04720': 'LONG-TERM POTENTIATION',
                    '04730': 'LONG-TERM DEPRESSION',
                    '04910': 'INSULIN SIGNALING PATHWAY',
                    '04912': 'GnRH SIGNALING PATHWAY',
                    '04916': 'MELANOGENESIS',
                    '04920': 'ADIPOCYTOKINE SIGNALING PATHWAY'
                };
                pathwayNames = Object.keys(pathwayDict);

                for (var i = 0, leni = this.outputItems.length; i < leni; i++) {
                    var outItem = this.outputItems[i];
                    var group = ' ';
                    if (outItem.indexOf('_normmatrix.') != -1) {
                        group = "Normalization";
                    } else if (outItem.indexOf('ALL.txt') != -1 || outItem.indexOf('svm_') != -1 || outItem.indexOf('pathiways_probabilities.txt') != -1) {
                        group = "Summary";
                    } else {
                        for (var p = 0; p < pathwayNames.length; p++) {
                            if (outItem.indexOf(pathwayNames[p]) != -1) {
                                group = pathwayNames[p];
                                break;
                            }
                        }
                    }

                    if (groups[group] == undefined) groups[group] = {title: pathwayDict[group], children: []};

                    var ext = outItem.substring(outItem.lastIndexOf('.'));
                    var item;
                    switch (ext) {
                        case '.png':
                        case '.jpeg':
                            var title = 'Multilevel results';
                            if (outItem.indexOf('PathwayResult') != -1) {
                                title = 'Pathway results';
                            }
                            if (outItem.indexOf('_normmatrix.') != -1) {
                                title = 'Normalization image';
                            }
                            if (outItem.indexOf('_preds.') != -1) {
                                title = 'Predictor results';
                            }
                            item = {
                                "title": title,
                                "file": outItem,
                                "renderers": [
                                    {type: 'file'},
                                    {type: 'image'}
                                ]
                            };
                            break;
                        case '.txt':
                            var tLayout = individualTable;
                            var title = 'Numerical results';
                            if (outItem.indexOf('ALL.txt') != -1) {
                                tLayout = summaryTable;
                            }
                            if (outItem.indexOf('wilcoxonALL.txt') != -1) {
                                tLayout = wilcoxonSumaryTable;
                            }
                            if (outItem.indexOf('wilcoxon.txt') != -1) {
                                tLayout = wilcoxonTable;
                            }
                            if (outItem.indexOf('svm_features.txt') != -1) {
                                title = 'Features';
                                tLayout = swmFeaturesTable;
                            }
                            var renderers = [
                                {type: 'file'},
                                {type: 'grid', tableLayout: tLayout}
                            ];
                            if (outItem.indexOf('_normmatrix.') != -1) {//if norm matrix was generated no table will be drawn
                                title = 'Normalization matrix';
                                renderers = [
                                    {type: 'file'}
                                ];
                            }
                            if (outItem.indexOf('svm_confusion_matrix.txt') != -1) {
                                title = 'Confusion Matrix';
                                renderers = [
                                    {type: 'file'},
                                    {type: 'table'}
                                ];
                            }
                            if (outItem.indexOf('svm_stats.txt') != -1) {
                                title = 'Stats';
                                renderers = [
                                    {type: 'file'},
                                    {type: 'table', header: true}
                                ];
                            }
                            if (outItem.indexOf('pathiways_probabilities.txt') != -1) {
                                title = 'Probabilities';
                                renderers = [
                                    {type: 'file'}
                                ];
                            }
                            item = {
                                "title": title,
                                "file": outItem,
                                "renderers": renderers
                            };
                            break;
                        case '.xgmml':
                            item = {
                                'title': 'Download Cytoscape xgmml file',
                                'file': outItem,
                                "renderers": [
                                    {type: 'file'}
                                ]
                            };
                            break;
                        default :
                            item = {
                                'title': '',
                                'file': outItem,
                                "renderers": [
                                ]
                            };
                            break;
                    }
                    groups[group].children.push(item);
                }


                var checkXgmml = function (p) {
                    for (var i = 0; i < p.children.length; i++) {
                        var file = p.children[i].file;
                        var ext = file.substring(file.lastIndexOf('.'))
                        if (ext === '.xgmml') {
                            return;
                        }
                    }
                    p.children.push({
                        'title': 'There are not significant subpathways',
                        'type': 'danger',
                        "renderers": [
                            {type: 'message'}
                        ]
                    });
                };


                for (var group in groups) {
                    if(group !== 'Summary'){
                        checkXgmml(groups[group]);
                    }
                    children.push(groups[group]);
                }

                return children;
            },
            sortOutputItems: function (a, b) {
                var getWeight = function (name) {
                    var filename = name.toLowerCase();
                    if (filename.indexOf('all.txt') != -1) {
                        return 10;
                    }
                    if (filename.indexOf('txt') != -1) {
                        return 9;
                    }
                    if (filename.indexOf('xgmml') != -1) {
                        return 8;
                    }
                    if (filename.indexOf('png') != -1 || filename.indexOf('jpg') != -1) {
                        return 7;
                    }
                    return 0;
                }
                return (getWeight(b) - getWeight(a));
            }
        }
    },
    "pathipred": {
        "layout": {
            "title": "Job results",
            "presentation": "tabs",
            "children": function () {
                var children = [];
                var groups = {};
                var summaryTable = {
                    name: "PATHIWAY_SUMMARY_TABLE",
                    colNames: ["Pathway Name", "Mean", "Upper limit", "Lower Limit", "UP/DOWN", "Significance", "SubpathsUP(sign)", "SubpathsDOWN(sign)", "SubpathsNEUTRAL(sign)"],
                    colTypes: ["string", "float", "float", "float", "string", "string", "string", "string", "string"],
                    colVisibility: [1, 1, 1, 1, 1, 1, 1, 1, 1],
                    colOrder: [0, 1, 2, 3, 4, 5, 6, 7, 8]
                };
                var individualTable = {
                    name: "PATHIWAY_INDIVIDUAL_TABLE",
                    colNames: ["Subpathway Name", "Mean", "Upper limit", "Lower Limit", "UP/DOWN", "Significance"],
                    colTypes: ["string", "float", "float", "float", "string", "string"],
                    colVisibility: [1, 1, 1, 1, 1, 1],
                    colOrder: [0, 1, 2, 3, 4, 5]
                };
                var wilcoxonTable = {
                    name: "PATHIWAY_WILCOXON_TABLE",
                    colNames: ["Subpathway Name", "p-value", "FDR p-value", "UP/DOWN"],
                    colTypes: ["string", "string", "string", "string"],
                    colVisibility: [1, 1, 1, 1],
                    colOrder: [0, 1, 2, 3]
                };
                var wilcoxonSumaryTable = {
                    name: "PATHIWAY_WILCOXON_SUMMARY",
                    colNames: ["Pathway Name", "Significant", "SubpathsUP(sign)", "SubpathsDOWN(sign)"],
                    colTypes: ["string", "string", "string", "string"],
                    colVisibility: [1, 1, 1, 1],
                    colOrder: [0, 1, 2, 3]
                };
                var swmFeaturesTable = {
                    name: "SWM_FEATURES_TABLE",
                    colNames: ["Subpathway Name"],
                    colTypes: ["string"],
                    colVisibility: [1],
                    colOrder: [0]
                };
                var pathiwaysProbabilitiesTable = {
                    name: "SWM_FEATURES_TABLE",
                    colNames: ["Subpathway Name"],
                    colTypes: ["string"],
                    colVisibility: [1],
                    colOrder: [0]
                };
                var pathwayDict = {'Summary': 'Summary', 'Normalization': 'Normalization',
                    '03320': 'PPAR SIGNALING PATHWAY',
                    '04010': 'MAPK SIGNALING PATHWAY',
                    '04012': 'ERBB SIGNALING PATHWAY',
                    '04020': 'CALCIUM SIGNALING PATHWAY',
                    '04060': 'CITOKINE-CYTOKINE RECEPTOR INTERACTION',
                    '04062': 'CHEMOKINE SIGNALING PATHWAY',
                    '04080': 'NEUROACTIVE LIGAND-RECEPTOR INTERACTION',
                    '04110': 'CELL CYCLE',
                    '04115': 'p53 SIGNALING PATHWAY',
                    '04150': 'mTOR SIGNALING PATHWAY',
                    '04210': 'APOPTOSIS',
                    '04310': 'WNT SIGNALING PATHWAY',
                    '04330': 'NOTCH SIGNALING PATHWAY',
                    '04340': 'HEDGEHOG SIGNALING PATHWAY',
                    '04350': 'TGF-BETA SIGNALING PATHWAY',
                    '04370': 'VEGF SIGNALING PATHWAY',
                    '04510': 'FOCAL ADHESION',
                    '04512': 'ECM-RECEPTOR INTERACTION',
                    '04514': 'CELL ADHESION MOLECULES',
                    '04520': 'ADHERENS JUNCTION',
                    '04530': 'TIGHT JUNCTION',
                    '04540': 'GAP JUNCTION',
                    '04610': 'COMPLEMENT AND COAGULATION CASCADES',
                    '04612': 'ANTIGEN PROCESING AND PRESENTATION',
                    '04620': 'TOLL-LIKE RECEPTOR SIGNALING PATHWAY',
                    '04630': 'JAK-STAT SIGNALING PATHWAY',
                    '04650': 'NATURAL CELL MEDIATED CYTOTOXICITY',
                    '04660': 'T CELL RECEPTOR SIGNALING PATHWAY',
                    '04662': 'B CELL RECEPTOR SIGNALING PATHWAY',
                    '04664': 'Fc EPSILON RI SIGNALING PATHWAY',
                    '04670': 'LEUKOCYTE TRANSENDOTHELIAL MIGRATION',
                    '04720': 'LONG-TERM POTENTIATION',
                    '04730': 'LONG-TERM DEPRESSION',
                    '04910': 'INSULIN SIGNALING PATHWAY',
                    '04912': 'GnRH SIGNALING PATHWAY',
                    '04916': 'MELANOGENESIS',
                    '04920': 'ADIPOCYTOKINE SIGNALING PATHWAY'
                };
                pathwayNames = Object.keys(pathwayDict);

                for (var i = 0, leni = this.outputItems.length; i < leni; i++) {
                    var outItem = this.outputItems[i];
                    var group = ' ';
                    if (outItem.indexOf('_normmatrix.') != -1) {
                        group = "Normalization";
                    } else if (outItem.indexOf('ALL.txt') != -1 || outItem.indexOf('svm_') != -1 || outItem.indexOf('pathiways_probabilities.txt') != -1) {
                        group = "Summary";
                    } else {
                        for (var p = 0; p < pathwayNames.length; p++) {
                            if (outItem.indexOf(pathwayNames[p]) != -1) {
                                group = pathwayNames[p];
                                break;
                            }
                        }
                    }

                    if (groups[group] == undefined) groups[group] = {title: pathwayDict[group], children: []};

                    var ext = outItem.substring(outItem.lastIndexOf('.'));
                    var item;
                    switch (ext) {
                        case '.png':
                        case '.jpeg':
                            var title = 'Multilevel results';
                            if (outItem.indexOf('PathwayResult') != -1) {
                                title = 'Pathway results';
                            }
                            if (outItem.indexOf('_normmatrix.') != -1) {
                                title = 'Normalization image';
                            }
                            if (outItem.indexOf('_preds.') != -1) {
                                title = 'Predictor results';
                            }
                            item = {
                                "title": title,
                                "file": outItem,
                                "renderers": [
                                    {type: 'file'},
                                    {type: 'image'}
                                ]
                            };
                            break;
                        case '.txt':
                            var tLayout = individualTable;
                            var title = 'Numerical results';
                            if (outItem.indexOf('ALL.txt') != -1) {
                                tLayout = summaryTable;
                            }
                            if (outItem.indexOf('wilcoxonALL.txt') != -1) {
                                tLayout = wilcoxonSumaryTable;
                            }
                            if (outItem.indexOf('wilcoxon.txt') != -1) {
                                tLayout = wilcoxonTable;
                            }
                            if (outItem.indexOf('svm_features.txt') != -1) {
                                title = 'Features';
                                tLayout = swmFeaturesTable;
                            }
                            var renderers = [
                                {type: 'file'},
                                {type: 'grid', tableLayout: tLayout}
                            ];
                            if (outItem.indexOf('_normmatrix.') != -1) {//if norm matrix was generated no table will be drawn
                                title = 'Normalization matrix';
                                renderers = [
                                    {type: 'file'}
                                ];
                            }
                            if (outItem.indexOf('svm_confusion_matrix.txt') != -1) {
                                title = 'Confusion Matrix';
                                renderers = [
                                    {type: 'file'},
                                    {type: 'table'}
                                ];
                            }
                            if (outItem.indexOf('svm_stats.txt') != -1) {
                                title = 'Stats';
                                renderers = [
                                    {type: 'file'},
                                    {type: 'table', header: true}
                                ];
                            }
                            if (outItem.indexOf('pathiways_probabilities.txt') != -1) {
                                title = 'Probabilities';
                                renderers = [
                                    {type: 'file'}
                                ];
                            }
                            item = {
                                "title": title,
                                "file": outItem,
                                "renderers": renderers
                            };
                            break;
                        case '.xgmml':
                            item = {
                                'title': 'Download Cytoscape xgmml file',
                                'file': outItem,
                                "renderers": [
                                    {type: 'file'}
                                ]
                            };
                            break;
                        case '.RData':
                            item = {
                                'title': 'SVM Best model',
                                'file': outItem,
                                "renderers": [
                                    {type: 'file'}
                                ]
                            };
                            break;
                        default :
                            item = {
                                'title': '',
                                'file': outItem,
                                "renderers": [
                                ]
                            };
                            break;
                    }
                    groups[group].children.push(item);
                }

                for (var group in groups) {
                    children.push(groups[group]);
                }

                return children;
            },
            sortOutputItems: function (a, b) {
                var getWeight = function (name) {
                    var filename = name.toLowerCase();
                    if (filename.indexOf('all.txt') != -1) {
                        return 10;
                    }
                    if (filename.indexOf('txt') != -1) {
                        return 9;
                    }
                    if (filename.indexOf('xgmml') != -1) {
                        return 8;
                    }
                    if (filename.indexOf('png') != -1 || filename.indexOf('jpg') != -1) {
                        return 7;
                    }
                    return 0;
                }
                return (getWeight(b) - getWeight(a));
            }
        }
    },
    "pathipred-prediction": {
        "layout": {
            "title": "Job results",
//            "presentation": "tabs",
            "children": function () {
                var children = [];

//-rw-r--r-- 1 cafetero cafetero  188 Oct  2 17:08 prediction_stats.txt
//-rw-r--r-- 1 cafetero cafetero 1.2K Oct  2 17:08 prediction.txt
//-rw-r--r-- 1 cafetero cafetero  480 Oct  2 17:08 result.xml
//-rw-r--r-- 1 cafetero cafetero    0 Oct  2 17:08 sge_err.log
//-rw-r--r-- 1 cafetero cafetero 3.9K Oct  2 17:08 sge_out.log
//-rw-r--r-- 1 cafetero cafetero   45 Oct  2 17:08 svm_confusion_matrix.txt

                var predictionStatsTable = {
                    name: "Prediction Stats",
                    colNames: ["Sample", "Prediction"],
                    colTypes: ["string", "String"],
                    colVisibility: [1, 1],
                    colOrder: [0, 1]
                };

                for (var i = 0; i < this.outputItems.length; i++) {
                    var outItem = this.outputItems[i];
                    var outTitle = outItem.replace('.txt', '').replace(/_/, ' ');
                    var renderers = [
                        {type: 'file'}
                    ];

                    if (outItem.indexOf('prediction.txt') != -1) {
                        renderers.push({type: 'grid', tableLayout: predictionStatsTable});
                    }
                    if (outItem.indexOf('svm_confusion_matrix.txt') != -1 || outItem.indexOf('prediction_stats.txt') != -1) {
                        renderers.push({type: 'table'});
                    }

                    children.push({
                        "title": outTitle,
                        "file": outItem,
                        "renderers": renderers
                    });
                }
                return  [
                    {title: 'Results', children: children}
                ];
            }
        }
    }
};
