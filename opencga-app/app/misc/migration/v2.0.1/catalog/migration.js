if (versionNeedsUpdate(20001, 1)) {
    runUpdate(function () {
        var query = {
            '$or': [
                {
                    'customAnnotationSets': {'$exists': false}
                }, {
                    'customAnnotationSets': null,
                }, {
                    'customInternalAnnotationSets': {'$exists': false}
                }, {
                    'customInternalAnnotationSets': null
                }
            ]
        }

        var projection = { 'customAnnotationSets': 1, 'customInternalAnnotationSets': 1 }

        function initialiseAnnotationSetArrays(bulk, doc) {
            var init = {}
            if (isUndefinedOrNull(doc['customAnnotationSets'])) {
                init['customAnnotationSets'] = [];
            }
            if (isUndefinedOrNull(doc['customInternalAnnotationSets'])) {
                init['customInternalAnnotationSets'] = [];
            }

            bulk.find({"_id": doc._id}).updateOne({"$set": init});
        }

        migrateCollection("file", query, projection, initialiseAnnotationSetArrays);
        migrateCollection("sample", query, projection, initialiseAnnotationSetArrays);
        migrateCollection("individual", query, projection, initialiseAnnotationSetArrays);
        migrateCollection("cohort", query, projection, initialiseAnnotationSetArrays);
        migrateCollection("family", query, projection, initialiseAnnotationSetArrays);
    }, "Ensure AnnotationSet arrays are initialised");

    runUpdate(function () {
        // Ensure all values from _al (array level) and _c (count) are of type NumberInt
        function toNumberInt(annotationset) {
            if (isNotEmptyArray(annotationset['_al'])) {
                var arrayLevel = [];
                for (var value of annotationset['_al']) {
                    arrayLevel.push(NumberInt(value));
                }

                annotationset['_al'] = arrayLevel;
            }

            if (isNotEmptyArray(annotationset['_c'])) {
                var count = [];
                for (var value of annotationset['_c']) {
                    count.push(NumberInt(value));
                }

                annotationset['_c'] = count;
            }
        }

        function checkEntry(bulk, doc) {
            var toset = {}

            if (isNotEmptyArray(doc['customAnnotationSets'])) {
                for (aset of doc['customAnnotationSets']) {
                    toNumberInt(aset);
                }
                toset['customAnnotationSets'] = doc['customAnnotationSets'];
            }

            if (isNotEmptyArray(doc['customInternalAnnotationSets'])) {
                for (aset of doc['customInternalAnnotationSets']) {
                    toNumberInt(aset);
                }
                toset['customInternalAnnotationSets'] = doc['customInternalAnnotationSets'];
            }

            if (Object.keys(toset).length > 0) {
                bulk.find({"_id": doc._id}).updateOne({"$set": toset});
            }
        }

        var query = {
            '$or': [
                {
                    'customAnnotationSets': {'$ne': []}
                }, {
                    'customInternalAnnotationSets': {'$ne': []}
                }
            ]
        }
        var projection = { 'customAnnotationSets': 1, 'customInternalAnnotationSets': 1 }

        migrateCollection("file", query, projection, checkEntry);
        migrateCollection("sample", query, projection, checkEntry);
        migrateCollection("individual", query, projection, checkEntry);
        migrateCollection("cohort", query, projection, checkEntry);
        migrateCollection("family", query, projection, checkEntry);
    }, "Ensure AnnotationSet meta information is properly typed");

    setOpenCGAVersion("2.0.1", 20001, 1);
}
