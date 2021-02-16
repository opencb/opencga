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
        function objectToNumberInt(object) {
            if (typeof object == 'number') {
                return NumberInt(object);
            } else {
                var count = [];
                for (value of object) {
                    count.push(objectToNumberInt(value));
                }
                return count;
            }
        }

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
                annotationset['_c'] = objectToNumberInt(annotationset['_c']);
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

    runUpdate(function () {
        print('Adjusting sample indexes');
        db.sample.createIndex({"studyUid": 1, "_lastOfVersion": 1, "_acl": 1}, {"background": true});
        db.sample.createIndex({"customAnnotationSets.as": 1, "studyUid": 1}, {"background": true});
        db.sample.createIndex({"customAnnotationSets.vs": 1, "studyUid": 1}, {"background": true});
        db.sample.createIndex({"customAnnotationSets.id": 1, "customAnnotationSets.value": 1, "studyUid": 1}, {"background": true});
        db.sample.createIndex({"customInternalAnnotationSets.as": 1, "studyUid": 1}, {"background": true});
        db.sample.createIndex({"customInternalAnnotationSets.vs": 1, "studyUid": 1}, {"background": true});
        db.sample.createIndex({"customInternalAnnotationSets.id": 1, "customInternalAnnotationSets.value": 1, "studyUid": 1}, {"background": true});

        db.sample.dropIndex({"studyUid": 1});
        db.sample.dropIndex({"_lastOfVersion": 1, "studyUid": 1});
        db.sample.dropIndex({"_acl": 1, "studyUid": 1});
        db.sample.dropIndex({"customAnnotationSets.as": 1});
        db.sample.dropIndex({"customAnnotationSets.vs": 1});
        db.sample.dropIndex({"customAnnotationSets.id": 1, "customAnnotationSets.value": 1});
        db.sample.dropIndex({"customInternalAnnotationSets.as": 1});
        db.sample.dropIndex({"customInternalAnnotationSets.vs": 1});
        db.sample.dropIndex({"customInternalAnnotationSets.id": 1, "customInternalAnnotationSets.value": 1});


        print('Adjusting individual indexes...')
        db.individual.createIndex({"studyUid": 1, "_lastOfVersion": 1, "_acl": 1}, {"background": true});
        db.individual.createIndex({"customAnnotationSets.as": 1, "studyUid": 1}, {"background": true});
        db.individual.createIndex({"customAnnotationSets.vs": 1, "studyUid": 1}, {"background": true});
        db.individual.createIndex({"customAnnotationSets.id": 1, "customAnnotationSets.value": 1, "studyUid": 1}, {"background": true});
        db.individual.createIndex({"customInternalAnnotationSets.as": 1, "studyUid": 1}, {"background": true});
        db.individual.createIndex({"customInternalAnnotationSets.vs": 1, "studyUid": 1}, {"background": true});
        db.individual.createIndex({"customInternalAnnotationSets.id": 1, "customInternalAnnotationSets.value": 1, "studyUid": 1}, {"background": true});

        db.individual.dropIndex({"studyUid": 1});
        db.individual.dropIndex({"_lastOfVersion": 1, "studyUid": 1});
        db.individual.dropIndex({"_acl": 1, "studyUid": 1});
        db.individual.dropIndex({"customAnnotationSets.as": 1});
        db.individual.dropIndex({"customAnnotationSets.vs": 1});
        db.individual.dropIndex({"customAnnotationSets.id": 1, "customAnnotationSets.value": 1});
        db.individual.dropIndex({"customInternalAnnotationSets.as": 1});
        db.individual.dropIndex({"customInternalAnnotationSets.vs": 1});
        db.individual.dropIndex({"customInternalAnnotationSets.id": 1, "customInternalAnnotationSets.value": 1});


        print('Adjusting cohort indexes...')
        db.cohort.createIndex({"studyUid": 1, "_acl": 1}, {"background": true});
        db.cohort.createIndex({"customAnnotationSets.as": 1, "studyUid": 1}, {"background": true});
        db.cohort.createIndex({"customAnnotationSets.vs": 1, "studyUid": 1}, {"background": true});
        db.cohort.createIndex({"customAnnotationSets.id": 1, "customAnnotationSets.value": 1, "studyUid": 1}, {"background": true});
        db.cohort.createIndex({"customInternalAnnotationSets.as": 1, "studyUid": 1}, {"background": true});
        db.cohort.createIndex({"customInternalAnnotationSets.vs": 1, "studyUid": 1}, {"background": true});
        db.cohort.createIndex({"customInternalAnnotationSets.id": 1, "customInternalAnnotationSets.value": 1, "studyUid": 1}, {"background": true});

        db.cohort.dropIndex({"studyUid": 1});
        db.cohort.dropIndex({"_acl": 1, "studyUid": 1});
        db.cohort.dropIndex({"customAnnotationSets.as": 1});
        db.cohort.dropIndex({"customAnnotationSets.vs": 1});
        db.cohort.dropIndex({"customAnnotationSets.id": 1, "customAnnotationSets.value": 1});
        db.cohort.dropIndex({"customInternalAnnotationSets.as": 1});
        db.cohort.dropIndex({"customInternalAnnotationSets.vs": 1});
        db.cohort.dropIndex({"customInternalAnnotationSets.id": 1, "customInternalAnnotationSets.value": 1});


        print('Adjusting family indexes...')
        db.family.createIndex({"studyUid": 1, "_lastOfVersion": 1, "_acl": 1}, {"background": true});
        db.family.createIndex({"customAnnotationSets.as": 1, "studyUid": 1}, {"background": true});
        db.family.createIndex({"customAnnotationSets.vs": 1, "studyUid": 1}, {"background": true});
        db.family.createIndex({"customAnnotationSets.id": 1, "customAnnotationSets.value": 1, "studyUid": 1}, {"background": true});
        db.family.createIndex({"customInternalAnnotationSets.as": 1, "studyUid": 1}, {"background": true});
        db.family.createIndex({"customInternalAnnotationSets.vs": 1, "studyUid": 1}, {"background": true});
        db.family.createIndex({"customInternalAnnotationSets.id": 1, "customInternalAnnotationSets.value": 1, "studyUid": 1}, {"background": true});

        db.family.dropIndex({"studyUid": 1});
        db.family.dropIndex({"_lastOfVersion": 1, "studyUid": 1});
        db.family.dropIndex({"_acl": 1, "studyUid": 1});
        db.family.dropIndex({"customAnnotationSets.as": 1});
        db.family.dropIndex({"customAnnotationSets.vs": 1});
        db.family.dropIndex({"customAnnotationSets.id": 1, "customAnnotationSets.value": 1});
        db.family.dropIndex({"customInternalAnnotationSets.as": 1});
        db.family.dropIndex({"customInternalAnnotationSets.vs": 1});
        db.family.dropIndex({"customInternalAnnotationSets.id": 1, "customInternalAnnotationSets.value": 1});


        print('Adjusting file indexes...');
        db.file.createIndex({"studyUid": 1, "_acl": 1}, {"background": true});
        db.file.createIndex({"customAnnotationSets.as": 1, "studyUid": 1}, {"background": true});
        db.file.createIndex({"customAnnotationSets.vs": 1, "studyUid": 1}, {"background": true});
        db.file.createIndex({"customAnnotationSets.id": 1, "customAnnotationSets.value": 1, "studyUid": 1}, {"background": true});
        db.file.createIndex({"customInternalAnnotationSets.as": 1, "studyUid": 1}, {"background": true});
        db.file.createIndex({"customInternalAnnotationSets.vs": 1, "studyUid": 1}, {"background": true});
        db.file.createIndex({"customInternalAnnotationSets.id": 1, "customInternalAnnotationSets.value": 1, "studyUid": 1}, {"background": true});

        db.file.dropIndex({"studyUid": 1});
        db.file.dropIndex({"_acl": 1, "studyUid": 1});
        db.file.dropIndex({"customAnnotationSets.as": 1});
        db.file.dropIndex({"customAnnotationSets.vs": 1});
        db.file.dropIndex({"customAnnotationSets.id": 1, "customAnnotationSets.value": 1});
        db.file.dropIndex({"customInternalAnnotationSets.as": 1});
        db.file.dropIndex({"customInternalAnnotationSets.vs": 1});
        db.file.dropIndex({"customInternalAnnotationSets.id": 1, "customInternalAnnotationSets.value": 1});


        print('Adjusting job indexes...');
        db.job.createIndex({"studyUid": 1, "_acl": 1}, {"background": true});

        db.job.dropIndex({"studyUid": 1});
        db.job.dropIndex({"_acl": 1, "studyUid": 1});


        print('Adjusting panel indexes...');
        db.panel.createIndex({"studyUid": 1, "_lastOfVersion": 1, "_acl": 1}, {"background": true});

        db.panel.dropIndex({"studyUid": 1});
        db.panel.dropIndex({"_lastOfVersion": 1, "studyUid": 1});
        db.panel.dropIndex({"_acl": 1, "studyUid": 1});


        print('Adjusting clinical indexes...');
        db.clinical.createIndex({"studyUid": 1, "_acl": 1}, {"background": true});
        db.clinical.createIndex({"disorder.id": 1, "studyUid": 1}, {"background": true});
        db.clinical.createIndex({"disorder.name": 1, "studyUid": 1}, {"background": true});

        db.clinical.dropIndex({"studyUid": 1});
        db.clinical.dropIndex({"_acl": 1, "studyUid": 1});

        print('Adjusting interpretation indexes...');
        db.panel.createIndex({"studyUid": 1, "_lastOfVersion": 1}, {"background": true});

        db.panel.dropIndex({"studyUid": 1});
    }, "Fix indexes");

    runUpdate(function () {
        function fixPermissionString(aclList) {
            var anyUpdate = false;
            var acls = [];
            for (var acl of aclList) {
                acls.push(acl.replace("UPDATE", "WRITE"));
                if (acl.indexOf("UPDATE") > -1) {
                    anyUpdate = true;
                }
            }
            return {
                'toUpdate': anyUpdate,
                'acl': acls
            }
        }

        function fixPermissions(bulk, doc) {
            var toset = {};
            if (isNotEmptyArray(doc['_acl'])) {
                var result = fixPermissionString(doc['_acl']);
                if (result['toUpdate']) {
                    toset['_acl'] = result['acl'];
                }
            }
            if (isNotEmptyArray(doc['_userAcls'])) {
                var result = fixPermissionString(doc['_userAcls']);
                if (result['toUpdate']) {
                    toset['_userAcls'] = result['acl'];
                }
            }

            if (Object.keys(toset).length > 0) {
                bulk.find({"_id": doc._id}).updateOne({"$set": toset});
            }
        }

        var query = {
            '_acl': {"$exists": true, "$ne": []},
            '_userAcls': {"$exists": true, "$ne": []}
        };

        var projection = {'_acl': 1, '_userAcls': 1};

        migrateCollection("job", query, projection, fixPermissions);
        migrateCollection("sample", query, projection, fixPermissions);
        migrateCollection("individual", query, projection, fixPermissions);
        migrateCollection("family", query, projection, fixPermissions);
        migrateCollection("cohort", query, projection, fixPermissions);
        migrateCollection("panel", query, projection, fixPermissions);
        migrateCollection("clinical", query, projection, fixPermissions);
    }, "Fix Update/Write ACL permissions");

    setOpenCGAVersion("2.0.1", 20001, 1);
}
