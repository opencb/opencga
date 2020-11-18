if (versionNeedsUpdate(20000, 5)) {
    runUpdate(function () {
        db.interpretation.createIndex({"clinicalAnalysisId": 1, "studyUid": 1}, {"background": true});
        db.interpretation.createIndex({"status": 1, "studyUid": 1}, {"background": true});
        db.interpretation.createIndex({"primaryFindings.id": 1, "studyUid": 1}, {"background": true});
        db.interpretation.createIndex({"secondaryFindings.id": 1, "studyUid": 1}, {"background": true});

        db.interpretation.createIndex({"uuid": 1, "version": 1}, {"unique": true, "background": true});
        db.interpretation.createIndex({"uid": 1, "version": 1}, {"unique": true, "background": true});
        db.interpretation.createIndex({"id": 1, "version": 1, "studyUid": 1}, {"unique": true, "background": true});
        db.interpretation.dropIndex({"uuid": 1});
        db.interpretation.dropIndex({"uid": 1});
        db.interpretation.dropIndex({"id": 1, "studyUid": 1});
    });

    // # 1668
    runUpdate(function () {
        migrateCollection("clinical", {"qualityControl": {"$exists": false}}, {"_creationDate": 1}, function (bulk, doc) {
            bulk.find({"_id": doc._id}).updateOne({
                "$set": {
                    "qualityControl": {
                        "summary": "UNKNOWN",
                        "comment": "",
                        "user": "",
                        "date": doc._creationDate
                    }
                }
            });
        });
    });

    // # 1673
    runUpdate(function () {

        // The clinical configuration will be autocompleted during migration by Java
        db.study.update({"configuration": {"$exists": false}}, {"$set": {"configuration": {"clinical": {}}}}, {"multi": true});

        migrateCollection("clinical", {"consent.consents": {"$exists": false}}, {
            "creationDate": 1,
            "priority": 1,
            "status": 1
        }, function (bulk, doc) {
            var clinicalParams = {
                "consent": {
                    "consents": [],
                    "date": doc.creationDate
                },
                "priority": {
                    "id": doc.priority,
                    "description": "",
                    "rank": 0,
                    "date": doc.creationDate
                },
                "flags": [],
                "status": {
                    "id": doc.status.name,
                    "description": "",
                    "date": doc.status.date
                }
            };

            bulk.find({"_id": doc._id}).updateOne({"$set": clinicalParams});
        });

        migrateCollection("interpretation", {"status.id": {"$exists": false}}, {"status": 1, "creationDate": 1}, function (bulk, doc) {
            var updateParams = {
                "status": {
                    "id": doc.status,
                    "description": "",
                    "date": doc.creationDate
                }
            };

            bulk.find({"_id": doc._id}).updateOne({"$set": updateParams});
        });

        // Change indexes
        db.clinical.dropIndex("flags_1_studyUid_1");
        db.clinical.dropIndex("priority_1_studyUid_1");
        db.interpretation.dropIndex("status_1_studyUid_1");

        db.clinical.createIndex({"status.id": 1, "studyUid": 1}, {"background": true});
        db.clinical.createIndex({"priority.id": 1, "studyUid": 1}, {"background": true});
        db.clinical.createIndex({"flags.id": 1, "studyUid": 1}, {"background": true});
        db.interpretation.createIndex({"status.id": 1, "studyUid": 1}, {"background": true});
    });

    // # 1674
    runUpdate(function () {
        migrateCollection("study", {"variableSets.internal": {"$exists": false}}, {"variableSets": 1}, function (bulk, doc) {
            if (isNotEmptyArray(doc.variableSets)) {
                var variableSets = [];
                var variableSetUid = undefined;
                for (var variableSet of doc.variableSets) {
                    if (variableSet.id !== "opencga_sample_variant_stats") {
                        variableSet['internal'] = false;
                        variableSets.push(variableSet);
                    } else {
                        variableSetUid = variableSet.uid;
                    }
                }

                if (isNotUndefinedOrNull(variableSetUid)) {
                    // Remove any annotations using the opencga_sample_variant_stats variable set
                    db.sample.update(
                        {"customAnnotationSets.vs": variableSetUid},
                        {"$pull": {"customAnnotationSets": {"vs": variableSetUid}}},
                        {"multi": true});
                }

                bulk.find({"_id": doc._id}).updateOne({"$set": {"variableSets": variableSets}});
            }
        });
    });

    runUpdate(function () {
        migrateCollection("metadata", {}, {}, function (bulk, doc) {
            var latestUpdate = doc._latestUpdate;
            if (isNotUndefinedOrNull(doc._fullVersion) && isNotUndefinedOrNull(doc._fullVersion.latestUpdate)) {
                latestUpdate = doc._fullVersion.latestUpdate;
            }

            bulk.find({"_id": doc._id}).updateOne(
                {
                    "$set":
                        {
                            "_fullVersion": {
                                "version": NumberInt(20000),
                                "release": NumberInt(4),
                                "lastJsUpdate": NumberInt(latestUpdate),
                                "lastJavaUpdate": NumberInt(0)
                            }
                        },
                    "$unset": {"_latestUpdate": ""}
                }
            );
        });
    });

    setOpenCGAVersion("2.0.0", 20000, 5);
}