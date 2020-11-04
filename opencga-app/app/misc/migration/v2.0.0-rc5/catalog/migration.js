if (getLatestUpdate() < 1) {
    print("\nStarting migration 1...");
    db.interpretation.createIndex({"clinicalAnalysisId": 1, "studyUid": 1}, {"background": true});
    db.interpretation.createIndex({"status": 1, "studyUid": 1}, {"background": true});
    db.interpretation.createIndex({"primaryFindings.id": 1, "studyUid": 1}, {"background": true});
    db.interpretation.createIndex({"secondaryFindings.id": 1, "studyUid": 1}, {"background": true});

    db.interpretation.createIndex({"uuid": 1, "version": 1}, {"unique": true, "background": true});
    db.interpretation.createIndex({"uid": 1, "version": 1}, {"unique": true, "background": true});
    db.interpretation.createIndex({"id": 1, "version": 1, "studyUid": 1}, {"unique": true, "background": true});
    db.interpretation.dropIndex({"uuid": 1})
    db.interpretation.dropIndex({"uid": 1})
    db.interpretation.dropIndex({"id": 1, "studyUid": 1})
    setLatestUpdate(1);
} else {
    print("\nSkipping migration 1...");
}

// # 1668
if (getLatestUpdate() < 2) {
    print("\nStarting migration 2...");
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
    setLatestUpdate(2);
} else {
    print("\nSkipping migration 2...");
}

// # 1673
if (getLatestUpdate() < 3) {
    print("\nStarting migration 3...");

    // The clinical configuration will be autocompleted during migration by Java
    db.study.update({"configuration": {"$exists": false}}, {"$set": {"configuration": {"clinical": {}}}}, {"multi": true});

    migrateCollection("clinical", {"consent.consents": {"$exists": false}}, {"creationDate": 1, "priority": 1, "status": 1}, function (bulk, doc) {
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
                "description": doc.status.description,
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
    setLatestUpdate(3);
} else {
    print("\nSkipping migration 3...");
}

// setOpenCGAVersion("2.0.0-rc5")