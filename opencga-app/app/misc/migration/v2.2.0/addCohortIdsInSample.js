load("../utils/migrateCollection.js");

// Create indexes
db.sample.createIndex({"cohortIds": 1, "studyUid": 1}, {"background": true});

migrateCollection("sample", {"cohortIds": {"$exists": false}}, {"uid": 1}, function(bulk, doc) {
    var cohortIds = [];

    db.cohort.find({"samples.uid": doc.uid}, {"id": 1}).forEach(function (cohort) {
        cohortIds.push(cohort.id);
    });

    bulk.find({"_id": doc._id}).updateOne({"$set": {"cohortIds": cohortIds}});
});
