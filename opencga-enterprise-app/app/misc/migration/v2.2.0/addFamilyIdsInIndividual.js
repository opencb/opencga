load("../utils/migrateCollection.js");

// Create indexes
db.individual.createIndex({"familyIds": 1, "studyUid": 1}, {"background": true});

migrateCollection("individual", {"familyIds": {"$exists": false}}, {"uid": 1}, function(bulk, doc) {
    var familyIds = [];

    db.family.find({"members.uid": doc.uid}, {"id": 1}).forEach(function (family) {
        familyIds.push(family.id);
    });

    bulk.find({"_id": doc._id}).updateOne({"$set": {"familyIds": familyIds}});
});
