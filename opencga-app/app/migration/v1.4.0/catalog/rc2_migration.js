// #906
migrateCollection("file", {"_reverse": { $exists: false } }, {name: 1}, function(bulk, doc) {
        bulk.find({"_id": doc._id}).updateOne({"$set": {"_reverse": doc.name.split("").reverse().join("")}});
    }
);

db.file.createIndex({"_reverse": 1, "studyUid": 1, "status.name": 1}, {"background": true});