load("../utils/migrateCollection.js");

// Initialise panels array in Clinical Analysis #1759

migrateCollection("clinical", {"$or":[{panels:{"$exists":false}}, {panels: null}]}, {panels: 1}, function(bulk, doc) {
    bulk.find({"_id": doc._id}).updateOne({"$set": {"panels": []}});
});