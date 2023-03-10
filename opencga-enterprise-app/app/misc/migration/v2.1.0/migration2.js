load("../utils/migrateCollection.js");

// Check group consistency #1735

migrateCollection("study", {}, {groups: 1}, function(bulk, doc) {
    var groups = doc["groups"];
    var toUpdate = false;
    for (var group of groups) {
        if (isUndefinedOrNull(group['userIds'])) {
            group['userIds'] = [];
            toUpdate = true;
        }
    }

    if (toUpdate) {
        bulk.find({"_id": doc._id}).updateOne({"$set": {"groups": doc['groups']}});
    }
});