if (versionNeedsUpdate(20100, 1)) {
    runUpdate(function () {
        db.sample.createIndex({"internal.rga.status": 1, "studyUid": 1}, {"background": true});
    }, "Add Sample RGA status #1693");

    runUpdate(function () {
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
    }, "Check group consistency #1735");
}