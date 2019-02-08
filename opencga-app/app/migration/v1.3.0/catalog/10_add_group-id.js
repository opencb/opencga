
migrateCollection("study", {}, {groups: 1}, function(bulk, doc) {
    if (typeof doc.groups === "undefined") {
        return;
    }

    for (var i = 0; i < doc.groups.length; i++) {
        doc.groups[i]['id'] = doc.groups[i]['name'];
        doc.groups[i]['name'] = doc.groups[i]['name'].substring(1);
    }

    var params = {
        groups: doc.groups
    };

    bulk.find({"_id": doc._id}).updateOne({"$set": params});
});