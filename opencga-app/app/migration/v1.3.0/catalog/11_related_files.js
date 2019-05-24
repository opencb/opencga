migrateCollection("file", {"relatedFiles": {"$nin": ["", null, []]}}, {relatedFiles: 1}, function(bulk, doc) {
    var relatedFiles = [];

    for (var i = 0; i < doc.relatedFiles.length; i++) {
        relatedFiles.push({
            "relation": doc.relatedFiles[i].relation,
            "file": {
                "id": doc.relatedFiles[i].fileId,
            }
        });
    }

    bulk.find({"_id": doc._id}).updateOne({"$set": {"relatedFiles": relatedFiles}});
});