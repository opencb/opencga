migrateCollection("metadata", {id: null}, {}, function(bulk, doc) {
    bulk.find({"_id": doc._id}).updateOne({"$set": {"id": doc._id}});
});