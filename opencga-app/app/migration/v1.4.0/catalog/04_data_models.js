// #823

migrateCollection("family", {"expectedSize" : { $exists: false } }, {_id: 1}, function(bulk, doc) {
    bulk.find({"_id": doc._id}).updateOne({"$set": {"expectedSize": -1}});
});