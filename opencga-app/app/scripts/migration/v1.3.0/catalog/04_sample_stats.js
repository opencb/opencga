// #717

var params = {
    stats: {}
};

migrateCollection("sample", {"stats": {$exists: false}}, {name: 1}, function(bulk, doc) {
    bulk.find({"_id": doc._id}).updateOne({"$set": params});
});