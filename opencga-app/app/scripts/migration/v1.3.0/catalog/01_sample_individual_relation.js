// #706

migrateCollection("sample", {"individual.id": {$exists: true}}, {id: 1, _studyId: 1, individual: 1}, function(bulk, doc) {
    if (doc.individual.id > 0) {
        db.individual.update({"_id": doc.individual.id}, {"$addToSet": {"samples": {"id": NumberLong(doc.id)}}})
    }

    bulk.find({"_id": doc._id}).updateOne({"$set": {"individual": {}}})
});