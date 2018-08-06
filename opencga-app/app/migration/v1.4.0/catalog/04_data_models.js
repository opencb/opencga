// #823

db.family.update({"expectedSize": {$exists: false}}, {$set: {"expectedSize": -1}}, {multi: true});
db.file.update({"customAnnotationSets": {$exists: false}}, {$set: {"customAnnotationSets": [], "checksum": ""}}, {multi: true});


// Modification date
migrateCollection("project", {"_modificationDate" : { $exists: false } }, {creationDate: 1}, function(bulk, doc) {
    bulk.find({"_id": doc._id}).updateOne({"$set": {"_modificationDate": stringToDate(doc.creationDate), "modificationDate": doc.creationDate}});
});

migrateCollection("study", {"_modificationDate" : { $exists: false } }, {creationDate: 1}, function(bulk, doc) {
    bulk.find({"_id": doc._id}).updateOne({"$set": {"_modificationDate": stringToDate(doc.creationDate), "modificationDate": doc.creationDate}});
});

migrateCollection("sample", {"_modificationDate" : { $exists: false } }, {creationDate: 1}, function(bulk, doc) {
    bulk.find({"_id": doc._id}).updateOne({"$set": {"_modificationDate": stringToDate(doc.creationDate), "modificationDate": doc.creationDate}});
});

migrateCollection("file", {"_modificationDate" : { $exists: false } }, {creationDate: 1}, function(bulk, doc) {
    bulk.find({"_id": doc._id}).updateOne({"$set": {"_modificationDate": stringToDate(doc.creationDate), "modificationDate": doc.creationDate}});
});

migrateCollection("individual", {"_modificationDate" : { $exists: false } }, {creationDate: 1}, function(bulk, doc) {
    bulk.find({"_id": doc._id}).updateOne({"$set": {"_modificationDate": stringToDate(doc.creationDate), "modificationDate": doc.creationDate}});
});

migrateCollection("clinical", {"_modificationDate" : { $exists: false } }, {creationDate: 1}, function(bulk, doc) {
    bulk.find({"_id": doc._id}).updateOne({"$set": {"_modificationDate": stringToDate(doc.creationDate), "modificationDate": doc.creationDate}});
});

migrateCollection("cohort", {"_modificationDate" : { $exists: false } }, {creationDate: 1}, function(bulk, doc) {
    bulk.find({"_id": doc._id}).updateOne({"$set": {"_modificationDate": stringToDate(doc.creationDate), "modificationDate": doc.creationDate}});
});

migrateCollection("family", {"_modificationDate" : { $exists: false } }, {creationDate: 1}, function(bulk, doc) {
    bulk.find({"_id": doc._id}).updateOne({"$set": {"_modificationDate": stringToDate(doc.creationDate), "modificationDate": doc.creationDate}});
});

migrateCollection("job", {"_modificationDate" : { $exists: false } }, {creationDate: 1}, function(bulk, doc) {
    bulk.find({"_id": doc._id}).updateOne({"$set": {"_modificationDate": stringToDate(doc.creationDate), "modificationDate": doc.creationDate}});
});