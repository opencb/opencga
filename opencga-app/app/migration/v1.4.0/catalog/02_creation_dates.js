// #752

function stringToDate(myString) {
    return new ISODate(myString.substring(0, 8) + " " + myString.substring(8, 14));
}

migrateCollection("user", {"projects" : { $exists: true, $ne: [] } }, {projects: 1}, function(bulk, doc) {
    var projects = [];
    for (var i = 0; i < doc.projects.length; i++) {
        var project = doc.projects[i];
        project["_creationDate"] = stringToDate(project.creationDate);
        projects.push(project);
    }

    bulk.find({"_id": doc._id}).updateOne({"$set": {"projects": projects}});
});

migrateCollection("study", {"_creationDate" : { $exists: false } }, {creationDate: 1}, function(bulk, doc) {
    bulk.find({"_id": doc._id}).updateOne({"$set": {"_creationDate": stringToDate(doc.creationDate)}});
});

migrateCollection("sample", {"_creationDate" : { $exists: false } }, {creationDate: 1}, function(bulk, doc) {
    bulk.find({"_id": doc._id}).updateOne({"$set": {"_creationDate": stringToDate(doc.creationDate)}});
});

migrateCollection("file", {"_creationDate" : { $exists: false } }, {creationDate: 1}, function(bulk, doc) {
    bulk.find({"_id": doc._id}).updateOne({"$set": {"_creationDate": stringToDate(doc.creationDate)}});
});

migrateCollection("individual", {"_creationDate" : { $exists: false } }, {creationDate: 1}, function(bulk, doc) {
    bulk.find({"_id": doc._id}).updateOne({"$set": {"_creationDate": stringToDate(doc.creationDate)}});
});

migrateCollection("clinical", {"_creationDate" : { $exists: false } }, {creationDate: 1}, function(bulk, doc) {
    bulk.find({"_id": doc._id}).updateOne({"$set": {"_creationDate": stringToDate(doc.creationDate)}});
});

migrateCollection("cohort", {"_creationDate" : { $exists: false } }, {creationDate: 1}, function(bulk, doc) {
    bulk.find({"_id": doc._id}).updateOne({"$set": {"_creationDate": stringToDate(doc.creationDate)}});
});

migrateCollection("family", {"_creationDate" : { $exists: false } }, {creationDate: 1}, function(bulk, doc) {
    bulk.find({"_id": doc._id}).updateOne({"$set": {"_creationDate": stringToDate(doc.creationDate)}});
});

migrateCollection("job", {"_creationDate" : { $exists: false } }, {creationDate: 1}, function(bulk, doc) {
    bulk.find({"_id": doc._id}).updateOne({"$set": {"_creationDate": stringToDate(doc.creationDate)}});
});
