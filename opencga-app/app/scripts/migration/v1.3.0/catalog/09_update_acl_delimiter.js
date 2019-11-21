// #740

function changeDelimiter(bulk, doc) {
    var permissions = [];

    if (typeof doc._acl !== "undefined") {
        for (var i = 0; i < doc._acl.length; i++) {
            if (isPresent(doc._acl[i], "CONFIDENTIAL")) {
                permissions.push(splitAndModify(doc._acl[i], "CONFIDENTIAL"));
            } else if (isPresent(doc._acl[i], "VIEW")) {
                permissions.push(splitAndModify(doc._acl[i], "VIEW"));
            } else if (isPresent(doc._acl[i], "WRITE")) {
                permissions.push(splitAndModify(doc._acl[i], "WRITE"));
            } else if (isPresent(doc._acl[i], "UPDATE")) {
                permissions.push(splitAndModify(doc._acl[i], "UPDATE"));
            } else if (isPresent(doc._acl[i], "DELETE")) {
                permissions.push(splitAndModify(doc._acl[i], "DELETE"));
            } else if (isPresent(doc._acl[i], "DOWNLOAD")) {
                permissions.push(splitAndModify(doc._acl[i], "DOWNLOAD"));
            } else if (isPresent(doc._acl[i], "UPLOAD")) {
                permissions.push(splitAndModify(doc._acl[i], "UPLOAD"));
            } else if (isPresent(doc._acl[i], "NONE")) {
                permissions.push(splitAndModify(doc._acl[i], "NONE"));
            } else {
                throw "Unexpected permission found " + doc._acl[i];
            }
        }
        if (permissions.length > 0) {
            bulk.find({"_id": doc._id}).updateOne({"$set": {"_acl": permissions}});
        }
    }
}

function isPresent(currentAcl, partialPermission) {
    if (currentAcl.search("__" + partialPermission) !== -1) {
        return false;
    }
    return currentAcl.search("_" + partialPermission) !== -1;
}

function splitAndModify(currentAcl, partialPermission) {
    var splitted = currentAcl.split("_" + partialPermission);
    if (splitted.length !== 2) {
        throw "The number of splitted elements is " + splitted.length + " instead of 2";
    }
    return splitted[0] + "__" + partialPermission + splitted[1];
}

migrateCollection("study", {"_acl" : { $exists: true, $ne: [] }}, {}, changeDelimiter);
migrateCollection("clinical", {"_acl" : { $exists: true, $ne: [] }}, {}, changeDelimiter);
migrateCollection("cohort", {"_acl" : { $exists: true, $ne: [] }}, {}, changeDelimiter);
migrateCollection("dataset", {"_acl" : { $exists: true, $ne: [] }}, {}, changeDelimiter);
migrateCollection("family", {"_acl" : { $exists: true, $ne: [] }}, {}, changeDelimiter);
migrateCollection("file", {"_acl" : { $exists: true, $ne: [] }}, {}, changeDelimiter);
migrateCollection("individual", {"_acl" : { $exists: true, $ne: [] }}, {}, changeDelimiter);
migrateCollection("job", {"_acl" : { $exists: true, $ne: [] }}, {}, changeDelimiter);
migrateCollection("panel", {"_acl" : { $exists: true, $ne: [] }}, {}, changeDelimiter);
migrateCollection("sample", {"_acl" : { $exists: true, $ne: [] }}, {}, changeDelimiter);
