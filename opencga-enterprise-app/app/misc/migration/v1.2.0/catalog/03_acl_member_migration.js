// Migration to support tickets #632 and #642
// An array of _acl will be created based on the actual permissions in acl (#632)
// A new @members group will be created containing all the existing users in the studies (#642)

function getUser(user) {
    if (user === "anonymous") {
        return "*";
    } else {
        return user;
    }
}

function buildNewPermissions(aclEntryList) {
    var permissions = [];
    for (var i in aclEntryList) {
        var acl = aclEntryList[i];
        if (acl.permissions.length > 0) {
            for (var j in acl.permissions) {
                permissions.push(getUser(acl.member) + "_" + acl.permissions[j]);
            }
        }
        permissions.push(getUser(acl.member) + "_NONE");
    }
    return permissions;
}

// We will create a new internal group called @members.
migrateCollection("study", {_acl: {$exists: false}}, {acl: 1, groups: 1}, function(bulk, doc) {
    var acl = buildNewPermissions(doc.acl);

    // We will obtain all the users with any permission in the study
    var users = new Set();
    // Look for users in groups
    for (var i in doc.groups) {
        if (doc.groups[i].name === "@members") {
            // TODO: We need to take this group out. At the moment, we will assume that group does not exist.
            throw "A group called @members has been found in study " + doc._id + ". Please, remove that group before running this script"
                + " as that group name is going to be used for internal purposes.";
        }
        for (var j in doc.groups[i].userIds) {
            // Add all the users belonging to the group
            users.add(getUser(doc.groups[i].userIds[j]));
        }
    }
    // Look for users with permissions
    for (var i in doc.acl) {
        if (!doc.acl[i].member.startsWith("@")) {
            users.add(getUser(doc.acl[i].member));
        }
    }

    // Add new group members containing all the users registered in the study
    doc.groups.push({
        "name": "@members",
        "userIds": Array.from(users),
        "syncedFrom": null
    });

    bulk.find({"_id": doc._id}).updateOne({"$set": {"_acl": acl, "groups": doc.groups}});
});

migrateCollection("cohort", {_acl: {$exists: false}}, {acl: 1}, function(bulk, doc) {
    var acl = buildNewPermissions(doc.acl);
    bulk.find({"_id": doc._id}).updateOne({"$set": {"_acl": acl}});
});
migrateCollection("family", {_acl: {$exists: false}}, {acl: 1}, function(bulk, doc) {
    var acl = buildNewPermissions(doc.acl);
    bulk.find({"_id": doc._id}).updateOne({"$set": {"_acl": acl}});
});
migrateCollection("file", {_acl: {$exists: false}}, {acl: 1}, function(bulk, doc) {
    var acl = buildNewPermissions(doc.acl);
    bulk.find({"_id": doc._id}).updateOne({"$set": {"_acl": acl}});
});
migrateCollection("individual", {_acl: {$exists: false}}, {acl: 1}, function(bulk, doc) {
    var acl = buildNewPermissions(doc.acl);
    bulk.find({"_id": doc._id}).updateOne({"$set": {"_acl": acl}});
});
migrateCollection("job", {_acl: {$exists: false}}, {acl: 1}, function(bulk, doc) {
    var acl = buildNewPermissions(doc.acl);
    bulk.find({"_id": doc._id}).updateOne({"$set": {"_acl": acl}});
});
migrateCollection("panel", {_acl: {$exists: false}}, {acl: 1}, function(bulk, doc) {
    var acl = buildNewPermissions(doc.acl);
    bulk.find({"_id": doc._id}).updateOne({"$set": {"_acl": acl}});
});
migrateCollection("sample", {_acl: {$exists: false}}, {acl: 1}, function(bulk, doc) {
    var acl = buildNewPermissions(doc.acl);
    bulk.find({"_id": doc._id}).updateOne({"$set": {"_acl": acl}});
});