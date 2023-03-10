// This script assumes the roleToProband is perfectly defined and filled in. Otherwise, run roleToProband.js before
// #1255

migrateCollection("clinical", {}, {uuid: 1, roleToProband: 1, family: 1}, function(bulk, doc) {
    var roleToProband = {};

    if (isNotUndefinedOrNull(doc.roleToProband) && isNotUndefinedOrNull(doc.family) && isNotEmptyArray(doc.family.members)) {
        for (var i = 0; i < doc.family.members.length; i++) {
            var role = doc.roleToProband[doc.family.members[i].id];

            if (isEmpty(role)) {
                print("Role from member " + doc.family.members[i].id + " not found: " + doc.uuid);
                // return;
            }
            if (isUndefinedOrNull(roleToProband[role])) {
                roleToProband[role] = [];
            }
            roleToProband[role].push(doc.family.members[i]);
        }

        var members = [];
        if (isNotEmptyArray(roleToProband["PROBAND"])) {
            members.push(roleToProband["PROBAND"][0]);
            delete roleToProband["PROBAND"];
        }
        if (isNotEmptyArray(roleToProband["FATHER"])) {
            members.push(roleToProband["FATHER"][0]);
            delete roleToProband["FATHER"];
        }
        if (isNotEmptyArray(roleToProband["MOTHER"])) {
            members.push(roleToProband["MOTHER"][0]);
            delete roleToProband["MOTHER"];
        }
        // Insert the rest
        for (role in roleToProband) {
            for (var i = 0; i < roleToProband[role].length; i++) {
                members.push(roleToProband[role][i]);
            }
        }

        bulk.find({"_id": doc._id}).updateOne({"$set": {"family.members": members}});
    } else {
        print("roleToProband is null or family does not have members: " + doc.uuid);
        return;
    }
});