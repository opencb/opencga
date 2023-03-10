// #711

var deprecatedStudyPermissions = /((VIEW_STUDY)|(UPDATE_STUDY)|(SHARE_STUDY)|(VIEW_VARIABLE_SET)|(WRITE_VARIABLE_SET)|(DELETE_VARIABLE_SET)|(SHARE_FILES)|(SHARE_JOBS)|(SHARE_SAMPLES)|(SHARE_INDIVIDUALS)|(SHARE_FAMILIES)|(SHARE_COHORTS)|(SHARE_DATASETS)|(SHARE_PANELS)|(SHARE_CLINICAL_ANALYSIS))$/;
var deprecatedPermission = /SHARE$/;

function removeAcls(bulk, doc) {

    // Remove deprecated permissions
    var acls = [];
    if (typeof doc._acl !== "undefined") {
        for (var i = 0; i < doc._acl.length; i++) {
            if (doc._acl[i].search(deprecatedPermission) === -1) {
                acls.push(doc._acl[i]);
            }
        }
        if (acls.length !== doc._acl.length) {
            bulk.find({"_id": doc._id}).updateOne({"$set": {"_acl": acls}});
        }
    } else {
        bulk.find({"_id": doc._id}).updateOne({"$set": {"_acl": acls}});
    }

}

migrateCollection("study", {"groups.name": {$ne: "@admins"}}, {groups: 1, _acl: 1}, function(bulk, doc) {
    if (typeof doc.groups === "undefined") {
        doc.groups = [{
            name: "@members",
            userIds: []
        }];
    }
    doc.groups.push({
        name: "@admins",
        userIds: []
    });

    // Remove all permissions
    var acls = [];
    if (typeof doc._acl !== "undefined") {
        for (var i = 0; i < doc._acl.length; i++) {
            if (doc._acl[i].search(deprecatedStudyPermissions) === -1) {
                acls.push(doc._acl[i]);
            }
        }
    }

    var params = {
        groups: doc.groups,
        _acl: acls
    };

    migrateCollection("clinical", {"_studyId": doc._id}, {_acl: 1}, removeAcls);
    migrateCollection("cohort", {"_studyId": doc._id}, {_acl: 1}, removeAcls);
    migrateCollection("family", {"_studyId": doc._id}, {_acl: 1}, removeAcls);
    migrateCollection("file", {"_studyId": doc._id}, {_acl: 1}, removeAcls);
    migrateCollection("individual", {"_studyId": doc._id}, {_acl: 1}, removeAcls);
    migrateCollection("job", {"_studyId": doc._id}, {_acl: 1}, removeAcls);
    migrateCollection("panel", {"_studyId": doc._id}, {_acl: 1}, removeAcls);
    migrateCollection("sample", {"_studyId": doc._id}, {_acl: 1}, removeAcls);

    bulk.find({"_id": doc._id}).updateOne({"$set": params});
});