// #906
migrateCollection("file", {"_reverse": { $exists: false } }, {name: 1}, function(bulk, doc) {
        bulk.find({"_id": doc._id}).updateOne({"$set": {"_reverse": doc.name.split("").reverse().join("")}});
    }
);

db.file.createIndex({"_reverse": 1, "studyUid": 1, "status.name": 1}, {"background": true});


// #912
db.job.createIndex({"_creationDate": 1, "studyUid": 1, "status.name": 1}, {"background": true});
db.job.createIndex({"_modificationDate": 1, "studyUid": 1, "status.name": 1}, {"background": true});
db.file.createIndex({"_creationDate": 1, "studyUid": 1, "status.name": 1}, {"background": true});
db.file.createIndex({"_modificationDate": 1, "studyUid": 1, "status.name": 1}, {"background": true});
db.sample.createIndex({"_creationDate": 1, "studyUid": 1, "status.name": 1}, {"background": true});
db.sample.createIndex({"_modificationDate": 1, "studyUid": 1, "status.name": 1}, {"background": true});
db.individual.createIndex({"_creationDate": 1, "studyUid": 1, "status.name": 1}, {"background": true});
db.individual.createIndex({"_modificationDate": 1, "studyUid": 1, "status.name": 1}, {"background": true});
db.cohort.createIndex({"_creationDate": 1, "studyUid": 1, "status.name": 1}, {"background": true});
db.cohort.createIndex({"_modificationDate": 1, "studyUid": 1, "status.name": 1}, {"background": true});
db.family.createIndex({"_creationDate": 1, "studyUid": 1, "status.name": 1}, {"background": true});
db.family.createIndex({"_modificationDate": 1, "studyUid": 1, "status.name": 1}, {"background": true});
db.diseasePanel.createIndex({"_creationDate": 1, "studyUid": 1, "status.name": 1}, {"background": true});
db.diseasePanel.createIndex({"_modificationDate": 1, "studyUid": 1, "status.name": 1}, {"background": true});


// # Acls
function haveInternalPermissions(studyId, aclList, userMap) {
    var allUsers = [];
    var usersWithView = [];

    if (!(studyId.valueOf().toString() in userMap)) {
        userMap[studyId.valueOf().toString()] = [];
    }
    var userList = userMap[studyId.valueOf().toString()];

    for (var i in aclList) {
        var split = aclList[i].split("__");
        if (allUsers.indexOf(split[0]) === -1) {
            allUsers.push(split[0]);
        }
        if (split[1] === "VIEW") {
            usersWithView.push(split[0]);
        }
    }

    for (var i in usersWithView) {
        var index = allUsers.indexOf(usersWithView[i]);
        if (index > -1) {
            allUsers.splice(index, 1);
        }
    }

    for (var i in allUsers) {
        var index = userList.indexOf(allUsers[i]);
        if (index === -1) {
            userList.push(allUsers[i]);
        }
    }
}

function updateStudyPermissions(studyUserMap, entity) {
    for (var key in studyUserMap) {
        var userList = studyUserMap[key];
        if (userList.length > 0) {
            for (var i in userList) {
                var myObject = {};
                myObject["_withInternalAcls." + userList[i]] = entity;
                db.study.update({"uid": NumberLong(key)}, {"$addToSet": myObject});
            }
        }
    }
}

print("\nMigrating cohort");
var studyUserMap = {};
migrateCollection("cohort", {"_acl" : { $exists: true, $ne: [] }}, {_acl: 1, studyUid: 1}, function(bulk, doc) {
    haveInternalPermissions(doc.studyUid, doc._acl, studyUserMap);
});
updateStudyPermissions(studyUserMap, "COHORT");

print("\nMigrating family");
studyUserMap = {};
migrateCollection("family", {"_acl" : { $exists: true, $ne: [] }}, {_acl: 1, studyUid: 1}, function(bulk, doc) {
    haveInternalPermissions(doc.studyUid, doc._acl, studyUserMap);
});
updateStudyPermissions(studyUserMap, "FAMILY");

print("\nMigrating file");
studyUserMap = {};
migrateCollection("file", {"_acl" : { $exists: true, $ne: [] }}, {_acl: 1, studyUid: 1}, function(bulk, doc) {
    haveInternalPermissions(doc.studyUid, doc._acl, studyUserMap);
});
updateStudyPermissions(studyUserMap, "FILE");

print("\nMigrating individual");
studyUserMap = {};
migrateCollection("individual", {"_acl" : { $exists: true, $ne: [] }}, {_acl: 1, studyUid: 1}, function(bulk, doc) {
    haveInternalPermissions(doc.studyUid, doc._acl, studyUserMap);
});
updateStudyPermissions(studyUserMap, "INDIVIDUAL");

print("\nMigrating job");
studyUserMap = {};
migrateCollection("job", {"_acl" : { $exists: true, $ne: [] }}, {_acl: 1, studyUid: 1}, function(bulk, doc) {
    haveInternalPermissions(doc.studyUid, doc._acl, studyUserMap);
});
updateStudyPermissions(studyUserMap, "JOB");

print("\nMigrating sample");
studyUserMap = {};
migrateCollection("sample", {"_acl" : { $exists: true, $ne: [] }}, {_acl: 1, studyUid: 1}, function(bulk, doc) {
    haveInternalPermissions(doc.studyUid, doc._acl, studyUserMap);
});
updateStudyPermissions(studyUserMap, "SAMPLE");