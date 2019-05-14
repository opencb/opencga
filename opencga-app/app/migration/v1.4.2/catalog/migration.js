db.clinical.createIndex({"uuid": 1}, {"background": true});
db.clinical.createIndex({"uid": 1}, {"background": true});
db.clinical.createIndex({"id": 1, "studyUid": 1}, {"unique": true, "background": true});
db.clinical.createIndex({"type": 1, "studyUid": 1, "status.name": 1}, {"background": true});
db.clinical.createIndex({"status.name": 1, "studyUid": 1}, {"background": true});
db.clinical.createIndex({"_acl": 1, "studyUid": 1, "status.name": 1}, {"background": true});
db.clinical.createIndex({"_creationDate": 1, "studyUid": 1, "status.name": 1}, {"background": true});
db.clinical.createIndex({"_modificationDate": 1, "studyUid": 1, "status.name": 1}, {"background": true});
db.clinical.createIndex({"dueDate": 1, "studyUid": 1, "status.name": 1}, {"background": true});
db.clinical.createIndex({"priority": 1, "studyUid": 1, "status.name": 1}, {"background": true});
db.clinical.createIndex({"flags": 1, "studyUid": 1, "status.name": 1}, {"background": true});
db.clinical.createIndex({"studyUid": 1}, {"background": true});

db.interpretation.createIndex({"uuid": 1}, {"background": true});
db.interpretation.createIndex({"uid": 1}, {"background": true});
db.interpretation.createIndex({"id": 1, "studyUid": 1}, {"unique": true, "background": true});
db.interpretation.createIndex({"status": 1, "studyUid": 1}, {"background": true});
db.interpretation.createIndex({"_creationDate": 1, "studyUid": 1, "status.name": 1}, {"background": true});
db.interpretation.createIndex({"_modificationDate": 1, "studyUid": 1, "status.name": 1}, {"background": true});
db.interpretation.createIndex({"studyUid": 1}, {"background": true});


// #1268 - Authentication origin
migrateCollection("user", {}, {account: 1}, function(bulk, doc) {
    var changes = {};

    var account = doc.account;
    account["type"] = account.type.toUpperCase();
    account["authentication"] = {
        id: account.authOrigin,
        application: false
    };
    changes['account'] = account;

    bulk.find({"_id": doc._id}).updateOne({"$set": changes});
});