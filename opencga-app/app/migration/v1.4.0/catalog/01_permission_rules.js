// #745

// Add permissionRules map to all studies
db.study.update({"permissionRules": { $exists: false }}, {"$set": {"permissionRules": {}}}, {"multi": true});

// Add private field _permissionRulesApplied
db.sample.update({"_permissionRulesApplied": { $exists: false }}, {"$set": {"_permissionRulesApplied": []}}, {"multi": true});
db.file.update({"_permissionRulesApplied": { $exists: false }}, {"$set": {"_permissionRulesApplied": []}}, {"multi": true});
db.cohort.update({"_permissionRulesApplied": { $exists: false }}, {"$set": {"_permissionRulesApplied": []}}, {"multi": true});
db.individual.update({"_permissionRulesApplied": { $exists: false }}, {"$set": {"_permissionRulesApplied": []}}, {"multi": true});
db.family.update({"_permissionRulesApplied": { $exists: false }}, {"$set": {"_permissionRulesApplied": []}}, {"multi": true});
db.job.update({"_permissionRulesApplied": { $exists: false }}, {"$set": {"_permissionRulesApplied": []}}, {"multi": true});
db.clinical.update({"_permissionRulesApplied": { $exists: false }}, {"$set": {"_permissionRulesApplied": []}}, {"multi": true});

migrateCollection("sample", {"_acl" : { $exists: true, $ne: [] }, "_userAcls": { $exists: false } }, {_acl: 1}, function(bulk, doc) {
    bulk.find({"_id": doc._id}).updateOne({"$set": {"_userAcls": doc._acl}});
});

migrateCollection("file", {"_acl" : { $exists: true, $ne: [] }, "_userAcls": { $exists: false } }, {_acl: 1}, function(bulk, doc) {
    bulk.find({"_id": doc._id}).updateOne({"$set": {"_userAcls": doc._acl}});
});

migrateCollection("cohort", {"_acl" : { $exists: true, $ne: [] }, "_userAcls": { $exists: false } }, {_acl: 1}, function(bulk, doc) {
    bulk.find({"_id": doc._id}).updateOne({"$set": {"_userAcls": doc._acl}});
});

migrateCollection("individual", {"_acl" : { $exists: true, $ne: [] }, "_userAcls": { $exists: false } }, {_acl: 1}, function(bulk, doc) {
    bulk.find({"_id": doc._id}).updateOne({"$set": {"_userAcls": doc._acl}});
});

migrateCollection("family", {"_acl" : { $exists: true, $ne: [] }, "_userAcls": { $exists: false } }, {_acl: 1}, function(bulk, doc) {
    bulk.find({"_id": doc._id}).updateOne({"$set": {"_userAcls": doc._acl}});
});

migrateCollection("job", {"_acl" : { $exists: true, $ne: [] }, "_userAcls": { $exists: false } }, {_acl: 1}, function(bulk, doc) {
    bulk.find({"_id": doc._id}).updateOne({"$set": {"_userAcls": doc._acl}});
});

migrateCollection("clinical", {"_acl" : { $exists: true, $ne: [] }, "_userAcls": { $exists: false } }, {_acl: 1}, function(bulk, doc) {
    bulk.find({"_id": doc._id}).updateOne({"$set": {"_userAcls": doc._acl}});
});