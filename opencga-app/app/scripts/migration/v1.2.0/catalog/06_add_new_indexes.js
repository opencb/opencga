db.study.createIndex({"_acl": 1}, {"background": true});
db.cohort.createIndex({"_acl": 1}, {"background": true});
db.family.createIndex({"_acl": 1}, {"background": true});
db.file.createIndex({"_acl": 1}, {"background": true});
db.individual.createIndex({"_acl": 1}, {"background": true});
db.job.createIndex({"_acl": 1}, {"background": true});
db.sample.createIndex({"_acl": 1}, {"background": true});
db.clinical.createIndex({"_acl": 1}, {"background": true});


db.file.dropIndex({"sampleIds": 1});
db.file.createIndex({"samples.id": 1}, {"background": true});