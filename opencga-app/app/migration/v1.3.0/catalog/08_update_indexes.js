// Drop old indexes

db.user.dropIndex({"sessions.id": 1});
db.study.dropIndex({"acl.member": 1});
db.job.dropIndex({"acl.member": 1, "_studyId": 1});
db.file.dropIndex({"acl.member": 1, "_studyId": 1});
db.sample.dropIndex({"name": 1, "_studyId": 1});
db.sample.dropIndex({"acl.member": 1, "_studyId": 1});
db.sample.dropIndex({"ontologyTerms.id": 1});
db.individual.dropIndex({"name": 1, "_studyId": 1});
db.individual.dropIndex({"acl.member": 1, "_studyId": 1});
db.individual.dropIndex({"ontologyTerms.id": 1});
db.cohort.dropIndex({"acl.member": 1, "_studyId": 1});
db.dataset.dropIndex({"acl.member": 1, "_studyId": 1});
db.family.dropIndex({"acl.member": 1, "_studyId": 1});

db.cohort.dropIndex({"_acl": 1});
db.family.dropIndex({"_acl": 1});
db.file.dropIndex({"_acl": 1});
db.individual.dropIndex({"_acl": 1});
db.job.dropIndex({"_acl": 1});
db.sample.dropIndex({"_acl": 1});
db.clinical.dropIndex({"_acl": 1});


// Add new indexes
db.file.createIndex({"id": 1}, {"background": true});
db.clinical.createIndex({"id": 1}, {"background": true});
db.cohort.createIndex({"id": 1}, {"background": true});
db.job.createIndex({"id": 1}, {"background": true});
db.study.createIndex({"id": 1}, {"background": true});

db.cohort.createIndex({"_acl": 1, "_studyId": 1}, {"background": true});
db.family.createIndex({"_acl": 1, "_studyId": 1}, {"background": true});
db.file.createIndex({"_acl": 1, "_studyId": 1}, {"background": true});
db.individual.createIndex({"_acl": 1, "_studyId": 1}, {"background": true});
db.job.createIndex({"_acl": 1, "_studyId": 1}, {"background": true});
db.sample.createIndex({"_acl": 1, "_studyId": 1}, {"background": true});

db.sample.createIndex({"name": 1, "_studyId": 1, "version": 1}, {"unique": true, "background": true});
db.sample.createIndex({"id": 1, "version": 1}, {"unique": true, "background": true});
db.sample.createIndex({"phenotypes.id": 1}, {"background": true});

db.individual.createIndex({"name": 1, "_studyId": 1, "version": 1}, {"unique": true, "background": true});
db.individual.createIndex({"id": 1, "version": 1}, {"unique": true, "background": true});
db.individual.createIndex({"phenotypes.id": 1}, {"background": true});
db.individual.createIndex({"mother.id": 1}, {"background": true});
db.individual.createIndex({"father.id": 1}, {"background": true});

db.family.createIndex({"name": 1, "_studyId": 1, "version": 1}, {"unique": true, "background": true});
db.family.createIndex({"id": 1, "version": 1}, {"unique": true, "background": true});