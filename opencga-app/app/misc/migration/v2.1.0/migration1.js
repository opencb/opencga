load("../utils/migrateCollection.js");

// Add Sample RGA status #1693
db.sample.createIndex({"internal.rga.status": 1, "studyUid": 1}, {"background": true});