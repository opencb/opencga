if (versionNeedsUpdate(20100, 1)) {
    runUpdate(function () {
        db.individual.createIndex({"internal.rga.status": 1, "studyUid": 1}, {"background": true});
        db.sample.createIndex({"internal.rga.status": 1, "studyUid": 1}, {"background": true});
    });
}