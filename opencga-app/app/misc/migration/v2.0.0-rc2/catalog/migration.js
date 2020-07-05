// Remove null values of jobId
db.file.update({"jobId": null}, {"$set": {"jobId": ""}});
db.job.find({}, {id:1, output:1, stdout:1, stderr:1}).forEach(function(job) {
    fileUids = [];
    if (isNotEmptyArray(job.output)) {
        for (file of job.output) {
            fileUids.push(file.uid);
        }
    }
    if (isNotUndefinedOrNull(job.stdout) && isNotUndefinedOrNull(job.stdout.uid)) {
        fileUids.push(job.stdout.uid);
    }
    if (isNotUndefinedOrNull(job.stderr) && isNotUndefinedOrNull(job.stderr.uid)) {
        fileUids.push(job.stderr.uid);
    }

    // Update file documents
    print(job.id + ": " + fileUids);
    db.file.update({"uid": {"$in": fileUids}}, {"$set": {"jobId": "caca"}}, {"multi": true});
});