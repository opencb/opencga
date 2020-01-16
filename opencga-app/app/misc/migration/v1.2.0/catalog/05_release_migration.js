// Add release to all the entries. #616

// Add release to projects
migrateCollection("user", {projects: {$exists: true, $ne: []}}, {projects: 1}, function(bulk, doc) {
    var projects = [];
    var changed = false;
    for (var j = 0; j < doc.projects.length; j++) {
        var project = doc.projects[j];
        if (!project.hasOwnProperty("currentRelease")) {
            project["currentRelease"] = 1;
            changed = true;
        }
        projects.push(project);
    }
    if (changed) {
        bulk.find({"_id": doc._id}).updateOne({$set: {projects: projects}});
    }
});


// Add release to studies, files, samples, cohorts, individuals, jobs, panels,
db.study.update({}, {$set: {release: 1}}, {multi: 1});
db.cohort.update({}, {$set: {release: 1}}, {multi: 1});
db.family.update({}, {$set: {release: 1}}, {multi: 1});
db.file.update({}, {$set: {release: 1}}, {multi: 1});
db.individual.update({}, {$set: {release: 1}}, {multi: 1});
db.job.update({}, {$set: {release: 1}}, {multi: 1});
db.panel.update({}, {$set: {release: 1}}, {multi: 1});
db.sample.update({}, {$set: {release: 1}}, {multi: 1});

// Add release to variable sets
migrateCollection("study", {variableSets: {$exists: true, $ne: []}}, {variableSets: 1}, function(bulk, doc) {
    var variableSets = [];
    var changed = false;
    for (var j = 0; j < doc.variableSets.length; j++) {
        var varSet = doc.variableSets[j];
        if (!varSet.hasOwnProperty("release")) {
            varSet["release"] = 1;
            varSet["confidential"] = false;
            changed = true;
        }
        variableSets.push(varSet);
    }
    if (changed) {
        bulk.find({"_id": doc._id}).updateOne({$set: {variableSets: variableSets}});
    }
});


// Add release to annotation sets
function addReleaseToAnnotationSets(bulk, doc) {
    var annotationSets = [];
    var changed = false;
    for (var j = 0; j < doc.annotationSets.length; j++) {
        var annSet = doc.annotationSets[j];
        if (!annSet.hasOwnProperty("release")) {
            annSet["release"] = 1;
            changed = true;
        }
        annotationSets.push(annSet);
    }
    if (changed) {
        bulk.find({"_id": doc._id}).updateOne({$set: {annotationSets: annotationSets}});
    }
}

migrateCollection("sample", {annotationSets: {$exists: true, $ne: []}}, {annotationSets: 1}, addReleaseToAnnotationSets);
migrateCollection("cohort", {annotationSets: {$exists: true, $ne: []}}, {annotationSets: 1}, addReleaseToAnnotationSets);
migrateCollection("individual", {annotationSets: {$exists: true, $ne: []}}, {annotationSets: 1}, addReleaseToAnnotationSets);
migrateCollection("family", {annotationSets: {$exists: true, $ne: []}}, {annotationSets: 1}, addReleaseToAnnotationSets);
