// Variable set ids were not being stored as long but as integers

migrateCollection("study", {"variableSets" : {$exists: true, $ne: []}}, {variableSets: 1}, function(bulk, doc) {
    if (typeof doc.variableSets[0].id !== "number") {
        // It is already converted
        return;
    }
    var variableSets = [];
    doc.variableSets.forEach(function(variableSet) {
        variableSet.id = NumberLong(variableSet.id);
        variableSets.push(variableSet);
    });
    bulk.find({"_id": doc._id}).updateOne({"$set": {"variableSets": variableSets}});
});

function int2LongAnnotationSets(bulk, doc) {
    if (typeof doc.annotationSets[0].variableSetId !== "number") {
        // It is already converted
        return;
    }
    var annotationSets = [];
    doc.annotationSets.forEach(function(annotationSet) {
        annotationSet.variableSetId = NumberLong(annotationSet.variableSetId);
        annotationSets.push(annotationSet);
    });
    bulk.find({"_id": doc._id}).updateOne({"$set": {"annotationSets": annotationSets}});
}

migrateCollection("sample", {"annotationSets" : {$exists: true, $ne: []}}, {annotationSets: 1}, int2LongAnnotationSets);
migrateCollection("individual", {"annotationSets" : {$exists: true, $ne: []}}, {annotationSets: 1}, int2LongAnnotationSets);
migrateCollection("cohort", {"annotationSets" : {$exists: true, $ne: []}}, {annotationSets: 1}, int2LongAnnotationSets);
migrateCollection("family", {"annotationSets" : {$exists: true, $ne: []}}, {annotationSets: 1}, int2LongAnnotationSets);