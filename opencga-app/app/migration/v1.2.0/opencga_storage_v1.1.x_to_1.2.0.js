load("utils/migrateCollection.js")

/**
 * #192 -> Collect genotypes
 * #626 -> Populate alts into stage collection
 **/

if ( db.getCollection("variants").exists() == null ) {
    print("================================================================================");
    print("ERROR: This migration script can be executed on variants databases only");
    print("  Tip: Variants databases contains collections \"variants\", \"stage\", \"files\" and \"studies\"");
    print("================================================================================");
    quit(1);
}

var genotypes = {};

function registerGenotypes(doc) {
    for (var i in doc.studies) {
        var study = doc.studies[i];
        if (study.hasOwnProperty("gt")) {
            var gts = study.gt;
            for (var gt in gts) {
                if (!genotypes.hasOwnProperty(study.sid)) {
                    genotypes[study.sid] = {};
                    genotypes[study.sid][gt] = 1;
                } else {
                    genotypes[study.sid][gt] = 1;
                    //if (!genotypes[study.sid].hasOwnProperty(gt)) {
                    //    genotypes[study.sid][gt] = 1;
                    //} else {
                    //    genotypes[study.sid][gt]++;
                    //}
                }
            }
        }
    }
}

print(" --- Step 1/4 --- ");
migrateCollectionDifferentCollection("variants", "stage", {"studies.alts" : {"$exists" : true} }, {"studies" : 1}, function(bulk, doc) {

    var set = {};
    for (var i in doc.studies) {
        var study = doc.studies[i];
        if (study.hasOwnProperty("alts")) {
            set[study.sid + ".alts"] = study.alts;
        }
    }
    registerGenotypes(doc);

    bulk.find({ "_id" : doc._id}).updateOne({ "$set" : set });
});

print(" --- Step 2/4 --- ");
// Register genotypes from other random variants
db.getCollection("variants").find({},{"studies":1}).limit(10000).forEach(registerGenotypes);


print(" --- Step 3/4 --- ");
// Store loadedGenotypes in the studyConfiguration
for (var studyId in genotypes) {
    var gts = [];
    for (var gt in genotypes[studyId]) {
        gts.push(gt);
    }
    db.studies.update({_id:parseInt(studyId)},{$addToSet:{"attributes.loadedGenotypes":{$each:gts}}})
}

print(" --- Step 4/4 --- ");
// There is no other way to upgrade all the elements from one array.
// Mongo 3.6 will have new operators to modify all the elements from an arrays
db.variants.updateMany({"annotation.ct.exn":{$type:"int"}},{$unset: {
    "annotation.0.ct.0.exn":1,
    "annotation.0.ct.1.exn":1,
    "annotation.0.ct.2.exn":1,
    "annotation.0.ct.3.exn":1,
    "annotation.0.ct.4.exn":1,
    "annotation.0.ct.5.exn":1,
    "annotation.0.ct.6.exn":1,
    "annotation.0.ct.7.exn":1,
    "annotation.0.ct.8.exn":1,
    "annotation.0.ct.9.exn":1,
    "annotation.0.ct.10.exn":1,
    "annotation.0.ct.11.exn":1,
    "annotation.0.ct.12.exn":1,
    "annotation.0.ct.13.exn":1,
    "annotation.0.ct.14.exn":1,
    "annotation.0.ct.15.exn":1,
    "annotation.0.ct.16.exn":1,
    "annotation.0.ct.17.exn":1,
    "annotation.0.ct.18.exn":1,
    "annotation.0.ct.19.exn":1,
    "annotation.0.ct.20.exn":1,
    "annotation.0.ct.21.exn":1,
    "annotation.0.ct.22.exn":1,
    "annotation.0.ct.23.exn":1,
    "annotation.0.ct.24.exn":1,
    "annotation.0.ct.25.exn":1,
    "annotation.0.ct.26.exn":1,
    "annotation.0.ct.27.exn":1,
    "annotation.0.ct.28.exn":1,
    "annotation.0.ct.29.exn":1,
    "annotation.0.ct.30.exn":1,
    "annotation.0.ct.31.exn":1,
    "annotation.0.ct.32.exn":1,
    "annotation.0.ct.33.exn":1,
    "annotation.0.ct.34.exn":1,
    "annotation.0.ct.35.exn":1,
    "annotation.0.ct.36.exn":1,
    "annotation.0.ct.37.exn":1,
    "annotation.0.ct.38.exn":1,
    "annotation.0.ct.39.exn":1,
    "annotation.0.ct.40.exn":1,
    "annotation.0.ct.41.exn":1,
    "annotation.0.ct.42.exn":1,
    "annotation.0.ct.43.exn":1,
    "annotation.0.ct.44.exn":1,
    "annotation.0.ct.45.exn":1,
    "annotation.0.ct.46.exn":1,
    "annotation.0.ct.47.exn":1,
    "annotation.0.ct.48.exn":1,
    "annotation.0.ct.49.exn":1,
    "annotation.0.ct.50.exn":1,
    "annotation.0.ct.51.exn":1,
    "annotation.0.ct.52.exn":1,
    "annotation.0.ct.53.exn":1,
    "annotation.0.ct.54.exn":1,
    "annotation.0.ct.55.exn":1,
    "annotation.0.ct.56.exn":1,
    "annotation.0.ct.57.exn":1,
    "annotation.0.ct.58.exn":1,
    "annotation.0.ct.59.exn":1,
    "annotation.0.ct.60.exn":1,
    "annotation.0.ct.61.exn":1,
    "annotation.0.ct.62.exn":1,
    "annotation.0.ct.63.exn":1,
    "annotation.0.ct.64.exn":1,
    "annotation.0.ct.65.exn":1,
    "annotation.0.ct.66.exn":1,
    "annotation.0.ct.67.exn":1,
    "annotation.0.ct.68.exn":1,
    "annotation.0.ct.69.exn":1,
    "annotation.0.ct.70.exn":1 } })

print("Variants storage database migrated correctly!")
