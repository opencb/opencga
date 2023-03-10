// #718

migrateCollection("sample", {"ontologyTerms": {$exists: true}}, {ontologyTerms: 1}, function(bulk, doc) {
    var params = {
        "$set": {
            "phenotypes": doc.ontologyTerms
        },
        "$unset": {
            "ontologyTerms": ""
        }
    };

    bulk.find({"_id": doc._id}).updateOne(params);
});

migrateCollection("individual", {"ontologyTerms": {$exists: true}}, {ontologyTerms: 1}, function(bulk, doc) {
    var params = {
        "$set": {
            "phenotypes": doc.ontologyTerms
        },
        "$unset": {
            "ontologyTerms": ""
        }
    };

    bulk.find({"_id": doc._id}).updateOne(params);
});

migrateCollection("family", {"diseases": {$exists: true}}, {diseases: 1}, function(bulk, doc) {
    var params = {
        "$set": {
            "phenotypes": doc.diseases
        },
        "$unset": {
            "diseases": ""
        }
    };

    bulk.find({"_id": doc._id}).updateOne(params);
});