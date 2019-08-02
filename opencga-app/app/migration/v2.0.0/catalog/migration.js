// #1346

// First we update all the samples so they have a default value
db.getCollection("sample").update({}, {"$set": {"_individualUid": -1, "individualId": ""}}, {"multi": true});

var individuals = db.getCollection("individual").find({"samples" : { "$exists": true, "$ne": [] }}, {"samples": 1, "uid": 1, "id": 1});
for (var i = 0; i < individuals.length(); i++) {
    var individual = individuals[i];
    if (isNotEmptyArray(individual.samples)) {
        for (sample of individual.samples) {
            db.getCollection("sample").update({"uid": sample.uid, "version": sample.version},
                {"$set": {"_individualUid": individual.uid, "individualId": individual.id}},
                {"multi": true});
        }
    }
}