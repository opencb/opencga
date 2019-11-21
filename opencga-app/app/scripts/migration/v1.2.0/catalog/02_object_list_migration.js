// Move list of longs to list of object where needed. #621

function listLong2ListObject(originalList) {
    var newList = [];
    for (var i in originalList) {
        if (originalList[i])
            newList.push({
                "id": NumberLong(originalList[i])
            });
    }
    return newList;
}

// Rename sampleIds for samples in all files and move list<longs> to list<objects>
db.file.update({}, {$rename: {"sampleIds": "samples"}}, {multi: 1});
migrateCollection("file", {"samples" : { $exists: true, $ne: [] } }, {samples: 1}, function(bulk, doc) {
    if (typeof doc.samples[0] !== "number") {
        try {
            doc.samples[0].toNumber(); // If it works, it is because it is a NumberLong, otherwise, it will be already a converted object
        } catch(error) {
            return; // Already changed to object
        }
    }
    var samples = listLong2ListObject(doc.samples);
    bulk.find({"_id": doc._id}).updateOne({"$set": {"samples": samples}});
});

// Move list<long> to list<object> from input and output fields
migrateCollection("job",
    {$or: [{"input" : {$exists: true, $ne: []}}, {"output" : {$exists: true, $ne: []}}]},
    {input: 1, output: 1}, function(bulk, doc) {
        if (doc.input.length > 0) {
            if (typeof doc.input[0] !== "number") {
                try {
                    doc.input[0].toNumber(); // If it works, it is because it is a NumberLong, otherwise, it will be already a converted object
                } catch(error) {
                    return; // Already changed to object
                }
            }
        } else if (doc.output.length > 0) {
            if (typeof doc.input[0] !== "number") {
                try {
                    doc.input[0].toNumber(); // If it works, it is because it is a NumberLong, otherwise, it will be already a converted object
                } catch(error) {
                    return; // Already changed to object
                }
            }
        } else {
            return;
        }
        var input = listLong2ListObject(doc.input);
        var output = listLong2ListObject(doc.output);
        bulk.find({"_id": doc._id}).updateOne({"$set": {"input": input, "output": output}});
});

// Move list<long> to list<object> from samples in cohorts
migrateCollection("cohort", {"samples" : { $exists: true, $ne: [] } }, {samples: 1}, function(bulk, doc) {
    if (typeof doc.samples[0] !== "number") {
        try {
            doc.samples[0].toNumber(); // If it works, it is because it is a NumberLong, otherwise, it will be already a converted object
        } catch(error) {
            return; // Already changed to object
        }
    }
    var samples = listLong2ListObject(doc.samples);
    bulk.find({"_id": doc._id}).updateOne({"$set": {"samples": samples}});
});