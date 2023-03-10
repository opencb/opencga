// #722

migrateCollection("file", {$where: "this.samples.length > 1"}, {id: 1, samples: 1}, function(bulk, doc) {
    var samples = [];
    var sampleIds = [];

    for (var i = 0; i < doc.samples.length; i++) {
        if (sampleIds.indexOf(doc.samples[i].id.toString()) === -1) {
            sampleIds.push(doc.samples[i].id.toString());
            samples.push(doc.samples[i]);
        }
    }

    if (samples.length < doc.samples.length) {
        bulk.find({"_id": doc._id}).updateOne({"$set": {"samples": samples}});
    }
});