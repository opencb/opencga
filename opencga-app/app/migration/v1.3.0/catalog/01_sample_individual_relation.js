// #706

migrateCollection("individual", {samples: {$exists: true, $eq: []}}, {id: 1, _studyId: 1}, function(bulk, doc) {

    var samples = [];
    migrateCollection("sample", {"individual.id": doc.id, _studyId: doc._studyId}, {id: 1}, function (sBulk, sDoc) {
       samples.push({
           id: sDoc.id
       });

       sBulk.find({"_id": sDoc._id}).updateOne({"$set": {"individual": {}}})
    });

    bulk.find({"_id": doc._id}).updateOne({"$set": {"samples": samples}});

});