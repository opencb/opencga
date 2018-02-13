// #684

var version = {
    version: 1,
    _lastOfVersion: true,
    _releaseFromVersion: [],
    _lastOfRelease: true
};

migrateCollection("sample", {version: {$exists: false}}, {id: 1, _studyId: 1}, function(bulk, doc) {
    bulk.find({"_id": doc._id}).updateOne({"$set": version});
});

migrateCollection("individual", {version: {$exists: false}}, {id: 1, _studyId: 1, samples: 1}, function(bulk, doc) {
    if (typeof doc.samples === "undefined") {
        doc.samples = [];
    }
    for (var i = 0; i < doc.samples.length; i++) {
        doc.samples[i]["version"] = 1;
    }
    var params = {
        samples: doc.samples,
        version: 1,
        _lastOfVersion: true,
        _releaseFromVersion: [],
        _lastOfRelease: true
    };
    bulk.find({"_id": doc._id}).updateOne({"$set": params});
});

migrateCollection("family", {version: {$exists: false}}, {id: 1, _studyId: 1, members: 1}, function(bulk, doc) {
    if (typeof doc.members === "undefined") {
        doc.members = [];
    }
    for (var i = 0; i < doc.members.length; i++) {
        doc.members[i]["version"] = 1;
    }
    var params = {
        members: doc.members,
        version: 1,
        _lastOfVersion: true,
        _releaseFromVersion: [],
        _lastOfRelease: true
    };
    bulk.find({"_id": doc._id}).updateOne({"$set": params});
});

