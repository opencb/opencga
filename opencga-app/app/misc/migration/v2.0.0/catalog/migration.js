// ------------------  Store uuids as actual uuids instead of applying base64 over them
var Entity = Object.freeze({"AUDIT": 0, "PROJECT": 1, "STUDY": 2, "FILE": 3, "SAMPLE": 4, "COHORT": 5, "INDIVIDUAL": 6, "FAMILY": 7,
    "JOB": 8, "CLINICAL": 9, "PANEL": 10, "INTERPRETATION": 11});

function generateOpenCGAUUID(entity, date) {
    var mostSignificantBits = getMostSignificantBits(entity, date);
    var leastSignificantBits = getLeastSignificantBits();
    return _generateUUID(mostSignificantBits, leastSignificantBits);
}

function _generateUUID(mostSigBits, leastSigBits) {
    return (digits(mostSigBits.shiftRightUnsigned(32), 8) + "-" +
        digits(mostSigBits.shiftRightUnsigned(16), 4) + "-" +
        digits(mostSigBits, 4) + "-" +
        digits(leastSigBits.shiftRightUnsigned(48), 4) + "-" +
        digits(leastSigBits, 12));
}

function digits(value, digits) {
    hi = Long.fromNumber(1).shiftLeft(digits * 4).toUnsigned();
    return hi.or(value.and(hi - 1)).toString(16).substring(1);
}

function getMostSignificantBits(entity, date) {
    var time = Long.fromNumber(date.getTime());

    var timeLow = time.and(0xffffffff);
    var timeMid = time.shiftRightUnsigned(32).and(0xffff);

    var uuidVersion = Long.fromNumber(0);
    var internalVersion = Long.fromNumber(0);
    var entityBin = Long.fromNumber(0xff & Entity[entity]);

    return timeLow.shiftLeft(32).toUnsigned().or(timeMid.shiftLeft(16).toUnsigned().or(uuidVersion.shiftLeft(12).toUnsigned()
        .or(internalVersion.shiftLeft(8).toUnsigned().or(entityBin))));
}

function getLeastSignificantBits() {
    var installation = Long.fromNumber(0x1);

    // 12 hex digits random
    var rand = Long.fromNumber(Math.random() * 100000000000000000);
    var randomNumber = rand.and(0xffffffffffff);
    return installation.shiftLeft(48).or(randomNumber);
}

// Custom status template
var customStatus = {
    "name": "",
    "description": "",
    "date" :""
};


// --------------------------------  Migration starts !!!  -----------------------------------------

// --------------------- User - Projects ----------------
print("\nMigrating user-projects...")

// Project map of uid -> uuid so we can easily update project uuids stored in study collection
var projectMap = {};
// Tickets #1528 #1529
migrateCollection("user", {"_password": {"$exists": false}}, {}, function(bulk, doc) {
    // #1531
    doc['status']['description'] = doc['status']['message'];
    delete doc['status']['message']

    var filters = [];
    if (isNotUndefinedOrNull(doc.configs) && isNotEmptyArray(doc.configs.filters)) {
        filters = doc.configs.filters;
    }

    for (var filter of filters) {
        filter['id'] = filter['name'];
        filter['resource'] = filter['bioformat'];

        delete filter['name'];
        delete filter['bioformat'];
    }

    var set = {
        "quota": {
            "diskUsage": doc['size'],
            "cpuUsage": -1,
            "maxDisk": doc['quota'],
            "maxCpu": -1
        },
        "internal": {
            "status": doc['status']
        },
        "_password": doc["password"],
        "filters": filters
    };
    var unset = {
        "password": "",
        "lastModified": "",
        "size": "",
        "account.authOrigin": "",
        "status": "",
        "configs.filters": "",
        "tools": ""
    };

    // #1529
    if (isNotEmptyArray(doc.projects)) {
        for (var i = 0; i < doc.projects.length; i++) {
            var project = doc.projects[i];

            project['uuid'] = generateOpenCGAUUID("PROJECT", project['_creationDate']);
            // Store project map so we can update uuid from study collection
            projectMap[project['uid']] = project['uuid'];

            project['internal'] = {
                'status': project['status'],
                'datastores': project['dataStores']
            };

            // #1531
            project['internal']['status']['description'] = project['internal']['status']['message']
            delete project['internal']['status']['message']

            delete project['lastModified'];
            delete project['size'];
            delete project['organization'];
            delete project['organism']['taxonomyCode'];
            delete project['status'];
            delete project['dataStores'];
        }

        set['projects'] = doc.projects;
    }

    bulk.find({"_id": doc._id}).updateOne({"$set": set, "$unset": unset});
});

// Add new opencga administrator user to user collection #1425
var user = db.getCollection("user").findOne({"id": "opencga"});
if (user === null) {
    var metadata = db.getCollection("metadata").findOne({});
    db.getCollection("user").insert({
        "id": "opencga",
        "name": "opencga",
        "email": metadata["admin"]["email"],
        "_password": metadata["admin"]["password"],
        "organization": "",
        "account": {
            "type": "ADMINISTRATOR",
            "creationDate": metadata["creationDate"],
            "expirationDate": "",
            "authentication": {
                "id": "internal",
                "application": false
            }
        },
        "internal": {
            "status": {
                "name": "READY",
                "date": metadata["creationDate"],
                "description": ""
            }
        },
        "quota": {
            "diskUsage": -1,
            "cpuUsage": -1,
            "maxDisk": -1,
            "maxCpu": -1
        },
        "projects": [],
        "configs": {
        },
        "filters": [],
        "attributes": {}
    });
    // Remove from metadata collection the admin credentials
    db.getCollection("metadata").update({}, {"$unset": {"admin.email": "", "admin.password": ""}})
}


// --------------------- Study ----------------

// Ticket #1479 - TEXT -> STRING
function changeVariableType(variables) {
    for (var i in variables) {
        var variable = variables[i];
        if (variable["type"] === "TEXT") {
            variable["type"] = "STRING";
        }
        if (isNotUndefinedOrNull(variable.variableSet) && variable.variableSet.length > 0) {
            changeVariableType(variable.variableSet);
        }
    }
}

// #1487, #1479, #1532, #1513
print("\nMigrating study...")
migrateCollection("study", {"internal": {"$exists": false}}, {}, function(bulk, doc) {
    doc['status']['description'] = doc['status']['message'];
    delete doc['status']['message'];

    var set = {
        "uuid": generateOpenCGAUUID("STUDY", doc['_creationDate']),
        "_project.uuid": projectMap[doc['_project']['uid']],
        "internal": {
            "status": doc['status']
        },
        "status": customStatus,
        "notification": {
            "webhook": null
        }
    };
    var unset = {
        "stats": "",
        "type": "",
        "datasets": "",
        "experiments": "",
        "lastModified": "",
        "cipher": ""
    };

    // ----------------  #1513  ----------------
    // Remove 'name' field from groups
    if (isNotEmptyArray(doc.groups) && isNotUndefinedOrNull(doc.groups[0].name)) {
        for (var i = 0; i < doc.groups.length; i++) {
            delete doc.groups[i]['name'];
        }
        set["groups"] = doc.groups;
    }
    // !----------------  #1513  ----------------

    // ----------------  #1479  ----------------
    // Check variableSets
    if (isNotUndefinedOrNull(doc.variableSets) && doc.variableSets.length > 0) {
        for (var i in doc.variableSets) {
            changeVariableType(doc.variableSets[i].variables);
        }
        set["variableSets"] = doc.variableSets;
    }
    // !----------------  #1479  ----------------

    // ----------------  #1601  ----------------
    if (isNotEmptyArray(doc._acl)) {
        var acl = [];
        for (var auxAcl of doc._acl) {
            if (auxAcl.endsWith("VIEW_FILE_HEADERS")) {
                acl.push(auxAcl.replace("VIEW_FILE_HEADERS", "VIEW_FILE_HEADER"));
            } else if (auxAcl.endsWith("VIEW_FILE_CONTENTS")) {
                acl.push(auxAcl.replace("VIEW_FILE_CONTENTS", "VIEW_FILE_CONTENT"));
            } else {
                acl.push(auxAcl);
            }
        }
        set["_acl"] = acl;
    }
    // !----------------  #1601  ----------------

    bulk.find({"_id": doc._id}).updateOne({"$set": set, "$unset": unset});
});



// --------------------- File ----------------

// #1533
print("\nMigrating files...")
migrateCollection("file", {"internal": {"$exists": false}}, {}, function(bulk, doc) {
    doc['status']['description'] = doc['status']['message'];
    delete doc['status']['message'];

    var set = {
        "uuid": generateOpenCGAUUID("FILE", doc['_creationDate']),
        "internal": {
            "status": doc['status'],
            "index": doc['index']
        },
        "jobId": "",
        "status": customStatus
    };
    var unset = {
        "index": "",
        "job": "",
        "sampleIds": ""
    };

    if (isUndefinedOrNull(doc.tags)) {
        // We initialise tags array when this is null or not defined
        set["tags"] = [];
    }

    bulk.find({"_id": doc._id}).updateOne({"$set": set, "$unset": unset});
});


// --------------------- Sample ----------------
print("\nMigrating samples...")
// # Add individualIds to samples
var sampleIndividualMap = {};
db.individual.find({"_lastOfVersion": true, "samples" : { "$exists": true, "$ne": [] }}, {"samples": 1, "uid": 1, "id": 1}).forEach(function(doc) {
    doc.samples.forEach(function(sample) {
        var sampleUid = Number(sample['uid']);
        sampleIndividualMap[sampleUid] = {
            "_individualUid": doc.uid,
            "individualId": doc.id
        }
    });
});

// # Add fileIds to samples
var sampleFileMap = {};
db.file.find({samples:{$ne:[]}}, {id:1, samples:1}).forEach(function(doc) {
    doc.samples.forEach(function(sample) {
        var sampleUid = Number(sample['uid']);
        if (!(sampleUid in sampleFileMap)) {
            sampleFileMap[sampleUid] = [];
        }
        sampleFileMap[sampleUid].push(doc.id);
    });
});

// #1540
migrateCollection("sample", {"internal": {"$exists": false}}, {}, function(bulk, doc) {
    doc['status']['description'] = doc['status']['message'];
    delete doc['status']['message'];

    var set = {
        "uuid": generateOpenCGAUUID("SAMPLE", doc['_creationDate']),
        "internal": {
            "status": doc['status']
        },
        "fileIds": [],
        "status": customStatus,
        "_individualUid": -1,
        "individualId": ""
    };
    var unset = {
        "name": "",
        "source": "",
        "stats": "",
        "type": "",
        "individual": ""
    };

    var sampleUid = Number(doc['uid']);
    if (sampleUid in sampleIndividualMap) {
        set["_individualUid"] = sampleIndividualMap[sampleUid]["_individualUid"];
        set["individualId"] = sampleIndividualMap[sampleUid]["individualId"];
    }

    if (sampleUid in sampleFileMap) {
        set["fileIds"] = sampleFileMap[sampleUid];
    }

    bulk.find({"_id": doc._id}).updateOne({"$set": set, "$unset": unset});
});


// --------------------- Cohort ----------------

print("\nMigrating cohorts...")
// #1539
migrateCollection("cohort", {"internal": {"$exists": false}}, {}, function(bulk, doc) {
    doc['status']['description'] = doc['status']['message'];
    delete doc['status']['message'];

    var set = {
        "uuid": generateOpenCGAUUID("COHORT", doc['_creationDate']),
        "internal": {
            "status": doc['status']
        },
        "status": customStatus
    };
    var unset = {
        "name": "",
        "stats": "",
        "family": ""
    };

    bulk.find({"_id": doc._id}).updateOne({"$set": set, "$unset": unset});
});

// --------------------- Individual ----------------
print("\nMigrating individuals...")
// #1535
migrateCollection("individual", {"internal": {"$exists": false}}, {}, function(bulk, doc) {
    doc['status']['description'] = doc['status']['message'];
    delete doc['status']['message'];

    var set = {
        "uuid": generateOpenCGAUUID("INDIVIDUAL", doc['_creationDate']),
        "internal": {
            "status": doc['status']
        },
        "status": customStatus
    };
    var unset = {
        "affectationStatus": "",
        "multiples": "",
        "fatherId": "",
        "motherId": "",
        "family": "",
        "species": "",
    };

    bulk.find({"_id": doc._id}).updateOne({"$set": set, "$unset": unset});
});

// --------------------- Family ----------------
print("\nMigrating families...")
// #1536
migrateCollection("family", {"internal": {"$exists": false}}, {}, function(bulk, doc) {
    doc['status']['description'] = doc['status']['message'];
    delete doc['status']['message'];

    var set = {
        "uuid": generateOpenCGAUUID("FAMILY", doc['_creationDate']),
        "internal": {
            "status": doc['status']
        },
        "status": customStatus
    };

    bulk.find({"_id": doc._id}).updateOne({"$set": set});
});

// --------------------- Job ----------------

// Remove all jobs
db.getCollection("job").remove({});

// --------------------- Panel ----------------

// #1577: Remove all panels
db.getCollection("panel").remove({});

// --------------------- Clinical ----------------
print("\nMigrating clinical...")
// #1537
migrateCollection("clinical", {"internal": {"$exists": false}}, {}, function(bulk, doc) {
    doc['status']['description'] = doc['status']['message'];
    delete doc['status']['message'];

    var set = {
        "uuid": generateOpenCGAUUID("CLINICAL", doc['_creationDate']),
        "internal": {
            "status": doc['status']
        },
        "status": customStatus
    };
    var unset = {
        "name": ""
    };

    bulk.find({"_id": doc._id}).updateOne({"$set": set, "$unset": unset});
});

// --------------------- Interpretation ----------------
print("\nMigrating interpretations...")
migrateCollection("interpretation", {"internal": {"$exists": false}}, {}, function(bulk, doc) {
    var set = {
        "uuid": generateOpenCGAUUID("INTERPRETATION", doc['_creationDate']),
        "internal": {
            "status": {
                "name": doc['status'],
                "description": "",
                "date": ""
            }
        }
    };

    bulk.find({"_id": doc._id}).updateOne({"$set": set});
});

// --------------------- Dataset ----------------

// Drop dataset collection
db.getCollection("dataset").drop()


print("\nCreating user indexes...")
db.user.createIndex({"id": 1}, {"unique": true, "background": true});
db.user.createIndex({"projects.uid": 1}, {"unique": true, "background": true});
db.user.createIndex({"projects.uuid": 1}, {"unique": true, "background": true});
db.user.createIndex({"projects.fqn": 1}, {"unique": true, "background": true});
db.user.createIndex({"projects.id": 1, "id": 1}, {"unique": true, "background": true});

print("\nCreating study indexes...")
db.study.createIndex({"uid": 1}, {"unique": true, "background": true});
db.study.createIndex({"uuid": 1}, {"unique": true, "background": true});
db.study.createIndex({"fqn": 1}, {"unique": true, "background": true});
db.study.createIndex({"id": 1, "_project.uid": 1}, {"unique": true, "background": true});
db.study.createIndex({"groups.id": 1, "uid": 1}, {"unique": true, "background": true});
db.study.createIndex({"groups.userIds": 1, "uid": 1}, {"unique": true, "background": true});
db.study.createIndex({"_acl": 1}, {"background": true});
db.study.createIndex({"_project.uid": 1}, {"background": true});
db.study.createIndex({"variableSets.id": 1, "uid": 1}, {"unique": true, "background": true});

print("\nCreating job indexes...")
db.job.createIndex({"uuid": 1}, {"unique": true, "background": true});
db.job.createIndex({"uid": 1}, {"unique": true, "background": true});
db.job.createIndex({"id": 1, "studyUid": 1}, {"unique": true, "background": true});
db.job.createIndex({"studyUid": 1}, {"background": true});
db.job.createIndex({"tool.id": 1, "studyUid": 1}, {"background": true});
db.job.createIndex({"tool.type": 1, "studyUid": 1}, {"background": true});
db.job.createIndex({"userId": 1, "studyUid": 1}, {"background": true});
db.job.createIndex({"input.uid": 1, "studyUid": 1}, {"background": true});
db.job.createIndex({"output.uid": 1, "studyUid": 1}, {"background": true});
db.job.createIndex({"tags": 1, "studyUid": 1}, {"background": true});
db.job.createIndex({"visited": 1, "studyUid": 1}, {"background": true});
db.job.createIndex({"internal.status.name": 1, "studyUid": 1}, {"background": true});
db.job.createIndex({"_priority": 1, "_creationDate": 1, "studyUid": 1}, {"background": true});
db.job.createIndex({"_priority": 1, "studyUid": 1}, {"background": true});
db.job.createIndex({"_creationDate": 1, "studyUid": 1}, {"background": true});
db.job.createIndex({"_modificationDate": 1, "studyUid": 1}, {"background": true});
db.job.createIndex({"_acl": 1, "studyUid": 1}, {"background": true});

print("\nCreating file indexes...")
db.file.createIndex({"uuid": 1}, {"unique": true, "background": true});
db.file.createIndex({"uid": 1}, {"unique": true, "background": true});
db.file.createIndex({"id": 1, "studyUid": 1}, {"unique": true, "background": true});
db.file.createIndex({"path": 1, "studyUid": 1}, {"unique": true, "background": true});
db.file.createIndex({"name": 1, "studyUid": 1}, {"background": true});
db.file.createIndex({"studyUid": 1}, {"background": true});
db.file.createIndex({"_reverse": 1, "studyUid": 1}, {"background": true});
db.file.createIndex({"type": 1, "studyUid": 1}, {"background": true});
db.file.createIndex({"format": 1, "studyUid": 1}, {"background": true});
db.file.createIndex({"bioformat": 1, "studyUid": 1}, {"background": true});
db.file.createIndex({"uri": 1, "studyUid": 1}, {"background": true});
db.file.createIndex({"tags": 1, "studyUid": 1}, {"background": true});
db.file.createIndex({"samples.uid": 1, "studyUid": 1}, {"background": true});
db.file.createIndex({"_creationDate": 1, "studyUid": 1}, {"background": true});
db.file.createIndex({"_modificationDate": 1, "studyUid": 1}, {"background": true});
db.file.createIndex({"jobId": 1, "studyUid": 1}, {"background": true});
db.file.createIndex({"internal.status.name": 1, "studyUid": 1}, {"background": true});
db.file.createIndex({"_acl": 1, "studyUid": 1}, {"background": true});
db.file.createIndex({"customAnnotationSets.as": 1}, {"background": true});
db.file.createIndex({"customAnnotationSets.vs": 1}, {"background": true});
db.file.createIndex({"customAnnotationSets.id": 1, "customAnnotationSets.value": 1}, {"background": true});

print("\nCreating sample indexes...")
db.sample.createIndex({"uuid": 1, "version": 1}, {"unique": true, "background": true});
db.sample.createIndex({"uid": 1, "version": 1}, {"unique": true, "background": true});
db.sample.createIndex({"id": 1, "studyUid": 1, "version": 1}, {"unique": true, "background": true});
db.sample.createIndex({"phenotypes.id": 1, "studyUid": 1}, {"background": true});
db.sample.createIndex({"fileIds": 1, "studyUid": 1}, {"background": true});
db.sample.createIndex({"individualId": 1, "studyUid": 1}, {"background": true});
db.sample.createIndex({"_creationDate": 1, "studyUid": 1}, {"background": true});
db.sample.createIndex({"_modificationDate": 1, "studyUid": 1}, {"background": true});
db.sample.createIndex({"studyUid": 1}, {"background": true});
db.sample.createIndex({"processing.product": 1, "studyUid": 1}, {"background": true});
db.sample.createIndex({"processing.preparationMethod": 1, "studyUid": 1}, {"background": true});
db.sample.createIndex({"processing.extractionMethod": 1, "studyUid": 1}, {"background": true});
db.sample.createIndex({"processing.labSampleId": 1, "studyUid": 1}, {"background": true});
db.sample.createIndex({"collection.tissue": 1, "studyUid": 1}, {"background": true});
db.sample.createIndex({"collection.organ": 1, "studyUid": 1}, {"background": true});
db.sample.createIndex({"collection.method": 1, "studyUid": 1}, {"background": true});
db.sample.createIndex({"_lastOfVersion": 1, "studyUid": 1}, {"background": true});
db.sample.createIndex({"_acl": 1, "studyUid": 1}, {"background": true});
db.sample.createIndex({"customAnnotationSets.as": 1}, {"background": true});
db.sample.createIndex({"customAnnotationSets.vs": 1}, {"background": true});
db.sample.createIndex({"customAnnotationSets.id": 1, "customAnnotationSets.value": 1}, {"background": true});
db.sample.createIndex({"internal.status.name": 1, "studyUid": 1}, {"background": true});

print("\nCreating individual indexes...")
db.individual.createIndex({"uuid": 1, "version": 1}, {"unique": true, "background": true});
db.individual.createIndex({"uid": 1, "version": 1}, {"unique": true, "background": true});
db.individual.createIndex({"id": 1, "studyUid": 1, "version": 1}, {"unique": true, "background": true});
db.individual.createIndex({"name": 1, "studyUid": 1}, {"background": true});
db.individual.createIndex({"studyUid": 1}, {"background": true});
db.individual.createIndex({"father.uid": 1, "studyUid": 1}, {"background": true});
db.individual.createIndex({"mother.uid": 1, "studyUid": 1}, {"background": true});
db.individual.createIndex({"sex": 1, "studyUid": 1}, {"background": true});
db.individual.createIndex({"karyotypicSex": 1, "studyUid": 1}, {"background": true});
db.individual.createIndex({"dateOfBirth": 1, "studyUid": 1}, {"background": true});
db.individual.createIndex({"lifeStatus": 1, "studyUid": 1}, {"background": true});
db.individual.createIndex({"samples.uid": 1, "studyUid": 1}, {"background": true});
db.individual.createIndex({"phenotypes.id": 1, "studyUid": 1}, {"background": true});
db.individual.createIndex({"disorders.id": 1, "studyUid": 1}, {"background": true});
db.individual.createIndex({"_creationDate": 1, "studyUid": 1}, {"background": true});
db.individual.createIndex({"_modificationDate": 1, "studyUid": 1}, {"background": true});
db.individual.createIndex({"_lastOfVersion": 1, "studyUid": 1}, {"background": true});
db.individual.createIndex({"_acl": 1, "studyUid": 1}, {"background": true});
db.individual.createIndex({"customAnnotationSets.as": 1}, {"background": true});
db.individual.createIndex({"customAnnotationSets.vs": 1}, {"background": true});
db.individual.createIndex({"customAnnotationSets.id": 1, "customAnnotationSets.value": 1}, {"background": true});
db.individual.createIndex({"internal.status.name": 1, "studyUid": 1}, {"background": true});

print("\nCreating cohort indexes...")
db.cohort.createIndex({"uuid": 1}, {"unique": true, "background": true});
db.cohort.createIndex({"uid": 1}, {"unique": true, "background": true});
db.cohort.createIndex({"id": 1, "studyUid": 1}, {"unique": true, "background": true});
db.cohort.createIndex({"studyUid": 1}, {"background": true});
db.cohort.createIndex({"type": 1, "studyUid": 1}, {"background": true});
db.cohort.createIndex({"samples.uid": 1, "studyUid": 1}, {"background": true});
db.cohort.createIndex({"_creationDate": 1, "studyUid": 1}, {"background": true});
db.cohort.createIndex({"_modificationDate": 1, "studyUid": 1}, {"background": true});
db.cohort.createIndex({"_acl": 1, "studyUid": 1}, {"background": true});
db.cohort.createIndex({"customAnnotationSets.as": 1}, {"background": true});
db.cohort.createIndex({"customAnnotationSets.vs": 1}, {"background": true});
db.cohort.createIndex({"customAnnotationSets.id": 1, "customAnnotationSets.value": 1}, {"background": true});
db.cohort.createIndex({"internal.status.name": 1, "studyUid": 1}, {"background": true});

print("\nCreating family indexes...")
db.family.createIndex({"uuid": 1, "version": 1}, {"unique": true, "background": true});
db.family.createIndex({"uid": 1, "version": 1}, {"unique": true, "background": true});
db.family.createIndex({"id": 1, "studyUid": 1, "version": 1}, {"unique": true, "background": true});
db.family.createIndex({"name": 1, "studyUid": 1}, {"background": true});
db.family.createIndex({"studyUid": 1}, {"background": true});
db.family.createIndex({"members.uid": 1, "studyUid": 1}, {"background": true});
db.family.createIndex({"phenotypes.id": 1, "studyUid": 1}, {"background": true});
db.family.createIndex({"disorders.id": 1, "studyUid": 1}, {"background": true});
db.family.createIndex({"_creationDate": 1, "studyUid": 1}, {"background": true});
db.family.createIndex({"_modificationDate": 1, "studyUid": 1}, {"background": true});
db.family.createIndex({"_lastOfVersion": 1, "studyUid": 1}, {"background": true});
db.family.createIndex({"_acl": 1, "studyUid": 1}, {"background": true});
db.family.createIndex({"customAnnotationSets.as": 1}, {"background": true});
db.family.createIndex({"customAnnotationSets.vs": 1}, {"background": true});
db.family.createIndex({"customAnnotationSets.id": 1, "customAnnotationSets.value": 1}, {"background": true});
db.family.createIndex({"internal.status.name": 1, "studyUid": 1}, {"background": true});

print("\nCreating panel indexes...")
db.panel.createIndex({"uuid": 1, "version": 1}, {"unique": true, "background": true});
db.panel.createIndex({"uid": 1, "version": 1}, {"unique": true, "background": true});
db.panel.createIndex({"id": 1, "studyUid": 1, "version": 1}, {"unique": true, "background": true});
db.panel.createIndex({"studyUid": 1}, {"background": true});
db.panel.createIndex({"tags": 1, "studyUid": 1}, {"background": true});
db.panel.createIndex({"_creationDate": 1, "studyUid": 1}, {"background": true});
db.panel.createIndex({"_modificationDate": 1, "studyUid": 1}, {"background": true});
db.panel.createIndex({"_lastOfVersion": 1, "studyUid": 1}, {"background": true});
db.panel.createIndex({"_acl": 1, "studyUid": 1}, {"background": true});

print("\nCreating clinical indexes...")
db.clinical.createIndex({"uuid": 1}, {"unique": true, "background": true});
db.clinical.createIndex({"uid": 1}, {"unique": true, "background": true});
db.clinical.createIndex({"id": 1, "studyUid": 1}, {"unique": true, "background": true});
db.clinical.createIndex({"studyUid": 1}, {"background": true});
db.clinical.createIndex({"type": 1, "studyUid": 1}, {"background": true});
db.clinical.createIndex({"dueDate": 1, "studyUid": 1}, {"background": true});
db.clinical.createIndex({"priority": 1, "studyUid": 1}, {"background": true});
db.clinical.createIndex({"flags": 1, "studyUid": 1}, {"background": true});
db.clinical.createIndex({"_creationDate": 1, "studyUid": 1}, {"background": true});
db.clinical.createIndex({"_modificationDate": 1, "studyUid": 1}, {"background": true});
db.clinical.createIndex({"_acl": 1, "studyUid": 1}, {"background": true});
db.clinical.createIndex({"internal.status.name": 1, "studyUid": 1}, {"background": true});

print("\nCreating interpretation indexes...")
db.interpretation.createIndex({"uuid": 1}, {"unique": true, "background": true});
db.interpretation.createIndex({"uid": 1}, {"unique": true, "background": true});
db.interpretation.createIndex({"id": 1, "studyUid": 1}, {"unique": true, "background": true});
db.interpretation.createIndex({"studyUid": 1}, {"background": true});
db.interpretation.createIndex({"analyst.name": 1, "studyUid": 1}, {"background": true});
db.interpretation.createIndex({"method.name": 1, "studyUid": 1}, {"background": true});
db.interpretation.createIndex({"_creationDate": 1, "studyUid": 1}, {"background": true});
db.interpretation.createIndex({"_modificationDate": 1, "studyUid": 1}, {"background": true});
db.interpretation.createIndex({"internal.status.name": 1, "studyUid": 1}, {"background": true});

print("\nCreating indexes for deleted document collections...")
db.deleted_user.createIndex({"id": 1}, {"unique": true, "background": true});
db.deleted_user.createIndex({"projects.uid": 1}, {"unique": true, "background": true});
db.deleted_user.createIndex({"projects.uuid": 1}, {"unique": true, "background": true});
db.deleted_user.createIndex({"projects.fqn": 1}, {"unique": true, "background": true});
db.deleted_user.createIndex({"projects.id": 1, "id": 1}, {"unique": true, "background": true});

db.deleted_study.createIndex({"uid": 1}, {"unique": true, "background": true});
db.deleted_study.createIndex({"uuid": 1}, {"unique": true, "background": true});
db.deleted_study.createIndex({"fqn": 1}, {"unique": true, "background": true});
db.deleted_study.createIndex({"id": 1, "_project.uid": 1}, {"unique": true, "background": true});
db.deleted_study.createIndex({"groups.id": 1, "uid": 1}, {"unique": true, "background": true});
db.deleted_study.createIndex({"groups.userIds": 1, "uid": 1}, {"unique": true, "background": true});
db.deleted_study.createIndex({"_acl": 1}, {"background": true});
db.deleted_study.createIndex({"_project.uid": 1}, {"background": true});
db.deleted_study.createIndex({"variableSets.id": 1, "uid": 1}, {"unique": true, "background": true});

db.deleted_job.createIndex({"uuid": 1}, {"unique": true, "background": true});
db.deleted_job.createIndex({"uid": 1}, {"unique": true, "background": true});
db.deleted_job.createIndex({"id": 1, "studyUid": 1}, {"unique": true, "background": true});
db.deleted_job.createIndex({"studyUid": 1}, {"background": true});
db.deleted_job.createIndex({"tool.id": 1, "studyUid": 1}, {"background": true});
db.deleted_job.createIndex({"tool.type": 1, "studyUid": 1}, {"background": true});
db.deleted_job.createIndex({"userId": 1, "studyUid": 1}, {"background": true});
db.deleted_job.createIndex({"input.uid": 1, "studyUid": 1}, {"background": true});
db.deleted_job.createIndex({"output.uid": 1, "studyUid": 1}, {"background": true});
db.deleted_job.createIndex({"tags": 1, "studyUid": 1}, {"background": true});
db.deleted_job.createIndex({"visited": 1, "studyUid": 1}, {"background": true});
db.deleted_job.createIndex({"internal.status.name": 1, "studyUid": 1}, {"background": true});
db.deleted_job.createIndex({"_priority": 1, "_creationDate": 1, "studyUid": 1}, {"background": true});
db.deleted_job.createIndex({"_priority": 1, "studyUid": 1}, {"background": true});
db.deleted_job.createIndex({"_creationDate": 1, "studyUid": 1}, {"background": true});
db.deleted_job.createIndex({"_modificationDate": 1, "studyUid": 1}, {"background": true});
db.deleted_job.createIndex({"_acl": 1, "studyUid": 1}, {"background": true});

db.deleted_file.createIndex({"uuid": 1}, {"unique": true, "background": true});
db.deleted_file.createIndex({"uid": 1}, {"unique": true, "background": true});
db.deleted_file.createIndex({"id": 1, "studyUid": 1}, {"unique": true, "background": true});
db.deleted_file.createIndex({"path": 1, "studyUid": 1}, {"unique": true, "background": true});
db.deleted_file.createIndex({"name": 1, "studyUid": 1}, {"background": true});
db.deleted_file.createIndex({"studyUid": 1}, {"background": true});
db.deleted_file.createIndex({"_reverse": 1, "studyUid": 1}, {"background": true});
db.deleted_file.createIndex({"type": 1, "studyUid": 1}, {"background": true});
db.deleted_file.createIndex({"format": 1, "studyUid": 1}, {"background": true});
db.deleted_file.createIndex({"bioformat": 1, "studyUid": 1}, {"background": true});
db.deleted_file.createIndex({"uri": 1, "studyUid": 1}, {"background": true});
db.deleted_file.createIndex({"tags": 1, "studyUid": 1}, {"background": true});
db.deleted_file.createIndex({"samples.uid": 1, "studyUid": 1}, {"background": true});
db.deleted_file.createIndex({"_creationDate": 1, "studyUid": 1}, {"background": true});
db.deleted_file.createIndex({"_modificationDate": 1, "studyUid": 1}, {"background": true});
db.deleted_file.createIndex({"jobId": 1, "studyUid": 1}, {"background": true});
db.deleted_file.createIndex({"internal.status.name": 1, "studyUid": 1}, {"background": true});
db.deleted_file.createIndex({"_acl": 1, "studyUid": 1}, {"background": true});
db.deleted_file.createIndex({"customAnnotationSets.as": 1}, {"background": true});
db.deleted_file.createIndex({"customAnnotationSets.vs": 1}, {"background": true});
db.deleted_file.createIndex({"customAnnotationSets.id": 1, "customAnnotationSets.value": 1}, {"background": true});

db.deleted_sample.createIndex({"uuid": 1, "version": 1}, {"unique": true, "background": true});
db.deleted_sample.createIndex({"uid": 1, "version": 1}, {"unique": true, "background": true});
db.deleted_sample.createIndex({"id": 1, "studyUid": 1, "version": 1}, {"unique": true, "background": true});
db.deleted_sample.createIndex({"phenotypes.id": 1, "studyUid": 1}, {"background": true});
db.deleted_sample.createIndex({"fileIds": 1, "studyUid": 1}, {"background": true});
db.deleted_sample.createIndex({"individualId": 1, "studyUid": 1}, {"background": true});
db.deleted_sample.createIndex({"_creationDate": 1, "studyUid": 1}, {"background": true});
db.deleted_sample.createIndex({"_modificationDate": 1, "studyUid": 1}, {"background": true});
db.deleted_sample.createIndex({"studyUid": 1}, {"background": true});
db.deleted_sample.createIndex({"processing.product": 1, "studyUid": 1}, {"background": true});
db.deleted_sample.createIndex({"processing.preparationMethod": 1, "studyUid": 1}, {"background": true});
db.deleted_sample.createIndex({"processing.extractionMethod": 1, "studyUid": 1}, {"background": true});
db.deleted_sample.createIndex({"processing.labSampleId": 1, "studyUid": 1}, {"background": true});
db.deleted_sample.createIndex({"collection.tissue": 1, "studyUid": 1}, {"background": true});
db.deleted_sample.createIndex({"collection.organ": 1, "studyUid": 1}, {"background": true});
db.deleted_sample.createIndex({"collection.method": 1, "studyUid": 1}, {"background": true});
db.deleted_sample.createIndex({"_lastOfVersion": 1, "studyUid": 1}, {"background": true});
db.deleted_sample.createIndex({"_acl": 1, "studyUid": 1}, {"background": true});
db.deleted_sample.createIndex({"customAnnotationSets.as": 1}, {"background": true});
db.deleted_sample.createIndex({"customAnnotationSets.vs": 1}, {"background": true});
db.deleted_sample.createIndex({"customAnnotationSets.id": 1, "customAnnotationSets.value": 1}, {"background": true});
db.deleted_sample.createIndex({"internal.status.name": 1, "studyUid": 1}, {"background": true});

db.deleted_individual.createIndex({"uuid": 1, "version": 1}, {"unique": true, "background": true});
db.deleted_individual.createIndex({"uid": 1, "version": 1}, {"unique": true, "background": true});
db.deleted_individual.createIndex({"id": 1, "studyUid": 1, "version": 1}, {"unique": true, "background": true});
db.deleted_individual.createIndex({"name": 1, "studyUid": 1}, {"background": true});
db.deleted_individual.createIndex({"studyUid": 1}, {"background": true});
db.deleted_individual.createIndex({"father.uid": 1, "studyUid": 1}, {"background": true});
db.deleted_individual.createIndex({"mother.uid": 1, "studyUid": 1}, {"background": true});
db.deleted_individual.createIndex({"sex": 1, "studyUid": 1}, {"background": true});
db.deleted_individual.createIndex({"karyotypicSex": 1, "studyUid": 1}, {"background": true});
db.deleted_individual.createIndex({"dateOfBirth": 1, "studyUid": 1}, {"background": true});
db.deleted_individual.createIndex({"lifeStatus": 1, "studyUid": 1}, {"background": true});
db.deleted_individual.createIndex({"samples.uid": 1, "studyUid": 1}, {"background": true});
db.deleted_individual.createIndex({"phenotypes.id": 1, "studyUid": 1}, {"background": true});
db.deleted_individual.createIndex({"disorders.id": 1, "studyUid": 1}, {"background": true});
db.deleted_individual.createIndex({"_creationDate": 1, "studyUid": 1}, {"background": true});
db.deleted_individual.createIndex({"_modificationDate": 1, "studyUid": 1}, {"background": true});
db.deleted_individual.createIndex({"_lastOfVersion": 1, "studyUid": 1}, {"background": true});
db.deleted_individual.createIndex({"_acl": 1, "studyUid": 1}, {"background": true});
db.deleted_individual.createIndex({"customAnnotationSets.as": 1}, {"background": true});
db.deleted_individual.createIndex({"customAnnotationSets.vs": 1}, {"background": true});
db.deleted_individual.createIndex({"customAnnotationSets.id": 1, "customAnnotationSets.value": 1}, {"background": true});
db.deleted_individual.createIndex({"internal.status.name": 1, "studyUid": 1}, {"background": true});

db.deleted_cohort.createIndex({"uuid": 1}, {"unique": true, "background": true});
db.deleted_cohort.createIndex({"uid": 1}, {"unique": true, "background": true});
db.deleted_cohort.createIndex({"id": 1, "studyUid": 1}, {"unique": true, "background": true});
db.deleted_cohort.createIndex({"studyUid": 1}, {"background": true});
db.deleted_cohort.createIndex({"type": 1, "studyUid": 1}, {"background": true});
db.deleted_cohort.createIndex({"samples.uid": 1, "studyUid": 1}, {"background": true});
db.deleted_cohort.createIndex({"_creationDate": 1, "studyUid": 1}, {"background": true});
db.deleted_cohort.createIndex({"_modificationDate": 1, "studyUid": 1}, {"background": true});
db.deleted_cohort.createIndex({"_acl": 1, "studyUid": 1}, {"background": true});
db.deleted_cohort.createIndex({"customAnnotationSets.as": 1}, {"background": true});
db.deleted_cohort.createIndex({"customAnnotationSets.vs": 1}, {"background": true});
db.deleted_cohort.createIndex({"customAnnotationSets.id": 1, "customAnnotationSets.value": 1}, {"background": true});
db.deleted_cohort.createIndex({"internal.status.name": 1, "studyUid": 1}, {"background": true});

db.deleted_family.createIndex({"uuid": 1, "version": 1}, {"unique": true, "background": true});
db.deleted_family.createIndex({"uid": 1, "version": 1}, {"unique": true, "background": true});
db.deleted_family.createIndex({"id": 1, "studyUid": 1, "version": 1}, {"unique": true, "background": true});
db.deleted_family.createIndex({"name": 1, "studyUid": 1}, {"background": true});
db.deleted_family.createIndex({"studyUid": 1}, {"background": true});
db.deleted_family.createIndex({"members.uid": 1, "studyUid": 1}, {"background": true});
db.deleted_family.createIndex({"phenotypes.id": 1, "studyUid": 1}, {"background": true});
db.deleted_family.createIndex({"disorders.id": 1, "studyUid": 1}, {"background": true});
db.deleted_family.createIndex({"_creationDate": 1, "studyUid": 1}, {"background": true});
db.deleted_family.createIndex({"_modificationDate": 1, "studyUid": 1}, {"background": true});
db.deleted_family.createIndex({"_lastOfVersion": 1, "studyUid": 1}, {"background": true});
db.deleted_family.createIndex({"_acl": 1, "studyUid": 1}, {"background": true});
db.deleted_family.createIndex({"customAnnotationSets.as": 1}, {"background": true});
db.deleted_family.createIndex({"customAnnotationSets.vs": 1}, {"background": true});
db.deleted_family.createIndex({"customAnnotationSets.id": 1, "customAnnotationSets.value": 1}, {"background": true});
db.deleted_family.createIndex({"internal.status.name": 1, "studyUid": 1}, {"background": true});

db.deleted_panel.createIndex({"uuid": 1, "version": 1}, {"unique": true, "background": true});
db.deleted_panel.createIndex({"uid": 1, "version": 1}, {"unique": true, "background": true});
db.deleted_panel.createIndex({"id": 1, "studyUid": 1, "version": 1}, {"unique": true, "background": true});
db.deleted_panel.createIndex({"studyUid": 1}, {"background": true});
db.deleted_panel.createIndex({"tags": 1, "studyUid": 1}, {"background": true});
db.deleted_panel.createIndex({"_creationDate": 1, "studyUid": 1}, {"background": true});
db.deleted_panel.createIndex({"_modificationDate": 1, "studyUid": 1}, {"background": true});
db.deleted_panel.createIndex({"_lastOfVersion": 1, "studyUid": 1}, {"background": true});
db.deleted_panel.createIndex({"_acl": 1, "studyUid": 1}, {"background": true});

db.deleted_clinical.createIndex({"uuid": 1}, {"unique": true, "background": true});
db.deleted_clinical.createIndex({"uid": 1}, {"unique": true, "background": true});
db.deleted_clinical.createIndex({"id": 1, "studyUid": 1}, {"unique": true, "background": true});
db.deleted_clinical.createIndex({"studyUid": 1}, {"background": true});
db.deleted_clinical.createIndex({"type": 1, "studyUid": 1}, {"background": true});
db.deleted_clinical.createIndex({"dueDate": 1, "studyUid": 1}, {"background": true});
db.deleted_clinical.createIndex({"priority": 1, "studyUid": 1}, {"background": true});
db.deleted_clinical.createIndex({"flags": 1, "studyUid": 1}, {"background": true});
db.deleted_clinical.createIndex({"_creationDate": 1, "studyUid": 1}, {"background": true});
db.deleted_clinical.createIndex({"_modificationDate": 1, "studyUid": 1}, {"background": true});
db.deleted_clinical.createIndex({"_acl": 1, "studyUid": 1}, {"background": true});
db.deleted_clinical.createIndex({"internal.status.name": 1, "studyUid": 1}, {"background": true});

db.deleted_interpretation.createIndex({"uuid": 1}, {"unique": true, "background": true});
db.deleted_interpretation.createIndex({"uid": 1}, {"unique": true, "background": true});
db.deleted_interpretation.createIndex({"id": 1, "studyUid": 1}, {"unique": true, "background": true});
db.deleted_interpretation.createIndex({"studyUid": 1}, {"background": true});
db.deleted_interpretation.createIndex({"analyst.name": 1, "studyUid": 1}, {"background": true});
db.deleted_interpretation.createIndex({"method.name": 1, "studyUid": 1}, {"background": true});
db.deleted_interpretation.createIndex({"_creationDate": 1, "studyUid": 1}, {"background": true});
db.deleted_interpretation.createIndex({"_modificationDate": 1, "studyUid": 1}, {"background": true});
db.deleted_interpretation.createIndex({"internal.status.name": 1, "studyUid": 1}, {"background": true});