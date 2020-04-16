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

// Add empty array of tags when tags = null
db.getCollection("file").update({"tags": null}, {"$set": {"tags": []}}, {"multi": true});

// Drop dataset collection
db.getCollection("dataset").drop()

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
        "status": {
            "name": "READY",
            "date": metadata["creationDate"],
            "message": ""
        },
        "quota": {
            "diskUsage": -1,
            "cpuUsage": -1,
            "maxDisk": -1,
            "maxCpu": -1
        },
        "projects": [],
        "tools": [],
        "configs": {
            "filters": []
        },
        "attributes": {}
    });
    // Remove from metadata collection the admin credentials
    db.getCollection("metadata").update({}, {"$unset": {"admin.email": "", "admin.password": ""}})
}

// Remove experiments,lastModified, cipher and tools from studies and users documents
db.getCollection("study").update({}, {"$unset": {"experiments": "", "lastModified": "", "cipher": ""}}, {"multi": true});
db.getCollection("user").update({}, {"$unset": {"tools": ""}}, {"multi": true});

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

// Ticket #1479 - TEXT -> STRING
migrateCollection("study", {"variableSets" : { $exists: true, $ne: [] } }, {"variableSets": 1}, function(bulk, doc) {
    var setChanges = {};

    // Check variableSets
    if (isNotUndefinedOrNull(doc.variableSets) && doc.variableSets.length > 0) {
        for (var i in doc.variableSets) {
            changeVariableType(doc.variableSets[i].variables);
        }
        setChanges["variableSets"] = doc.variableSets;
    }

    if (Object.keys(setChanges).length > 0) {
        bulk.find({"_id": doc._id}).updateOne({"$set": setChanges});
    }
});

// Ticket #1487
db.getCollection("study").update({"notification": { $exists: false }}, {
    "$set": {
        "notification": {
            "webhook": null
        }}}, {"multi": true})

// Ticket #1513 - Remove 'name' field from groups
migrateCollection("study", {}, {groups: 1}, function(bulk, doc) {
    if (isEmptyArray(doc.groups) || isUndefinedOrNull(doc.groups[0].name)) {
        return;
    }

    for (var i = 0; i < doc.groups.length; i++) {
        delete doc.groups[i]['name'];
    }

    var params = {
        groups: doc.groups
    };

    bulk.find({"_id": doc._id}).updateOne({"$set": params});
});

var customStatus = {
    "name": "",
    "description": "",
    "date" :""
};

// Tickets #1528 #1529
migrateCollection("user", {"_password": {"$exists": false}}, {}, function(bulk, doc) {
    // #1531
    doc['status']['description'] = doc['status']['message'];

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
        "configs.filters": ""
    };

    // #1529
    if (isNotEmptyArray(doc.projects)) {
        for (var i = 0; i < doc.projects.length; i++) {
            var project = doc.projects[i];

            project['uuid'] = generateOpenCGAUUID("PROJECT", project['_creationDate']);
            project['internal'] = {
                'status': project['status'],
                'datastores': project['dataStores']
            };

            // #1531
            project['internal']['status']['description'] = project['internal']['status']['message']

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

// #1532
migrateCollection("study", {"internal": {"$exists": false}}, {}, function(bulk, doc) {
    doc['status']['description'] = doc['status']['message'];

    var set = {
        "uuid": generateOpenCGAUUID("STUDY", doc['_creationDate']),
        "internal": {
            "status": doc['status']
        },
        "status": customStatus
    };
    var unset = {
        "stats": "",
        "type": ""
    };

    bulk.find({"_id": doc._id}).updateOne({"$set": set, "$unset": unset});
});
FALTA VARIABLESETS
// #1535
migrateCollection("individual", {"internal": {"$exists": false}}, {}, function(bulk, doc) {
    doc['status']['description'] = doc['status']['message'];

    var set = {
        "uuid": generateOpenCGAUUID("INDIVIDUAL", doc['_creationDate']),
        "internal": {
            "status": doc['status']
        },
        "status": customStatus
    };
    var unset = {
        "affectationStatus": "",
        "multiples": ""
    };

    bulk.find({"_id": doc._id}).updateOne({"$set": set, "$unset": unset});
});

// #1536
migrateCollection("family", {"internal": {"$exists": false}}, {}, function(bulk, doc) {
    doc['status']['description'] = doc['status']['message'];

    var set = {
        "uuid": generateOpenCGAUUID("FAMILY", doc['_creationDate']),
        "internal": {
            "status": doc['status']
        },
        "status": customStatus
    };

    bulk.find({"_id": doc._id}).updateOne({"$set": set});
});

// #1537
migrateCollection("clinical", {"internal": {"$exists": false}}, {}, function(bulk, doc) {
    doc['status']['description'] = doc['status']['message'];

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

// #1540
migrateCollection("sample", {"internal": {"$exists": false}}, {}, function(bulk, doc) {
    doc['status']['description'] = doc['status']['message'];

    var set = {
        "uuid": generateOpenCGAUUID("SAMPLE", doc['_creationDate']),
        "internal": {
            "status": doc['status']
        },
        "status": customStatus
    };
    var unset = {
        "name": "",
        "source": "",
        "stats": "",
        "type": ""
    };

    bulk.find({"_id": doc._id}).updateOne({"$set": set, "$unset": unset});
});

// #1539
migrateCollection("cohort", {"internal": {"$exists": false}}, {}, function(bulk, doc) {
    doc['status']['description'] = doc['status']['message'];

    var set = {
        "uuid": generateOpenCGAUUID("COHORT", doc['_creationDate']),
        "internal": {
            "status": doc['status']
        },
        "status": customStatus
    };
    var unset = {
        "name": "",
        "stats": ""
    };

    bulk.find({"_id": doc._id}).updateOne({"$set": set, "$unset": unset});
});

// #1533
migrateCollection("file", {"internal": {"$exists": false}}, {}, function(bulk, doc) {
    doc['status']['description'] = doc['status']['message'];

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
        "job": ""
    };

    bulk.find({"_id": doc._id}).updateOne({"$set": set, "$unset": unset});
});

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

migrateCollection("panel", {"internal": {"$exists": false}}, {}, function(bulk, doc) {
    var set = {
        "uuid": generateOpenCGAUUID("INTERPRETATION", doc['_creationDate'])
        }
    };

    bulk.find({"_id": doc._id}).updateOne({"$set": set});
});

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


// TODO: Add indexes for new "deleted" collections
