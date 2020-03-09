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

// #1535
migrateCollection("individual", {"internal": {"$exists": false}}, {}, function(bulk, doc) {
    doc['status']['description'] = doc['status']['message'];

    var set = {
        "internal": {
            "status": doc['status']
        },
        "status": customStatus
    };
    var unset = {
        "affectationStatus": ""
    };

    bulk.find({"_id": doc._id}).updateOne({"$set": set, "$unset": unset});
});

// #1536
migrateCollection("family", {"internal": {"$exists": false}}, {}, function(bulk, doc) {
    doc['status']['description'] = doc['status']['message'];

    var set = {
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

// TODO: Add indexes for new "deleted" collections
