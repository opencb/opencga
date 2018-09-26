// First we will completely remove all the indexes
db.clinical.dropIndexes();
db.cohort.dropIndexes();
db.dataset.dropIndexes();
db.family.dropIndexes();
db.file.dropIndexes();
db.individual.dropIndexes();
db.job.dropIndexes();
db.panel.dropIndexes();
db.sample.dropIndexes();
db.study.dropIndexes();
db.user.dropIndexes();

// long.js library extracted from https://github.com/dcodeIO/long.js
load("catalog/long.js");

var Entity = Object.freeze({"PROJECT": 1, "STUDY": 2, "FILE": 3, "SAMPLE": 4, "COHORT": 5, "INDIVIDUAL": 6, "FAMILY": 7, "JOB": 8,
    "CLINICAL": 9, "PANEL": 10});


// Converts an ArrayBuffer directly to base64, without any intermediate 'convert to string then use window.btoa' step
// Extracted from https://gist.github.com/jonleighton/958841
function base64ArrayBuffer(arrayBuffer) {
    var base64    = ''
    // We modify the original encodings to convert url safe base64Url
    var encodings = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_'

    var bytes         = new Uint8Array(arrayBuffer)
    var byteLength    = bytes.byteLength
    var byteRemainder = byteLength % 3
    var mainLength    = byteLength - byteRemainder

    var a, b, c, d;
    var chunk;

    // Main loop deals with bytes in chunks of 3
    for (var i = 0; i < mainLength; i = i + 3) {
        // Combine the three bytes into a single integer
        chunk = (bytes[i] << 16) | (bytes[i + 1] << 8) | bytes[i + 2]

        // Use bitmasks to extract 6-bit segments from the triplet
        a = (chunk & 16515072) >> 18 // 16515072 = (2^6 - 1) << 18
        b = (chunk & 258048)   >> 12 // 258048   = (2^6 - 1) << 12
        c = (chunk & 4032)     >>  6 // 4032     = (2^6 - 1) << 6
        d = chunk & 63               // 63       = 2^6 - 1

        // Convert the raw binary segments to the appropriate ASCII encoding
        base64 += encodings[a] + encodings[b] + encodings[c] + encodings[d]
    }

    // Deal with the remaining bytes and padding
    if (byteRemainder == 1) {
        chunk = bytes[mainLength]

        a = (chunk & 252) >> 2 // 252 = (2^6 - 1) << 2

        // Set the 4 least significant bits to zero
        b = (chunk & 3)   << 4 // 3   = 2^2 - 1

        base64 += encodings[a] + encodings[b] + '=='
    } else if (byteRemainder == 2) {
        chunk = (bytes[mainLength] << 8) | bytes[mainLength + 1]

        a = (chunk & 64512) >> 10 // 64512 = (2^6 - 1) << 10
        b = (chunk & 1008)  >>  4 // 1008  = (2^6 - 1) << 4

        // Set the 2 least significant bits to zero
        c = (chunk & 15)    <<  2 // 15    = 2^4 - 1

        base64 += encodings[a] + encodings[b] + encodings[c] + '='
    }

    return base64
}


print("Migrating clinical analysis");
migrateCollection("clinical", {"uuid": {$exists: false}}, {attributes: 0, annotationSets: 0}, function(bulk, doc) {
    var setChanges = {};
    var unsetChanges = {};
    addPermissionRules(doc, setChanges);
    addPrivateCreationDateAndModificationDate(doc, setChanges);

    /* uid and uuid migration: #819 */
    setChanges["uid"] = doc["id"];
    setChanges["id"] = doc["name"];
    setChanges["studyUid"] = doc["_studyId"];
    setChanges["uuid"] = generateOpenCGAUUID("CLINICAL", setChanges["_creationDate"]);

    unsetChanges["_studyId"] = "";

    // Check germline file
    if (typeof doc.germline !== "undefined" && typeof doc.germline.id !== "undefined") {
        setChanges["germline"] = {
            "uid": doc.germline.id
        };
    }

    // Check somatic file
    if (typeof doc.somatic !== "undefined" && typeof doc.somatic.id !== "undefined") {
        setChanges["somatic"] = {
            "uid": doc.somatic.id
        };
    }

    // Check family
    if (typeof doc.family !== "undefined" && typeof doc.family.id !== "undefined") {
        setChanges["family"] = {
            "uid": doc.family.id
        };
    }

    // Check subjects
    if (typeof doc.subjects !== "undefined" && doc.subjects.length > 0) {
        for (var i in doc.subjects) {
            var member = doc.subjects[i];

            member["uid"] = member["id"];
            delete member["id"];
        }

        setChanges["subjects"] = doc.subjects;
    }

    // Check clinical interpretations
    if (typeof doc.interpretations !== "undefined" && doc.interpretations.length > 0) {
        for (var i in doc.interpretations) {
            var interpretation = doc.interpretations[i];
            if (typeof interpretation.file !== "undefined" && typeof interpretation.file.id !== "undefined") {
                interpretation["file"] = {
                    "uid": interpretation.file.id
                };
            }
        }

        setChanges["interpretations"] = doc.interpretations;
    }
    /* end uid and uuid migration: #819 */


    bulk.find({"_id": doc._id}).updateOne({"$set": setChanges, "$unset": unsetChanges});
});

print("\nMigrating cohort");
migrateCollection("cohort", {"uuid": {$exists: false}}, {attributes: 0, annotationSets: 0}, function(bulk, doc) {
    var setChanges = {};
    var unsetChanges = {};
    addPermissionRules(doc, setChanges);
    addPrivateCreationDateAndModificationDate(doc, setChanges);

    /* uid and uuid migration: #819 */
    setChanges["uid"] = doc["id"];
    setChanges["id"] = doc["name"];
    setChanges["studyUid"] = doc["_studyId"];
    setChanges["uuid"] = generateOpenCGAUUID("COHORT", setChanges["_creationDate"]);

    unsetChanges["acl"] = "";
    unsetChanges["_studyId"] = "";

    // Check samples
    if (typeof doc.samples !== "undefined" && doc.samples.length > 0) {
        for (var i in doc.samples) {
            var sample = doc.samples[i];

            sample["uid"] = sample["id"];
            delete sample["id"];
        }
        setChanges["samples"] = doc.samples;
    }
    /* end uid and uuid migration: #819 */

    bulk.find({"_id": doc._id}).updateOne({"$set": setChanges, "$unset": unsetChanges});
});

print("\nMigrating family");
migrateCollection("family", {"uuid": {$exists: false}}, {attributes: 0, annotationSets: 0}, function(bulk, doc) {
    var setChanges = {
        "expectedSize": -1 // #823
    };
    addPermissionRules(doc, setChanges);
    addPrivateCreationDateAndModificationDate(doc, setChanges);

    /* uid and uuid migration: #819 */
    setChanges["uid"] = doc["id"];
    setChanges["id"] = doc["name"];
    setChanges["studyUid"] = doc["_studyId"];
    setChanges["version"] = NumberInt(doc["version"]);
    setChanges["uuid"] = generateOpenCGAUUID("FAMILY", setChanges["_creationDate"]);

    unsetChanges["_studyId"] = "";

    // Check members
    if (typeof doc.members !== "undefined" && doc.members.length > 0) {
        for (var i in doc.members) {
            var member = doc.members[i];

            member["uid"] = member["id"];
            member["version"] = NumberInt(member["version"]);
            delete member["id"];
        }

        setChanges["members"] = doc.members;
    }
    /* end uid and uuid migration: #819 */

    bulk.find({"_id": doc._id}).updateOne({"$set": setChanges, "$unset": unsetChanges});
});

print("\nMigrating file");
migrateCollection("file", {"uuid": {$exists: false}}, {attributes: 0, stats: 0}, function(bulk, doc) {
    var setChanges = {
        "customAnnotationSets": [], // #823
        "checksum": ""              // #823
    };
    var unsetChanges = {};
    addPermissionRules(doc, setChanges);
    addPrivateCreationDateAndModificationDate(doc, setChanges);

    /* uid and uuid migration: #819 */
    setChanges["uid"] = doc["id"];
    setChanges["studyUid"] = doc["_studyId"];
    setChanges["id"] = doc["path"].replace(/\//g, ":");
    setChanges["uuid"] = generateOpenCGAUUID("FILE", setChanges["_creationDate"]);

    /* #906 */
    setChanges["_reverse"] = doc["name"].split("").reverse().join("");

    /* Check bigwig format autodetection */
    if (doc.name.endsWith(".bw")) {
        setChanges["format"] = "BIGWIG";
        setChanges["bioformat"] = "COVERAGE";
    }

    unsetChanges["acl"] = "";
    unsetChanges["_studyId"] = "";

    // Check samples
    if (typeof doc.samples !== "undefined" && doc.samples.length > 0) {
        for (var i in doc.samples) {
            var sample = doc.samples[i];

            sample["uid"] = sample["id"];
            delete sample["id"];
        }

        setChanges["samples"] = doc.samples;
    }

    // Check job
    if (typeof doc.job !== "undefined" && typeof doc.job.id !== "undefined") {
        setChanges["job"] = {
            "uid": doc.job.id
        };
    }

    // Check experiment
    if (typeof doc.experiment !== "undefined" && typeof doc.experiment.id !== "undefined") {
        setChanges["experiment"] = {
            "uid": doc.experiment.id
        };
    }
    /* end uid and uuid migration: #819 */

    bulk.find({"_id": doc._id}).updateOne({"$set": setChanges, "$unset": unsetChanges});
});

print("\nMigrating individual");
migrateCollection("individual", {"uuid": {$exists: false}}, {attributes: 0, annotationSets: 0}, function(bulk, doc) {
    var setChanges = {
        location: {  // #823
            address: null,
            postalCode: null,
            city: null,
            state: null,
            country: null
        }
    };
    var unsetChanges = {};
    addPermissionRules(doc, setChanges);
    addPrivateCreationDateAndModificationDate(doc, setChanges);

    /* uid and uuid migration: #819 */
    setChanges["uid"] = doc["id"];
    setChanges["id"] = doc["name"];
    setChanges["studyUid"] = doc["_studyId"];
    setChanges["version"] = NumberInt(doc["version"]);
    setChanges["uuid"] = generateOpenCGAUUID("INDIVIDUAL", setChanges["_creationDate"]);

    unsetChanges["acl"] = "";
    unsetChanges["_studyId"] = "";

    // Check father
    if (typeof doc.father !== "undefined" && typeof doc.father.id !== "undefined") {
        setChanges["father"] = {
            "uid": doc.father.id
        }
    }

    // Check mother
    if (typeof doc.mother !== "undefined" && typeof doc.mother.id !== "undefined") {
        setChanges["mother"] = {
            "uid": doc.mother.id
        }
    }

    // Check samples
    if (typeof doc.samples !== "undefined" && doc.samples.length > 0) {
        for (var i in doc.samples) {
            var sample = doc.samples[i];

            sample["uid"] = sample["id"];
            sample["version"] = NumberInt(sample["version"]);
            delete sample["id"];
        }

        setChanges["samples"] = doc.samples;
    }
    /* end uid and uuid migration: #819 */

    bulk.find({"_id": doc._id}).updateOne({"$set": setChanges, "$unset": unsetChanges});
});

print("\nMigrating job");
migrateCollection("job", {"uuid": {$exists: false}}, {attributes: 0}, function(bulk, doc) {
    var setChanges = {};
    var unsetChanges = {};
    addPermissionRules(doc, setChanges);
    addPrivateCreationDateAndModificationDate(doc, setChanges);

    /* uid and uuid migration: #819 */
    setChanges["uid"] = doc["id"];
    setChanges["id"] = doc["name"];
    setChanges["studyUid"] = doc["_studyId"];
    setChanges["uuid"] = generateOpenCGAUUID("JOB", setChanges["_creationDate"]);

    unsetChanges["acl"] = "";
    unsetChanges["_studyId"] = "";

    // Check input
    if (typeof doc.input !== "undefined" && doc.input.length > 0) {
        for (var i in doc.input) {
            var input = doc.input[i];

            input["uid"] = input["id"];
            delete input["id"];
        }

        setChanges["input"] = doc.input;
    }

    // Check output
    if (typeof doc.output !== "undefined" && doc.output.length > 0) {
        for (var i in doc.output) {
            var output = doc.output[i];

            output["uid"] = output["id"];
            delete output["id"];
        }

        setChanges["output"] = doc.output;
    }

    // Check outDir
    if (typeof doc.outDir !== "undefined" && typeof doc.outDir.id !== "undefined") {
        setChanges["outDir"] = {
            "uid": doc.outDir.id
        }
    }

    // Check stdOutput
    if (typeof doc.stdOutput !== "undefined" && typeof doc.stdOutput.id !== "undefined") {
        setChanges["stdOutput"] = {
            "uid": doc.stdOutput.id
        }
    }

    // Check stdError
    if (typeof doc.stdError !== "undefined" && typeof doc.stdError.id !== "undefined") {
        setChanges["stdError"] = {
            "uid": doc.stdError.id
        }
    }
    /* end uid and uuid migration: #819 */

    bulk.find({"_id": doc._id}).updateOne({"$set": setChanges, "$unset": unsetChanges});
});

print("\nMigrating sample");
migrateCollection("sample", {"uuid": {$exists: false}}, {attributes: 0, annotationSets: 0}, function(bulk, doc) {
    var setChanges = {};
    var unsetChanges = {};

    addPermissionRules(doc, setChanges);
    addPrivateCreationDateAndModificationDate(doc, setChanges);

    /* uid and uuid migration: #819 */
    setChanges["uid"] = doc["id"];
    setChanges["id"] = doc["name"];
    setChanges["version"] = NumberInt(doc["version"]);
    setChanges["studyUid"] = doc["_studyId"];
    setChanges["uuid"] = generateOpenCGAUUID("SAMPLE", setChanges["_creationDate"]);

    unsetChanges["acl"] = "";
    unsetChanges["_studyId"] = "";
    /* end uid and uuid migration: #819 */

    bulk.find({"_id": doc._id}).updateOne({"$set": setChanges, "$unset": unsetChanges});
});

// Global variable populated by the migrateCollection("user"... and used in migrateCollection("study"...
var projectUidFqnMap = {};

print("\nMigrating user");
migrateCollection("user", {"uuid": {$exists: false}}, {attributes: 0}, function(bulk, doc) {
    if (typeof doc.projects !== "undefined" && doc.projects.length > 0) {
        var changes = {};

        var projects = [];
        for (var i in doc.projects) {
            var project = doc.projects[i];
            addPrivateCreationDateAndModificationDate(project, project);

            /* uid and uuid migration: #819 */
            project["uid"] = project["id"];
            project["id"] = project["alias"];
            project["fqn"] = doc.id + "@" + project["alias"];

            project["uuid"] = generateOpenCGAUUID("PROJECT", project["_creationDate"]);

            // We populate the global map
            projectUidFqnMap[project["uid"]] = project["fqn"];
            /* end uid and uuid migration: #819 */

            projects.push(project);
        }
        if (projects.length > 0) {
            changes["projects"] = projects;
        }

        if (Object.keys(changes).length > 0) {
            bulk.find({"_id": doc._id}).updateOne({"$set": changes});
        }
    }
});

print("\nMigrating study");
migrateCollection("study", {"uuid": {$exists: false}}, {attributes: 0}, function(bulk, doc) {
    var setChanges = {
        "permissionRules": {}       // #745
    };
    var unsetChanges = {};

    addPrivateCreationDateAndModificationDate(doc, setChanges);

    /* uid and uuid migration: #819 */
    setChanges["uid"] = doc["id"];
    setChanges["id"] = doc["alias"];
    setChanges["fqn"] = projectUidFqnMap[doc._projectId] + ":" + doc["alias"];
    setChanges["_project"] = {
        "id": projectUidFqnMap[doc._projectId].split("@")[1],
        "uid": doc._projectId
    };
    setChanges["uuid"] = generateOpenCGAUUID("STUDY", setChanges["_creationDate"]);

    unsetChanges["_projectId"] = "";
    unsetChanges["acl"] = "";

    // Check variableSets
    if (typeof doc.variableSets !== "undefined" && doc.variableSets.length > 0) {
        for (var i in doc.variableSets) {
            var variableSet = doc.variableSets[i];
            variableSet["uid"] = variableSet["id"];
            variableSet["id"] = variableSet["name"];

            changeVariableIds(variableSet.variables);
        }
        setChanges["variableSets"] = doc.variableSets;
    }
    /* end uid and uuid migration: #819 */


    bulk.find({"_id": doc._id}).updateOne({"$set": setChanges, "$unset": unsetChanges});
});
print("");

// Update metadata version
db.metadata.update({}, {"$set": {"version": "1.4.0"}});

// Create all the indexes
db.user.createIndex({"id": 1}, {"background": true});
db.user.createIndex({"projects.uid": 1}, {"background": true});
db.user.createIndex({"projects.uuid": 1}, {"background": true});
db.user.createIndex({"projects.id": 1, "uid": 1}, {"background": true});

db.study.createIndex({"uid": 1}, {"background": true});
db.study.createIndex({"uuid": 1}, {"background": true});
db.study.createIndex({"id": 1, "_project.uid": 1}, {"unique": true, "background": true});
db.study.createIndex({"status.name": 1}, {"background": true});
db.study.createIndex({"_acl": 1}, {"background": true});
db.study.createIndex({"_project.uid": 1}, {"background": true});

db.job.createIndex({"uuid": 1}, {"background": true});
db.job.createIndex({"uid": 1}, {"background": true});
db.job.createIndex({"id": 1, "studyUid": 1}, {"unique": true, "background": true});
db.job.createIndex({"toolId": 1, "studyUid": 1, "status.name": 1}, {"background": true});
db.job.createIndex({"status.name": 1, "studyUid": 1}, {"background": true});
db.job.createIndex({"input.uid": 1, "studyUid": 1, "status.name": 1}, {"background": true});
db.job.createIndex({"output.uid": 1, "studyUid": 1, "status.name": 1}, {"background": true});
db.job.createIndex({"tags": 1, "studyUid": 1, "status.name": 1}, {"background": true});
db.job.createIndex({"_acl": 1, "studyUid": 1, "status.name": 1}, {"background": true});

db.file.createIndex({"uuid": 1}, {"background": true});
db.file.createIndex({"uid": 1}, {"background": true});
db.file.createIndex({"id": 1, "studyUid": 1}, {"unique": true, "background": true});
db.file.createIndex({"path": 1, "studyUid": 1}, {"unique": true, "background": true});
db.file.createIndex({"name": 1, "studyUid": 1, "status.name": 1}, {"background": true});
db.file.createIndex({"_reverse": 1, "studyUid": 1, "status.name": 1}, {"background": true});
db.file.createIndex({"type": 1, "studyUid": 1, "status.name": 1}, {"background": true});
db.file.createIndex({"format": 1, "studyUid": 1, "status.name": 1}, {"background": true});
db.file.createIndex({"bioformat": 1, "studyUid": 1, "status.name": 1}, {"background": true});
db.file.createIndex({"uri": 1, "studyUid": 1, "status.name": 1}, {"background": true});
db.file.createIndex({"tags": 1, "studyUid": 1, "status.name": 1}, {"background": true});
db.file.createIndex({"status.name": 1, "studyUid": 1}, {"background": true});
db.file.createIndex({"samples.uid": 1, "studyUid": 1, "status.name": 1}, {"background": true});
db.file.createIndex({"job.uid": 1, "studyUid": 1, "status.name": 1}, {"background": true});
db.file.createIndex({"_acl": 1, "studyUid": 1, "status.name": 1}, {"background": true});
db.file.createIndex({"studyUid": 1}, {"background": true});
db.file.createIndex({"customAnnotationSets.as": 1}, {"background": true});
db.file.createIndex({"customAnnotationSets.vs": 1}, {"background": true});
db.file.createIndex({"customAnnotationSets.id": 1, "customAnnotationSets.value": 1}, {"background": true});

db.sample.createIndex({"uuid": 1, "version": 1}, {"unique": true, "background": true});
db.sample.createIndex({"uid": 1, "version": 1}, {"unique": true, "background": true});
db.sample.createIndex({"id": 1, "studyUid": 1, "version": 1}, {"unique": true, "background": true});
db.sample.createIndex({"_acl": 1, "studyUid": 1, "status.name": 1}, {"background": true});
db.sample.createIndex({"customAnnotationSets.as": 1}, {"background": true});
db.sample.createIndex({"customAnnotationSets.vs": 1}, {"background": true});
db.sample.createIndex({"customAnnotationSets.id": 1, "customAnnotationSets.value": 1}, {"background": true});
db.sample.createIndex({"status.name": 1, "studyUid": 1}, {"background": true});
db.sample.createIndex({"phenotypes.id": 1, "studyUid": 1, "status.name": 1}, {"background": true});
db.sample.createIndex({"studyUid": 1}, {"background": true});
db.sample.createIndex({"_lastOfVersion": 1, "studyUid": 1}, {"background": true});

db.individual.createIndex({"uuid": 1, "version": 1}, {"unique": true, "background": true});
db.individual.createIndex({"uid": 1, "version": 1}, {"unique": true, "background": true});
db.individual.createIndex({"id": 1, "studyUid": 1, "version": 1}, {"unique": true, "background": true});
db.individual.createIndex({"name": 1, "studyUid": 1}, {"background": true});
db.individual.createIndex({"_acl": 1, "studyUid": 1, "status.name": 1}, {"background": true});
db.individual.createIndex({"status.name": 1, "studyUid": 1}, {"background": true});
db.individual.createIndex({"samples.uid": 1, "studyUid": 1, "status.name": 1}, {"background": true});
db.individual.createIndex({"phenotypes.id": 1, "studyUid": 1, "status.name": 1}, {"background": true});
db.individual.createIndex({"customAnnotationSets.as": 1}, {"background": true});
db.individual.createIndex({"customAnnotationSets.vs": 1}, {"background": true});
db.individual.createIndex({"customAnnotationSets.id": 1, "customAnnotationSets.value": 1}, {"background": true});
db.individual.createIndex({"studyUid": 1}, {"background": true});
db.individual.createIndex({"_lastOfVersion": 1, "studyUid": 1}, {"background": true});

db.cohort.createIndex({"uuid": 1}, {"background": true});
db.cohort.createIndex({"uid": 1}, {"background": true});
db.cohort.createIndex({"id": 1, "studyUid": 1}, {"unique": true, "background": true});
db.cohort.createIndex({"type": 1, "studyUid": 1, "status.name": 1}, {"background": true});
db.cohort.createIndex({"status.name": 1, "studyUid": 1}, {"background": true});
db.cohort.createIndex({"_acl": 1, "studyUid": 1, "status.name": 1}, {"background": true});
db.cohort.createIndex({"customAnnotationSets.as": 1}, {"background": true});
db.cohort.createIndex({"customAnnotationSets.vs": 1}, {"background": true});
db.cohort.createIndex({"customAnnotationSets.id": 1, "customAnnotationSets.value": 1}, {"background": true});
db.cohort.createIndex({"studyUid": 1}, {"background": true});

db.family.createIndex({"uuid": 1, "version": 1}, {"unique": true, "background": true});
db.family.createIndex({"uid": 1, "version": 1}, {"unique": true, "background": true});
db.family.createIndex({"id": 1, "studyUid": 1, "version": 1}, {"unique": true, "background": true});
db.family.createIndex({"name": 1, "studyUid": 1}, {"background": true});
db.family.createIndex({"members.uid": 1, "studyUid": 1, "status.name": 1}, {"background": true});
db.family.createIndex({"_acl": 1, "studyUid": 1, "status.name": 1}, {"background": true});
db.family.createIndex({"status.name": 1, "studyUid": 1}, {"background": true});
db.family.createIndex({"customAnnotationSets.as": 1}, {"background": true});
db.family.createIndex({"customAnnotationSets.vs": 1}, {"background": true});
db.family.createIndex({"customAnnotationSets.id": 1, "customAnnotationSets.value": 1}, {"background": true});
db.family.createIndex({"_lastOfVersion": 1, "studyUid": 1}, {"background": true});
db.family.createIndex({"studyUid": 1}, {"background": true});

db.diseasePanel.createIndex({"uuid": 1, "version": 1}, {"unique": true, "background": true});
db.diseasePanel.createIndex({"uid": 1, "version": 1}, {"unique": true, "background": true});
db.diseasePanel.createIndex({"id": 1, "studyUid": 1, "version": 1}, {"unique": true, "background": true});
db.diseasePanel.createIndex({"_lastOfVersion": 1, "studyUid": 1}, {"background": true});
db.diseasePanel.createIndex({"studyUid": 1}, {"background": true});

// Ticket #745 - Add permission rules
function addPermissionRules(doc, changes) {
    changes["_permissionRulesApplied"] = [];

    if (typeof doc._acl !== "undefined" && doc._acl.length > 0) {
        changes["_userAcls"] = doc._acl;
    } else {
        changes["_userAcls"] = [];
    }
}

// Ticket #752 - Add private creationDate variable
function stringToDate(myString) {
    return new ISODate(myString.substring(0, 8) + " " + myString.substring(8, 14));
}

function addPrivateCreationDateAndModificationDate(doc, changes) {
    changes["_creationDate"] = stringToDate(doc.creationDate);
    // Ticket #823 - data model change
    changes["_modificationDate"] = changes["_creationDate"];
    changes["modificationDate"] = doc.creationDate;
}


// Ticket #819 - uid and uuid migration
function changeVariableIds(variables) {
    for (var i in variables) {
        var variable = variables[i];

        variable["id"] = variable["name"];
        variable["name"] = typeof variable["title"] !== "undefined" ? variable["title"] : null;

        delete variable["title"];

        if (typeof variable.variableSet !== "undefined" && variable.variableSet !== null && variable.variableSet.length > 0) {
            changeVariableIds(variable.variableSet);
        }
    }
}

function generateOpenCGAUUID(entity, date) {
    var mostSignificantBits = getMostSignificantBits(entity, date);
    var leastSignificantBits = getLeastSignificantBits();

    return base64ArrayBuffer(mostSignificantBits.toBytes().concat(leastSignificantBits.toBytes())).slice(0, 22);
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
