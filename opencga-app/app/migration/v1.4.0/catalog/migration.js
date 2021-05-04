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


function haveInternalPermissions(studyId, aclList, userMap) {
    var allUsers = [];
    var usersWithView = [];

    if (!(studyId.valueOf().toString() in userMap)) {
        userMap[studyId.valueOf().toString()] = [];
    }
    var userList = userMap[studyId.valueOf().toString()];

    for (var i in aclList) {
        var split = aclList[i].split("__");
        if (allUsers.indexOf(split[0]) === -1) {
            allUsers.push(split[0]);
        }
        if (split[1] === "VIEW") {
            usersWithView.push(split[0]);
        }
    }

    for (var i in usersWithView) {
        var index = allUsers.indexOf(usersWithView[i]);
        if (index > -1) {
            allUsers.splice(index, 1);
        }
    }

    for (var i in allUsers) {
        var index = userList.indexOf(allUsers[i]);
        if (index === -1) {
            userList.push(allUsers[i]);
        }
    }
}

function updateStudyPermissions(studyUserMap, entity) {
    for (var key in studyUserMap) {
        var userList = studyUserMap[key];
        if (userList.length > 0) {
            for (var i in userList) {
                var myObject = {};
                myObject["_withInternalAcls." + userList[i]] = entity;
                db.study.update({"uid": NumberLong(key)}, {"$addToSet": myObject});
                db.study.update({"id": NumberLong(key)}, {"$addToSet": myObject});
            }
        }
    }
}

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
    if (isNotUndefinedOrNull(doc.germline) && isNotUndefinedOrNull(doc.germline.id)) {
        setChanges["germline"] = {
            "uid": doc.germline.id
        };
    }

    // Check somatic file
    if (isNotUndefinedOrNull(doc.somatic) && isNotUndefinedOrNull(doc.somatic.id)) {
        setChanges["somatic"] = {
            "uid": doc.somatic.id
        };
    }

    // Check family
    if (isNotUndefinedOrNull(doc.family) && isNotUndefinedOrNull(doc.family.id)) {
        setChanges["family"] = {
            "uid": doc.family.id
        };
    }

    // Check subjects
    if (isNotUndefinedOrNull(doc.subjects) && doc.subjects.length > 0) {
        for (var i in doc.subjects) {
            var member = doc.subjects[i];

            member["uid"] = member["id"];
            delete member["id"];
        }

        setChanges["subjects"] = doc.subjects;
    }

    // Check clinical interpretations
    if (isNotUndefinedOrNull(doc.interpretations) && doc.interpretations.length > 0) {
        for (var i in doc.interpretations) {
            var interpretation = doc.interpretations[i];
            if (isNotUndefinedOrNull(interpretation.file) && isNotUndefinedOrNull(interpretation.file.id)) {
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
var studyUserMap = {};
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
    if (isNotUndefinedOrNull(doc.samples) && doc.samples.length > 0) {
        for (var i in doc.samples) {
            var sample = doc.samples[i];

            sample["uid"] = sample["id"];
            delete sample["id"];
        }
        setChanges["samples"] = doc.samples;
    }
    /* end uid and uuid migration: #819 */

    haveInternalPermissions(doc._studyId, doc._acl, studyUserMap);

    bulk.find({"_id": doc._id}).updateOne({"$set": setChanges, "$unset": unsetChanges});
});
updateStudyPermissions(studyUserMap, "COHORT");


print("\nMigrating family");
var studyUserMap = {};
migrateCollection("family", {"uuid": {$exists: false}}, {attributes: 0, annotationSets: 0}, function(bulk, doc) {
    var setChanges = {
        "expectedSize": -1 // #823
    };
    var unsetChanges = {};

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
    if (isNotUndefinedOrNull(doc.members) && doc.members.length > 0) {
        for (var i in doc.members) {
            var member = doc.members[i];

            member["uid"] = member["id"];
            member["version"] = NumberInt(member["version"]);
            delete member["id"];
        }

        setChanges["members"] = doc.members;
    }
    /* end uid and uuid migration: #819 */

    haveInternalPermissions(doc._studyId, doc._acl, studyUserMap);

    bulk.find({"_id": doc._id}).updateOne({"$set": setChanges, "$unset": unsetChanges});
});
updateStudyPermissions(studyUserMap, "FAMILY");


print("\nMigrating file");
var studyUserMap = {};
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
    if (isNotUndefinedOrNull(doc.samples) && doc.samples.length > 0) {
        for (var i in doc.samples) {
            var sample = doc.samples[i];

            sample["uid"] = sample["id"];
            delete sample["id"];
        }

        setChanges["samples"] = doc.samples;
    }

    // Check job
    if (isNotUndefinedOrNull(doc.job) && isNotUndefinedOrNull(doc.job.id)) {
        setChanges["job"] = {
            "uid": doc.job.id
        };
    }

    // Check experiment
    if (isNotUndefinedOrNull(doc.experiment) && isNotUndefinedOrNull(doc.experiment.id)) {
        setChanges["experiment"] = {
            "uid": doc.experiment.id
        };
    }
    /* end uid and uuid migration: #819 */

    // Check related files
    if (isNotEmptyArray(doc.relatedFiles)) {
        var relatedFiles = [];

        for (var i = 0; i < doc.relatedFiles.length; i++) {
            relatedFiles.push({
                "relation": doc.relatedFiles[i].relation,
                "file": {
                    "id": doc.relatedFiles[i].fileId,
                }
            });
        }

        setChanges["relatedFiles"] = relatedFiles;
    }

    haveInternalPermissions(doc._studyId, doc._acl, studyUserMap);

    bulk.find({"_id": doc._id}).updateOne({"$set": setChanges, "$unset": unsetChanges});
});
updateStudyPermissions(studyUserMap, "FILE");


print("\nMigrating individual");
var studyUserMap = {};
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
    if (isNotUndefinedOrNull(doc.father) && isNotUndefinedOrNull(doc.father.id)) {
        setChanges["father"] = {
            "uid": doc.father.id
        }
    }

    // Check mother
    if (isNotUndefinedOrNull(doc.mother) && isNotUndefinedOrNull(doc.mother.id)) {
        setChanges["mother"] = {
            "uid": doc.mother.id
        }
    }

    // Check samples
    if (isNotUndefinedOrNull(doc.samples) && doc.samples.length > 0) {
        for (var i in doc.samples) {
            var sample = doc.samples[i];

            sample["uid"] = sample["id"];
            sample["version"] = NumberInt(sample["version"]);
            delete sample["id"];
        }

        setChanges["samples"] = doc.samples;
    }
    /* end uid and uuid migration: #819 */

    haveInternalPermissions(doc._studyId, doc._acl, studyUserMap);

    bulk.find({"_id": doc._id}).updateOne({"$set": setChanges, "$unset": unsetChanges});
});
updateStudyPermissions(studyUserMap, "INDIVIDUAL");


print("\nMigrating job");
var studyUserMap = {};
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
    if (isNotUndefinedOrNull(doc.input) && doc.input.length > 0) {
        for (var i in doc.input) {
            var input = doc.input[i];

            input["uid"] = input["id"];
            delete input["id"];
        }

        setChanges["input"] = doc.input;
    }

    // Check output
    if (isNotUndefinedOrNull(doc.output) && doc.output.length > 0) {
        for (var i in doc.output) {
            var output = doc.output[i];

            output["uid"] = output["id"];
            delete output["id"];
        }

        setChanges["output"] = doc.output;
    }

    // Check outDir
    if (isNotUndefinedOrNull(doc.outDir) && isNotUndefinedOrNull(doc.outDir.id)) {
        setChanges["outDir"] = {
            "uid": doc.outDir.id
        }
    }

    // Check stdOutput
    if (isNotUndefinedOrNull(doc.stdOutput) && isNotUndefinedOrNull(doc.stdOutput.id)) {
        setChanges["stdOutput"] = {
            "uid": doc.stdOutput.id
        }
    }

    // Check stdError
    if (isNotUndefinedOrNull(doc.stdError) && isNotUndefinedOrNull(doc.stdError.id)) {
        setChanges["stdError"] = {
            "uid": doc.stdError.id
        }
    }
    /* end uid and uuid migration: #819 */

    haveInternalPermissions(doc._studyId, doc._acl, studyUserMap);

    bulk.find({"_id": doc._id}).updateOne({"$set": setChanges, "$unset": unsetChanges});
});
updateStudyPermissions(studyUserMap, "JOB");


print("\nMigrating sample");
var studyUserMap = {};
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

    haveInternalPermissions(doc._studyId, doc._acl, studyUserMap);

    bulk.find({"_id": doc._id}).updateOne({"$set": setChanges, "$unset": unsetChanges});
});
updateStudyPermissions(studyUserMap, "SAMPLE");


// Global variable populated by the migrateCollection("user"... and used in migrateCollection("study"...
var projectUidFqnMap = {};

print("\nMigrating user");
migrateCollection("user", {}, {attributes: 0}, function(bulk, doc) {
    var changes = {};

    if (isNotUndefinedOrNull(doc.projects) && doc.projects.length > 0 && isUndefinedOrNull(doc.projects[0].uid)) {
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
    }

    // #1268
    var account = doc.account;
    account["type"] = account.type.toUpperCase();
    account["authentication"] = {
        id: account.authOrigin,
        application: false
    };
    changes['account'] = account;

    if (Object.keys(changes).length > 0) {
        bulk.find({"_id": doc._id}).updateOne({"$set": changes});
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
    if (isNotUndefinedOrNull(doc.variableSets) && doc.variableSets.length > 0) {
        for (var i in doc.variableSets) {
            var variableSet = doc.variableSets[i];
            variableSet["uid"] = variableSet["id"];
            variableSet["id"] = variableSet["name"];
            variableSet["entities"] = [];

            changeVariableIds(variableSet.variables);
        }
        setChanges["variableSets"] = doc.variableSets;
    }
    /* end uid and uuid migration: #819 */

    // Add 'id' to groups
    var groups = doc["groups"];
    for (var i in groups) {
        var group = groups[i];
        if (!group.hasOwnProperty('id')) {
            group['id'] = group['name'];
        }
        // Add owner to @members group
        if (group['id'] === "@members") {
            var owner = projectUidFqnMap[doc._projectId].split("@")[0];
            if (group['userIds'].indexOf(owner) < 0) {
                group['userIds'].push(owner);
            }
        }
    }
    setChanges["groups"] = groups;

    bulk.find({"_id": doc._id}).updateOne({"$set": setChanges, "$unset": unsetChanges});
});
print("");

// Update metadata version
db.metadata.update({}, {"$set": {"version": "1.4.0"}});

print("\nCreating new indexes...")
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

db.panel.createIndex({"uuid": 1, "version": 1}, {"unique": true, "background": true});
db.panel.createIndex({"uid": 1, "version": 1}, {"unique": true, "background": true});
db.panel.createIndex({"id": 1, "studyUid": 1, "version": 1}, {"unique": true, "background": true});
db.panel.createIndex({"_lastOfVersion": 1, "studyUid": 1}, {"background": true});
db.panel.createIndex({"studyUid": 1}, {"background": true});

db.clinical.createIndex({"uuid": 1}, {"background": true});
db.clinical.createIndex({"uid": 1}, {"background": true});
db.clinical.createIndex({"id": 1, "studyUid": 1}, {"unique": true, "background": true});
db.clinical.createIndex({"type": 1, "studyUid": 1, "status.name": 1}, {"background": true});
db.clinical.createIndex({"status.name": 1, "studyUid": 1}, {"background": true});
db.clinical.createIndex({"_acl": 1, "studyUid": 1, "status.name": 1}, {"background": true});
db.clinical.createIndex({"_creationDate": 1, "studyUid": 1, "status.name": 1}, {"background": true});
db.clinical.createIndex({"_modificationDate": 1, "studyUid": 1, "status.name": 1}, {"background": true});
db.clinical.createIndex({"dueDate": 1, "studyUid": 1, "status.name": 1}, {"background": true});
db.clinical.createIndex({"priority": 1, "studyUid": 1, "status.name": 1}, {"background": true});
db.clinical.createIndex({"flags": 1, "studyUid": 1, "status.name": 1}, {"background": true});
db.clinical.createIndex({"studyUid": 1}, {"background": true});

db.interpretation.createIndex({"uuid": 1}, {"background": true});
db.interpretation.createIndex({"uid": 1}, {"background": true});
db.interpretation.createIndex({"id": 1, "studyUid": 1}, {"unique": true, "background": true});
db.interpretation.createIndex({"status": 1, "studyUid": 1}, {"background": true});
db.interpretation.createIndex({"_creationDate": 1, "studyUid": 1, "status.name": 1}, {"background": true});
db.interpretation.createIndex({"_modificationDate": 1, "studyUid": 1, "status.name": 1}, {"background": true});
db.interpretation.createIndex({"studyUid": 1}, {"background": true});

// #912
db.job.createIndex({"_creationDate": 1, "studyUid": 1, "status.name": 1}, {"background": true});
db.job.createIndex({"_modificationDate": 1, "studyUid": 1, "status.name": 1}, {"background": true});

db.file.createIndex({"_creationDate": 1, "studyUid": 1, "status.name": 1}, {"background": true});
db.file.createIndex({"_modificationDate": 1, "studyUid": 1, "status.name": 1}, {"background": true});

db.sample.createIndex({"_creationDate": 1, "studyUid": 1, "status.name": 1}, {"background": true});
db.sample.createIndex({"_modificationDate": 1, "studyUid": 1, "status.name": 1}, {"background": true});

db.individual.createIndex({"_creationDate": 1, "studyUid": 1, "status.name": 1}, {"background": true});
db.individual.createIndex({"_modificationDate": 1, "studyUid": 1, "status.name": 1}, {"background": true});

db.cohort.createIndex({"_creationDate": 1, "studyUid": 1, "status.name": 1}, {"background": true});
db.cohort.createIndex({"_modificationDate": 1, "studyUid": 1, "status.name": 1}, {"background": true});

db.family.createIndex({"_creationDate": 1, "studyUid": 1, "status.name": 1}, {"background": true});
db.family.createIndex({"_modificationDate": 1, "studyUid": 1, "status.name": 1}, {"background": true});

db.panel.createIndex({"_creationDate": 1, "studyUid": 1, "status.name": 1}, {"background": true});
db.panel.createIndex({"_modificationDate": 1, "studyUid": 1, "status.name": 1}, {"background": true});

// Ticket #745 - Add permission rules
function addPermissionRules(doc, changes) {
    changes["_permissionRulesApplied"] = [];

    if (isNotUndefinedOrNull(doc._acl) && doc._acl.length > 0) {
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
        variable["name"] = isNotUndefinedOrNull(variable["title"]) ? variable["title"] : null;

        delete variable["title"];

        if (isNotUndefinedOrNull(variable.variableSet) && variable.variableSet.length > 0) {
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
