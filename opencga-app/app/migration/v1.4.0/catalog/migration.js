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

// Create Base64 Object - Extracted from https://scotch.io/tutorials/how-to-encode-and-decode-strings-with-base64-in-javascript
var Base64={_keyStr:"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=",encode:function(e){var t="";var n,r,i,s,o,u,a;var f=0;e=Base64._utf8_encode(e);while(f<e.length){n=e.charCodeAt(f++);r=e.charCodeAt(f++);i=e.charCodeAt(f++);s=n>>2;o=(n&3)<<4|r>>4;u=(r&15)<<2|i>>6;a=i&63;if(isNaN(r)){u=a=64}else if(isNaN(i)){a=64}t=t+this._keyStr.charAt(s)+this._keyStr.charAt(o)+this._keyStr.charAt(u)+this._keyStr.charAt(a)}return t},decode:function(e){var t="";var n,r,i;var s,o,u,a;var f=0;e=e.replace(/[^A-Za-z0-9+/=]/g,"");while(f<e.length){s=this._keyStr.indexOf(e.charAt(f++));o=this._keyStr.indexOf(e.charAt(f++));u=this._keyStr.indexOf(e.charAt(f++));a=this._keyStr.indexOf(e.charAt(f++));n=s<<2|o>>4;r=(o&15)<<4|u>>2;i=(u&3)<<6|a;t=t+String.fromCharCode(n);if(u!=64){t=t+String.fromCharCode(r)}if(a!=64){t=t+String.fromCharCode(i)}}t=Base64._utf8_decode(t);return t},_utf8_encode:function(e){e=e.replace(/rn/g,"n");var t="";for(var n=0;n<e.length;n++){var r=e.charCodeAt(n);if(r<128){t+=String.fromCharCode(r)}else if(r>127&&r<2048){t+=String.fromCharCode(r>>6|192);t+=String.fromCharCode(r&63|128)}else{t+=String.fromCharCode(r>>12|224);t+=String.fromCharCode(r>>6&63|128);t+=String.fromCharCode(r&63|128)}}return t},_utf8_decode:function(e){var t="";var n=0;var r=c1=c2=0;while(n<e.length){r=e.charCodeAt(n);if(r<128){t+=String.fromCharCode(r);n++}else if(r>191&&r<224){c2=e.charCodeAt(n+1);t+=String.fromCharCode((r&31)<<6|c2&63);n+=2}else{c2=e.charCodeAt(n+1);c3=e.charCodeAt(n+2);t+=String.fromCharCode((r&15)<<12|(c2&63)<<6|c3&63);n+=3}}return t}}




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
            "uid": doc.family.id,
            "version": doc.family.version
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
    setChanges["uuid"] = generateOpenCGAUUID("FAMILY", setChanges["_creationDate"]);

    unsetChanges["_studyId"] = "";

    // Check members
    if (typeof doc.members !== "undefined" && doc.members.length > 0) {
        for (var i in doc.members) {
            var member = doc.members[i];

            member["uid"] = member["id"];
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
    var setChanges = {};
    var unsetChanges = {};
    addPermissionRules(doc, setChanges);
    addPrivateCreationDateAndModificationDate(doc, setChanges);

    /* uid and uuid migration: #819 */
    setChanges["uid"] = doc["id"];
    setChanges["id"] = doc["name"];
    setChanges["studyUid"] = doc["_studyId"];
    setChanges["uuid"] = generateOpenCGAUUID("INDIVIDUAL", setChanges["_creationDate"]);

    unsetChanges["_studyId"] = "";

    // Check father
    if (typeof doc.father !== "undefined" && typeof doc.father.id !== "undefined") {
        setChanges["father"] = {
            "uid": doc.father.id,
            "version": doc.father.version
        }
    }

    // Check mother
    if (typeof doc.mother !== "undefined" && typeof doc.mother.id !== "undefined") {
        setChanges["mother"] = {
            "uid": doc.mother.id,
            "version": doc.mother.version
        }
    }

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
            "uid": doc.outDir.id,
            "version": doc.outDir.version
        }
    }

    // Check stdOutput
    if (typeof doc.stdOutput !== "undefined" && typeof doc.stdOutput.id !== "undefined") {
        setChanges["stdOutput"] = {
            "uid": doc.stdOutput.id,
            "version": doc.stdOutput.version
        }
    }

    // Check stdError
    if (typeof doc.stdError !== "undefined" && typeof doc.stdError.id !== "undefined") {
        setChanges["stdError"] = {
            "uid": doc.stdError.id,
            "version": doc.stdError.version
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
    setChanges["studyUid"] = doc["_studyId"];
    setChanges["uuid"] = generateOpenCGAUUID("SAMPLE", setChanges["_creationDate"]);

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
    var mostSignificantBits = padLeft(getMostSignificantBits(entity, date).toString(16), 16);
    var leastSignificantBits = padLeft(getLeastSignificantBits().toString(16), 16);

    var uuid = mostSignificantBits.slice(0, 8) + "-" + mostSignificantBits.slice(8, 12) + "-" + mostSignificantBits.slice(12, 16) + "-"
        + leastSignificantBits.slice(0, 4) + "-" + leastSignificantBits.slice(4, 16);

    return Base64.encode(uuid);
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

function padLeft(hexString, length) {
    if (hexString.length === length) {
        return hexString
    }

    var tmpString = hexString;
    for (var i = hexString.length; i < length; i++) {
        tmpString = "0" + tmpString;
    }
    return tmpString;
}