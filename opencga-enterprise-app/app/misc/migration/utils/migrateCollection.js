function migrateCollectionDifferentCollection(inputCollection, outputCollection, query, projection, migrateFunc) {
    var bulk = db.getCollection(outputCollection).initializeOrderedBulkOp();
    var count = 0;
    var bulkSize = 1000;
    db.getCollection(inputCollection).find(query,projection).forEach(function(doc) {
        migrateFunc(bulk, doc);
        if ( bulk.nUpdateOps + bulk.nInsertOps + bulk.nRemoveOps >= bulkSize ) {
            count += bulk.nUpdateOps + bulk.nInsertOps + bulk.nRemoveOps;
            print("Execute bulk! " + count);
            bulk.execute();
            bulk = db.getCollection(outputCollection).initializeOrderedBulkOp();
        }
    });

    if ( bulk.nUpdateOps + bulk.nInsertOps + bulk.nRemoveOps > 0 ) {
        count += bulk.nUpdateOps + bulk.nInsertOps + bulk.nRemoveOps;
        print("Execute bulk! " + count);
        bulk.execute();
        bulk = db.getCollection(outputCollection).initializeOrderedBulkOp();
    }

    if (count == 0) {
        print("Nothing to do!");
    }
}

function migrateCollection(collection, query, projection, migrateFunc) {
    migrateCollectionDifferentCollection(collection, collection, query, projection, migrateFunc);
}

function isNotUndefinedOrNull(obj) {
    return typeof obj !== 'undefined' && obj !== null;
}

function isUndefinedOrNull(obj) {
    return typeof obj === 'undefined' || obj === null;
}

function isEmpty(obj) {
    if (typeof obj === "undefined" || obj === null) {
        return true;
    }

    // obj is an actual Object
    if (typeof obj === "object") {
        for(let key in obj) {
            if(obj.hasOwnProperty(key))
                return false;
        }
        return true;
    } else {
        // obj is a String
        if (typeof obj === "string") {
            return obj === "";
        }
    }
}

function isNotEmpty(obj) {
    return !this.isEmpty(obj);
}

function isEmptyArray(arr) {
    return typeof arr !== 'undefined' && arr !== null && arr.length === 0;
}

function isNotEmptyArray(arr) {
    return typeof arr !== 'undefined' && arr !== null && arr.length > 0;
}

// Auxiliary methods to write in metadata the latest update run to avoid running the same thing again
var version = undefined;
var updateCount = 1;

function setOpenCGAVersion(updateVersion, updateVersionInt, updateRelease) {
    var toset = {
        "version": updateVersion,
        "_fullVersion.version": NumberInt(updateVersionInt),
        "_fullVersion.release": NumberInt(updateRelease),
        "_fullVersion.lastJsUpdate": NumberInt(updateCount)
    }

    if (version.version < updateVersionInt) {
        // Also reset Java updates to 0 because we are doing a version upgrade
        toset["_fullVersion.lastJavaUpdate"] = NumberInt(0);
    }

    db.metadata.update({}, {"$set": toset});
}

function setLatestUpdate(latestUpdate) {
    db.metadata.update({}, {"$set": {"_fullVersion.lastJsUpdate": NumberInt(latestUpdate)}});
}

function getOpenCGAVersion() {
    if (typeof version === "undefined") {
        var metadata = db.metadata.findOne({}, {"_fullVersion": 1, "_latestUpdate": 1});
        if (isNotUndefinedOrNull(metadata._fullVersion)) {
            version = metadata._fullVersion;
        } else {
            version = {
                'version': 20000,
                'release': 4,
                'lastJsUpdate': 0
            };
            if (typeof metadata._latestUpdate !== "undefined") {
                version['lastJsUpdate'] = metadata._latestUpdate;
            }
        }
    }
    return version;
}

function versionNeedsUpdate(updateVersion, updateRelease) {
    var dbVersion = getOpenCGAVersion();
    var needsUpdate = dbVersion.version < updateVersion || (dbVersion.version == updateVersion && dbVersion.release <= updateRelease);
    var needsJsReset = dbVersion.version < updateVersion || (dbVersion.version == updateVersion && dbVersion.release < updateRelease);
    if (needsJsReset) {
        // Reset JS counter
        version.lastJsUpdate = 0;
    }
    if (needsUpdate) {
        print("Migrating from " + dbVersion.version + " release " + dbVersion.release + " to " + updateVersion + " release " + updateRelease);
    } else {
        print("Nothing to migrate. Current db version is " + dbVersion.version + " with release " + dbVersion.release);
    }
    return needsUpdate;
}

function getLatestUpdate() {
    return version.lastJsUpdate;
}

function runUpdate(migrateFunction, message) {
    var text = updateCount;
    if (isNotEmpty(message)) {
        text = text + " (" + message + ")";
    }

    if (getLatestUpdate() < updateCount) {
        print("Starting migration " + text + "...");
        migrateFunction();
        setLatestUpdate(updateCount);
    } else {
        print("Skipping migration " + text + "...");
    }
    updateCount++;
}
