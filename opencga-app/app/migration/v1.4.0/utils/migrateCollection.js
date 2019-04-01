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