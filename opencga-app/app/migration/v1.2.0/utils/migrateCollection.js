function migrateCollectionDifferentCollection(inputCollection, outputCollection, query, projection, migrateFunc) {
    var bulk = db.getCollection(outputCollection).initializeOrderedBulkOp();
    var count = 0;
    var bulkSize = 500;
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
