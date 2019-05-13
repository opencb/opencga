// #1268

migrateCollection("user", {}, {account: 1}, function(bulk, doc) {
    var changes = {};

    var account = doc.account;
    account["type"] = account.type.toUpperCase();
    account["authentication"] = {
        id: account.authOrigin,
        application: false
    };
    changes['account'] = account;

    bulk.find({"_id": doc._id}).updateOne({"$set": changes});
});