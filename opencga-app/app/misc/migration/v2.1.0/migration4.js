load("../utils/migrateCollection.js");

// Add Circos plot result to SampleVariantQualityControlMetrics #1730

// Remove alignmentMetrics
db.sample.update({"qualityControl.alignmentMetrics": {"$exists": true}},
    {"$unset": {"qualityControl.alignmentMetrics": ""}});


// Rename fileIds -> files
migrateCollection("sample", {"qualityControl.fileIds": {"$exists": true}}, {qualityControl: 1}, function(bulk, doc) {
    // Rename fileIds for files
    if (isNotUndefinedOrNull(doc.qualityControl)) {
        doc.qualityControl['files'] = doc.qualityControl.fileIds;
        delete doc.qualityControl['fileIds'];
        bulk.find({"_id": doc._id}).updateOne({"$set": {"qualityControl": doc.qualityControl}});
    }
});


// Remove vcfFileIds
db.sample.update({"qualityControl.variantMetrics.vcfFileIds": {"$exists": true}},
    {"$unset": {"qualityControl.variantMetrics.vcfFileIds": ""}});


// Initialise fileQualityControl
var fileQC = {
    "variant": {
    },
    "alignment": {
        "fastQcMetrics": {},
        "samtoolsStats": {},
        "samtoolsFlagStats": {},
        "hsMetrics": {}
    },
    "coverage": {
        "geneCoverageStats": []
    }
};
db.file.update({"qualityControl": {"$exists": false}}, {"$set": {"qualityControl": fileQC},
    "$unset": {"variant": "", "alignment": "", "coverage": ""}});