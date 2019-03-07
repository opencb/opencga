migrateCollection("clinical", {"roleToProband" : { $exists: false } }, {family: 1, proband: 1}, function(bulk, doc) {
    var roleToProband = {};
    if (typeof doc.proband !== "undefined" && typeof doc.proband.id === "string" && doc.proband.id.length > 0) {
        roleToProband[doc.proband.id] = "PROBAND";

        var motherId = undefined;
        var fatherId = undefined;
        if (typeof doc.proband.father !== "undefined" && typeof doc.proband.father.id === "string" && doc.proband.father.id.length > 0) {
            fatherId = doc.proband.father.id;
            roleToProband[fatherId] = "FATHER";
        }
        if (typeof doc.proband.mother !== "undefined" && typeof doc.proband.mother.id === "string" && doc.proband.mother.id.length > 0) {
            motherId = doc.proband.mother.id;
            roleToProband[motherId] = "MOTHER";
        }
        if (typeof motherId !== "undefined" && typeof fatherId !== "undefined" && typeof doc.family !== "undefined") {
            for (var i = 0; i < doc.family.members.length; i++) {
                var member = doc.family.members[i];
                if (!(member.id in roleToProband)) {
                    // We look for possible brothers or sisters
                    if (typeof member.father !== "undefined" && member.father.id === fatherId && typeof member.mother !== "undefined"
                        && member.mother.id === motherId) {
                        if (member.sex === "MALE") {
                            roleToProband[member.id] = "FULL_SIBLING_M";
                        } else if (member.sex === "FEMALE") {
                            roleToProband[member.id] = "FULL_SIBLING_F";
                        } else {
                            roleToProband[member.id] = "FULL_SIBLING";
                        }
                    } else {
                        roleToProband[member.id] = "UNKNOWN";
                    }
                }
            }
        }
    }

    bulk.find({"_id": doc._id}).updateOne({"$set": {"roleToProband": roleToProband}});
});