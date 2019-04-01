migrateCollection("clinical", {"roleToProband" : { $exists: false } }, {family: 1, proband: 1}, function(bulk, doc) {
    var roleToProband = {};
    if (isNotUndefinedOrNull(doc.proband) && isNotEmpty(doc.proband.id)) {
        roleToProband[doc.proband.id] = "PROBAND";

        var motherId = undefined;
        var fatherId = undefined;
        if (isNotUndefinedOrNull(doc.proband.father) && isNotEmpty(doc.proband.father.id)) {
            fatherId = doc.proband.father.id;
            roleToProband[fatherId] = "FATHER";
        }
        if (isNotUndefinedOrNull(doc.proband.mother) && isNotEmpty(doc.proband.mother.id)) {
            motherId = doc.proband.mother.id;
            roleToProband[motherId] = "MOTHER";
        }
        // if (typeof motherId !== "undefined" && typeof fatherId !== "undefined" && typeof doc.family !== "undefined") {
        if (isNotUndefinedOrNull(doc.family) && isNotEmptyArray(doc.family.members)) {
            for (var i = 0; i < doc.family.members.length; i++) {
                var member = doc.family.members[i];
                if (!(member.id in roleToProband)) {
                    // We look for possible brothers or sisters
                    if (isNotUndefinedOrNull(member.father) && member.father.id === fatherId && isNotUndefinedOrNull(member.mother)
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