// ------------------  Store uuids as actual uuids instead of applying base64 over them
var Entity = Object.freeze({"AUDIT": 0, "PROJECT": 1, "STUDY": 2, "FILE": 3, "SAMPLE": 4, "COHORT": 5, "INDIVIDUAL": 6, "FAMILY": 7,
    "JOB": 8, "CLINICAL": 9, "PANEL": 10, "INTERPRETATION": 11});

function generateOpenCGAUUID(entity, date) {
    var mostSignificantBits = getMostSignificantBits(entity, date);
    var leastSignificantBits = getLeastSignificantBits();
    return _generateUUID(mostSignificantBits, leastSignificantBits);
}

function _generateUUID(mostSigBits, leastSigBits) {
    return (digits(mostSigBits.shiftRightUnsigned(32), 8) + "-" +
        digits(mostSigBits.shiftRightUnsigned(16), 4) + "-" +
        digits(mostSigBits, 4) + "-" +
        digits(leastSigBits.shiftRightUnsigned(48), 4) + "-" +
        digits(leastSigBits, 12));
}

function digits(value, digits) {
    hi = Long.fromNumber(1).shiftLeft(digits * 4).toUnsigned();
    return hi.or(value.and(hi - 1)).toString(16).substring(1);
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




// Remove null values of jobId
db.file.update({"jobId": null}, {"$set": {"jobId": ""}});
db.job.find({}, {id:1, output:1, stdout:1, stderr:1}).forEach(function(job) {
    fileUids = [];
    if (isNotEmptyArray(job.output)) {
        for (file of job.output) {
            fileUids.push(file.uid);
        }
    }
    if (isNotUndefinedOrNull(job.stdout) && isNotUndefinedOrNull(job.stdout.uid)) {
        fileUids.push(job.stdout.uid);
    }
    if (isNotUndefinedOrNull(job.stderr) && isNotUndefinedOrNull(job.stderr.uid)) {
        fileUids.push(job.stderr.uid);
    }

    // Update file documents
    print(job.id + ": " + fileUids);
    db.file.update({"uid": {"$in": fileUids}}, {"$set": {"jobId": "caca"}}, {"multi": true});
});

// Fix clinical issue where member ids were not populated
function _getMemberId(memberUidToIdMap, memberUid) {
    if (memberUid in memberUidToIdMap) {
        return memberUidToIdMap[memberUid];
    } else {
        // If it is not in the map, we find it and store in map
        var memberId = db.individual.findOne({uid: memberUid}, {id: 1})['id'];
        memberUidToIdMap[memberUid] = memberId;
        return memberId;
    }
}

migrateCollection("clinical", {}, {}, function(bulk, doc) {
    var memberUidToIdMap = {};
    if (isNotUndefinedOrNull(doc.family)) {
        if (isNotEmptyArray(doc.family.members)) {
            doc.family.members.forEach(function(member) {
                memberUidToIdMap[member['uid']] = member['id'];
            });

            doc.family.members.forEach(function(member) {
                if (isNotUndefinedOrNull(member.father) && isNotUndefinedOrNull(member.father.uid)) {
                    member['father']['id'] = _getMemberId(memberUidToIdMap, member['father']['uid']);
                }
                if (isNotUndefinedOrNull(member.mother) && isNotUndefinedOrNull(member.mother.uid)) {
                    member['mother']['id'] = _getMemberId(memberUidToIdMap, member['mother']['uid']);
                }
            });
        }
    }
    if (isNotUndefinedOrNull(doc.proband)) {
        if (isNotUndefinedOrNull(doc.proband.father) && isNotUndefinedOrNull(doc.proband.father.uid)) {
            doc['proband']['father']['id'] = _getMemberId(memberUidToIdMap, doc['proband']['father']['uid']);
        }
        if (isNotUndefinedOrNull(doc.proband.mother) && isNotUndefinedOrNull(doc.proband.mother.uid)) {
            doc['proband']['mother']['id'] = _getMemberId(memberUidToIdMap, doc['proband']['mother']['uid']);
        }
    }

    bulk.find({"_id": doc._id}).updateOne({"$set": {
            'proband': doc.proband,
            'family': doc.family
        }
    });
});

// Set all _individualUid to NumberLong(-1) from sample collection
db.sample.update({_individualUid:-1},{$set:{_individualUid:NumberLong(-1)}},{multi:true})


// Remove proband and family references in clinical analysis #1625
function _getSampleIdReferences(sample) {
    if (isUndefinedOrNull(sample)) {
        return sample;
    }
    return {
        'uid': sample['uid'],
        'id': sample['id']
    }
}

function _getMemberAndSampleIdReferences(member, keepVersion) {
    if (isUndefinedOrNull(member)) {
        return member;
    }
    var newMember = {
        'uid': member['uid'],
        'id': member['id'],
        'samples': member['samples']
    };
    if (keepVersion) {
        newMember['version'] = NumberInt(member['version']);
    }

    if (isNotEmptyArray(member.samples)) {
        var samples = [];
        for (var sample of member.samples) {
            samples.push(_getSampleIdReferences(sample));
        }
        newMember['samples'] = samples;
    }

    return newMember;
}

migrateCollection("clinical", {}, {proband: 1, family: 1}, function(bulk, doc) {
    var toset = {};

    if (isNotUndefinedOrNull(doc.family)) {
        var family = {
            'uid': doc.family['uid'],
            'id': doc.family['id'],
            'version': NumberInt(doc.family['version']),
            'members': doc.family['members']
        };
        if (isNotEmptyArray(doc.family.members)) {
            var members = [];
            for (var member of doc.family.members) {
                members.push(_getMemberAndSampleIdReferences(member, false));
            }
            family['members'] = members;
        }

        toset['family'] = family;
    }

    if (isNotUndefinedOrNull(doc.proband)) {
        toset['proband'] = _getMemberAndSampleIdReferences(doc.proband, true);
    }

    bulk.find({"_id": doc._id}).updateOne({"$set": toset});
});


// #1629 - Fix fileIds reference
var sampleFileMap = {};
db.file.find({samples:{$ne:[]}}, {id:1, samples:1}).forEach(function(doc) {
    doc.samples.forEach(function(sample) {
        var sampleUid = Number(sample['uid']);
        if (!(sampleUid in sampleFileMap)) {
            sampleFileMap[sampleUid] = [];
        }
        sampleFileMap[sampleUid].push(doc.id);
    });
});

migrateCollection("sample", {}, {}, function(bulk, doc) {
    var sampleUid = Number(doc['uid']);
    if (sampleUid in sampleFileMap) {
        var set = {
            'fileIds': sampleFileMap[sampleUid]
        };
        bulk.find({"_id": doc._id}).updateOne({"$set": set});
    }
});

// Add new Variant permission
migrateCollection("study", {}, {}, function(bulk, doc) {
    if (isNotEmptyArray(doc._acl)) {
        var acls = new Set();
        for (var acl of doc._acl) {
            if (acl.endsWith("VIEW_SAMPLES")) {
                var member = acl.split("__VIEW_SAMPLES")[0];
                acls.add(member + "__VIEW_AGGREGATED_VARIANTS");
                acls.add(member + "__VIEW_SAMPLE_VARIANTS");
            }
            acls.add(acl);
        }
        bulk.find({"_id": doc._id}).updateOne({"$set": {"_acl": Array.from(acls)}});
    }
});

migrateCollection("sample", {}, {}, function(bulk, doc) {
    if (isNotEmptyArray(doc._acl)) {
        var acls = new Set();
        for (var acl of doc._acl) {
            if (acl.endsWith("__VIEW")) {
                var member = acl.split("__VIEW")[0];
                acls.add(member + "__VIEW_VARIANTS");
            }
            acls.add(acl);
        }
        bulk.find({"_id": doc._id}).updateOne({"$set": {"_acl": Array.from(acls)}});
    }
});


// Create default interpretations for all clinical analyses
function _createNewInterpretation(clinical) {
    var newUid = db.metadata.findAndModify({
        query: { },
        update: { $inc: { idCounter: NumberLong(1) } }
    })['idCounter'];

    var interpretation = {
        'id': clinical['id'] + "_1",
        'uuid': generateOpenCGAUUID("INTERPRETATION", clinical['_creationDate']),
        'description': '',
        'clinicalAnalysisId': clinical['id'],
        'analyst': {

        },
        'methods': [{

        }],
        'primaryFindings': [],
        'secondaryFindings': [],
        'comments': [],
        'status': '',
        'creationDate': clinical['creationDate'],
        'modificationDate': clinical['modificationDate'],
        'version': 1,
        'attributes': {},
        'studyUid': clinical['studyUid'],
        'uid': NumberLong(newUid),
        'internal': {
            'status': {
                'name': 'NOT_REVIEWED',
                'date': clinical['creationDate'],
                'description': ''
            }
        },
        '_creationDate': clinical['_creationDate'],
        '_modificationDate': clinical['_creationDate']
    }

    db.interpretation.insert(interpretation);
    return interpretation;
}


migrateCollection("clinical", {}, {'id': 1, 'studyUid': 1, '_creationDate': 1, 'creationDate': 1}, function(bulk, doc) {
    if (isUndefinedOrNull(doc.interpretation)) {
        var interpretationUid = _createNewInterpretation(doc)['uid'];
        doc['interpretation'] = {
            'uid': interpretationUid
        };

        var toset = {
            'interpretation': doc.interpretation
        };

        bulk.find({"_id": doc._id}).updateOne({"$set": toset});
    }
});

// Delete roleToProband and qualityControl from Clinical Analysis
db.clinical.update({}, {"$unset": {
        "qualityControl": "",
        "roleToProband": ""
    }}
);

migrateCollection("interpretation", {"modificationDate": {"$exists": false}}, {'creationDate': 1}, function(bulk, doc) {
    bulk.find({"_id": doc._id}).updateOne({"$set": { "modificationDate": doc.creationDate }});
});


print("\nFixing user indexes...")
db.user.createIndex({"projects.uid": 1, "id": 1}, {"unique": true, "background": true});
db.user.createIndex({"projects.uuid": 1, "id": 1}, {"unique": true, "background": true});
db.user.createIndex({"projects.fqn": 1, "id": 1}, {"unique": true, "background": true});
db.user.dropIndex("projects.uid_1");
db.user.dropIndex("projects.uuid_1");
db.user.dropIndex("projects.fqn_1");

print("\nFixing clinical indexes...")
db.clinical.createIndex({"proband.uid": 1, "studyUid": 1}, {"background": true});
db.clinical.createIndex({"family.members.uid": 1, "studyUid": 1}, {"background": true});
db.clinical.createIndex({"proband.samples.uid": 1, "studyUid": 1}, {"background": true});
db.clinical.createIndex({"family.members.samples.uid": 1, "studyUid": 1}, {"background": true});
db.clinical.createIndex({"family.uid": 1, "studyUid": 1}, {"background": true});