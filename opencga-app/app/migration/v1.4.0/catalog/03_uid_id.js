// #819

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


// Now we migrate to the new uids

var projectUidFqnMap = {};

// Migrate project ids and uids
migrateCollection("user", {"projects" : { $exists: true, $ne: [] }, "projects.uid": { $exists: false } }, {id: 1, projects: 1}, function(bulk, doc) {
    var finalProjects = [];
    for (var i in doc.projects) {
        var project = doc.projects[i];
        project["uid"] = project["id"];
        project["id"] = project["alias"];
        project["fqn"] = doc.id + "@" + project["alias"];
        finalProjects.push(project);

        // We populate the global map
        projectUidFqnMap[project["uid"]] = project["fqn"];
    }
    bulk.find({"_id": doc._id}).updateOne({"$set": {"projects": finalProjects}});
});

// Migrate the studies
migrateCollection("study", {"uid": { $exists: false } }, {id: 1, alias: 1, variableSets: 1, _projectId: 1}, function(bulk, doc) {
    var update = {};

    update["uid"] = doc["id"];
    update["id"] = doc["alias"];
    update["fqn"] = projectUidFqnMap[doc._projectId] + ":" + doc["alias"];
    update["_project"] = {
        "id": projectUidFqnMap[doc._projectId].split("@")[1],
        "uid": doc._projectId
    };

    // Check variableSets
    if (typeof doc.variableSets !== "undefined" && doc.variableSets.length > 0) {
        for (var i in doc.variableSets) {
            var variableSet = doc.variableSets[i];
            variableSet["uid"] = variableSet["id"];
            variableSet["id"] = variableSet["name"];

            changeVariableIds(variableSet.variables);
        }
        update["variableSets"] = doc.variableSets;
    }
    bulk.find({"_id": doc._id}).updateOne({"$set": update, "$unset": {"_projectId": "" }});
});

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

// Migrate the cohort
migrateCollection("cohort", {"uid": { $exists: false } }, {id: 1, name: 1, _studyId: 1, samples: 1}, function(bulk, doc) {

        var update = {};

        update["uid"] = doc["id"];
        update["id"] = doc["name"];
        update["studyUid"] = doc["_studyId"];

        // Check samples
        if (typeof doc.samples !== "undefined" && doc.samples.length > 0) {
            for (var i in doc.samples) {
                var sample = doc.samples[i];

                sample["uid"] = sample["id"];
                delete sample["id"];
            }

            update["samples"] = doc.samples;
        }

        bulk.find({"_id": doc._id}).updateOne({"$set": update, "$unset": {"_studyId": "" }});
    }
);

// Migrate the samples
migrateCollection("sample", {"uid": { $exists: false } }, {id: 1, name: 1, _studyId: 1}, function(bulk, doc) {
        var update = {};

        update["uid"] = doc["id"];
        update["id"] = doc["name"];
        update["studyUid"] = doc["_studyId"];

        bulk.find({"_id": doc._id}).updateOne({"$set": update, "$unset": {"_studyId": "" }});
    }
);

// Migrate the individual
migrateCollection("individual", {"uid": { $exists: false } }, {id: 1, name: 1, samples: 1, _studyId: 1, father: 1, mother: 1},
    function(bulk, doc) {
        var update = {};

        update["uid"] = doc["id"];
        update["id"] = doc["name"];
        update["studyUid"] = doc["_studyId"];

        // Check father
        if (typeof doc.father !== "undefined" && typeof doc.father.id !== "undefined") {
            update["father"] = {
                "uid": doc.father.id,
                "version": doc.father.version
            }
        }

        // Check mother
        if (typeof doc.mother !== "undefined" && typeof doc.mother.id !== "undefined") {
            update["mother"] = {
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

            update["samples"] = doc.samples;
        }

        bulk.find({"_id": doc._id}).updateOne({"$set": update, "$unset": {"_studyId": "" }});
    }
);

// Migrate the family
migrateCollection("family", {"uid": { $exists: false } }, {id: 1, name: 1, _studyId: 1, members: 1}, function(bulk, doc) {
        var update = {};

        update["uid"] = doc["id"];
        update["id"] = doc["name"];
        update["studyUid"] = doc["_studyId"];

        // Check members
        if (typeof doc.members !== "undefined" && doc.members.length > 0) {
            for (var i in doc.members) {
                var member = doc.members[i];

                member["uid"] = member["id"];
                delete member["id"];
            }

            update["members"] = doc.members;
        }

        bulk.find({"_id": doc._id}).updateOne({"$set": update, "$unset": {"_studyId": "" }});
    }
);

// Migrate the clinicalAnalysis
migrateCollection("clinical", {"uid": { $exists: false } }, {id: 1, name: 1, _studyId: 1, somatic: 1, germline: 1, subjects: 1, family: 1,
        interpretations: 1}, function(bulk, doc) {
        var update = {};

        update["uid"] = doc["id"];
        update["id"] = doc["name"];
        update["studyUid"] = doc["_studyId"];

        // Check germline file
        if (typeof doc.germline !== "undefined" && typeof doc.germline.id !== "undefined") {
            update["germline"] = {
                "uid": doc.germline.id
            };
        }

        // Check somatic file
        if (typeof doc.somatic !== "undefined" && typeof doc.somatic.id !== "undefined") {
            update["somatic"] = {
                "uid": doc.somatic.id
            };
        }

        // Check family
        if (typeof doc.family !== "undefined" && typeof doc.family.id !== "undefined") {
            update["family"] = {
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

            update["subjects"] = doc.subjects;
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

            update["interpretations"] = doc.interpretations;
        }

        bulk.find({"_id": doc._id}).updateOne({"$set": update, "$unset": {"_studyId": "" }});
    }
);

// Migrate the files
migrateCollection("file", {"uid": { $exists: false } }, {id: 1, path: 1, _studyId: 1, job: 1, experiment: 1, samples: 1}, function(bulk, doc) {
        var update = {};

        update["uid"] = doc["id"];
        update["studyUid"] = doc["_studyId"];
        update["id"] = doc["path"].replace(/\//g, ":");

        // Check samples
        if (typeof doc.samples !== "undefined" && doc.samples.length > 0) {
            for (var i in doc.samples) {
                var sample = doc.samples[i];

                sample["uid"] = sample["id"];
                delete sample["id"];
            }

            update["samples"] = doc.samples;
        }

        // Check job
        if (typeof doc.job !== "undefined" && typeof doc.job.id !== "undefined") {
            update["job"] = {
                "uid": doc.job.id
            };
        }

        // Check experiment
        if (typeof doc.experiment !== "undefined" && typeof doc.experiment.id !== "undefined") {
            update["experiment"] = {
                "uid": doc.experiment.id
            };
        }

        bulk.find({"_id": doc._id}).updateOne({"$set": update, "$unset": {"_studyId": ""}});
    }
);
