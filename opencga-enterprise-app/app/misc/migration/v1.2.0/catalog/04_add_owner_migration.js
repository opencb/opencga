// Add _ownerId to every study

var users = db.user.find({projects: {$exists: true, $ne: []}}, {"projects.id": 1});
for (var i = 0; i < users.length(); i++) {
    var user = users[i];
    var projectIds = [];
    for (var j = 0; j < user.projects.length; j++) {
        projectIds.push(user.projects[j].id);
    }

    // Add _ownerId to all the studies belonging to these projects
    db.study.update({"_projectId": {$in: projectIds}}, {$set: {_ownerId: user["_id"]}}, {multi: 1});
}