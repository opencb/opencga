// Remove sessions array from every user entry. #618

db.user.update({}, {$unset: {sessions: ""}}, {multi: 1});
db.metadata.update({}, {$unset: {"admin.sessions": ""}, $set: {"admin.secretKey": "dummy", "admin.algorithm": "HS256"}});