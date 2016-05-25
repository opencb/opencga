/*
 * Copyright 2015 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


db.getCollection('user').createIndex({"name": 1})
db.getCollection('user').createIndex({"sessions.id": 1}, {"unique": true})

//db.getCollection('study').createIndex({"name": 1, "_projectId": 1}, {"unique": true}) ??
db.getCollection('study').createIndex({"alias": 1, "_projectId": 1}, {"unique": true})
db.getCollection('study').createIndex({"_projectId": 1})

db.getCollection('sample').createIndex({"name": 1, "_studyId": 1}, {"unique": true})
db.getCollection('sample').createIndex({"annotationSets.variableSetId": 1})
db.getCollection('sample').createIndex({"annotationSets.annotations.id": 1})
db.getCollection('sample').createIndex({"annotationSets.annotations.value": 1})
db.getCollection('sample').createIndex({"_studyId": 1})

db.getCollection('individual').createIndex({"name": 1, "_studyId": 1}, {"unique": true})
db.getCollection('individual').createIndex({"annotationSets.variableSetId": 1})
db.getCollection('individual').createIndex({"annotationSets.annotations.id": 1})
db.getCollection('individual').createIndex({"annotationSets.annotations.value": 1})
db.getCollection('individual').createIndex({"_studyId": 1})

db.getCollection('file').createIndex({"name": 1})
db.getCollection('file').createIndex({"path": 1, "_studyId": 1}, {"unique": true})
db.getCollection('file').createIndex({"jobId": 1})
db.getCollection('file').createIndex({"_studyId": 1})

db.getCollection('cohort').createIndex({"name": 1, "_studyId": 1}, {"unique": true})
