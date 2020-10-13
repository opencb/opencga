print("\nAdding new interpretation indexes...")
db.interpretation.createIndex({"clinicalAnalysisId": 1, "studyUid": 1}, {"background": true});
db.interpretation.createIndex({"status": 1, "studyUid": 1}, {"background": true});
db.interpretation.createIndex({"primaryFindings.id": 1, "studyUid": 1}, {"background": true});
db.interpretation.createIndex({"secondaryFindings.id": 1, "studyUid": 1}, {"background": true});
