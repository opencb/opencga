load("utils/migrateCollection.js");


print(" --- Step 1/8 ---");
load("catalog/01_sample_individual_relation.js");
print(" --- Step 2/8 ---");
load("catalog/02_versioning.js");
print(" --- Step 3/8 ---");
load("catalog/03_admins_group.js");
print(" --- Step 4/8 ---");
load("catalog/04_sample_stats.js");
print(" --- Step 5/8 ---");
load("catalog/05_phenotypes_renaming.js");
print(" --- Step 6/8 ---");
load("catalog/06_repeated_samples.js");
print(" --- Step 7/8 ---");
load("catalog/07_change_metadata_identifier.js");
print(" --- Step 8/8 ---");
load("catalog/08_update_indexes.js");


print("Catalog database migrated correctly!");
