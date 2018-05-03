load("utils/migrateCollection.js");


print(" --- Step 1/X ---");
load("catalog/01_permission_rules.js");
print(" --- Step 2/X ---");
load("catalog/02_creation_dates.js");


print("Catalog database migrated correctly!");
