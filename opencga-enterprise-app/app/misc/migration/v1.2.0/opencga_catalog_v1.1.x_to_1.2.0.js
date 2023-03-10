
load("utils/migrateCollection.js")


print(" --- Step 1/7 ---")
load("catalog/01_jwt_migration.js")
print(" --- Step 2/7 ---")
load("catalog/02_object_list_migration.js")
print(" --- Step 3/7 ---")
load("catalog/03_acl_member_migration.js")
print(" --- Step 4/7 ---")
load("catalog/04_add_owner_migration.js")
print(" --- Step 5/7 ---")
load("catalog/05_release_migration.js")
print(" --- Step 6/7 ---")
load("catalog/06_add_new_indexes.js")
print(" --- Step 7/7 ---")
load("catalog/07_int2Long.js")


print("Catalog database migrated correctly!")
