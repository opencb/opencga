

Migrate OpenCGA database from version 1.1.x to 1.2.0

````bash
cd build/migration/v1.2.0

mongo ${CATALOG_DATABASE} opencga_catalog_v1.1.x_to_1.2.0.js

# Run for each storage database
mongo ${STORAGE_DATABASE_1} opencga_storage_v1.1.x_to_1.2.0.js 
mongo ${STORAGE_DATABASE_2} opencga_storage_v1.1.x_to_1.2.0.js 
.... 
mongo ${STORAGE_DATABASE_n} opencga_storage_v1.1.x_to_1.2.0.js 
````