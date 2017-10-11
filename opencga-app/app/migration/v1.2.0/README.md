

You need to migrate both Catalog and Variant Storaga databases. First, migrate OpenCGA Catalog by executing the following commands, note that `opencga_catalog_v1.1.x_to_1.2.0.js` script will execute the different scripts in the `catalog` folder:

```bash
cd build/migration/v1.2.0

mongo ${CATALOG_DATABASE} opencga_catalog_v1.1.x_to_1.2.0.js
```

Second, execute the following commands for each storage database, remember there is one variant database per OpenCGA Project:

````bash
# Run for each storage database
mongo ${STORAGE_DATABASE_1} opencga_storage_v1.1.x_to_1.2.0.js 
mongo ${STORAGE_DATABASE_2} opencga_storage_v1.1.x_to_1.2.0.js 
.... 
mongo ${STORAGE_DATABASE_n} opencga_storage_v1.1.x_to_1.2.0.js 
````
