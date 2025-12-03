#!/bin/bash
# MongoDB backup script that creates dumps compatible with MongoBackupUtils.restore()

# Check if required arguments are provided
if [ "$#" -lt 3 ]; then
    echo "Usage: $0 <backup-name> <first-database> <second-database> [mongodb-host] [mongodb-port]"
    echo "Example: $0 task-112 opencga_catalog_opencga opencga_catalog_test localhost 27017"
    exit 1
fi

# Input parameters
BACKUP_NAME=$1
FIRST_DB=$2
SECOND_DB=$3
MONGODB_HOST=${4:-"localhost"}
MONGODB_PORT=${5:-"27017"}

# Docker command
MONGO_CMD="docker exec mongo mongosh"

# Extract suffixes
extract_suffix() {
    local db_name=$1
    # Extract the part after the last underscore
    echo "$db_name" | rev | cut -d '_' -f 1 | rev
}

FIRST_SUFFIX=$(extract_suffix "$FIRST_DB")
SECOND_SUFFIX=$(extract_suffix "$SECOND_DB")

# Create directory structure
BASE_DIR="${BACKUP_NAME}/mongodb"
mkdir -p "${BASE_DIR}/${FIRST_SUFFIX}"
mkdir -p "${BASE_DIR}/${SECOND_SUFFIX}"

echo "Creating backup '${BACKUP_NAME}' for databases:"
echo "  - ${FIRST_DB} -> ${FIRST_SUFFIX}"
echo "  - ${SECOND_DB} -> ${SECOND_SUFFIX}"

# Function to dump collections from a database
dump_collections() {
    local db_name=$1
    local suffix=$2

    echo "Dumping collections from ${db_name}..."

    # Get list of collections directly (pipe output instead of capturing in variable)
    $MONGO_CMD --host "${MONGODB_HOST}" --port "${MONGODB_PORT}" "${db_name}" --quiet \
        --eval 'db.getCollectionNames().forEach(function(c) { print(c) })' | \
    while read collection; do
        echo "  - Exporting collection: ${collection}"
        target="${BASE_DIR}/${suffix}/${collection}.json.gz"

        # Format the export exactly as expected by MongoBackupUtils.restore()
        # Note the careful escaping of special characters
        $MONGO_CMD --host "${MONGODB_HOST}" --port "${MONGODB_PORT}" "${db_name}" --quiet \
            --eval "db.getCollection('${collection}')
                .find()
                .forEach(function(d){
                    print(EJSON.stringify(d, { relaxed: false})
                        .replace(/\\{\"\\\$oid\":\"(\\w{24})\"\\\}/g, \"ObjectId(\\\"\$1\\\")\")
                        .replace(/\\{\"\\\$date\":\\{\"\\\$numberLong\":\"([0-9]*)\"\\\}\\}/g, \"{\\\"\\\$date\\\":\$1}\")
                        .replace(/\\{\"\\\$numberLong\":\"([0-9]*)\"\\\}/g, \"NumberLong(\$1)\"));
                })" | gzip > "${target}"

        # Check if file was created successfully
        if [ -s "${target}" ]; then
            echo "    Successfully created ${target}"
        else
            echo "    Warning: ${target} is empty or was not created"
        fi
    done
}

# Dump collections from both databases
dump_collections "${FIRST_DB}" "${FIRST_SUFFIX}"
dump_collections "${SECOND_DB}" "${SECOND_SUFFIX}"

echo "Backup completed: ${BACKUP_NAME}"
echo "Directory structure:"
find "${BACKUP_NAME}" -type d | sort

# Show summary of files created
echo ""
echo "Files created:"
find "${BACKUP_NAME}" -name "*.json.gz" | wc -l