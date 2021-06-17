#!/bin/sh

echo "------------ OpenCGA INIT ------------"
/opt/opencga/bin/opencga.sh --version

set -x

PERSISTENT_CONF=/opt/volume/conf/
DEFAULT_CONF=/opt/opencga/default-conf/
OPENCGA_CONF=/opt/opencga/conf/

if find /opt/opencga/libs/opencga-storage-hadoop-deps-* &> /dev/null ; then
    /opt/opencga/init/setup-hadoop.sh ${OPENCGA_CONF}/hadoop
    cp -r -v ${OPENCGA_CONF}/hadoop/* ${PERSISTENT_CONF}/hadoop/
fi


FILE="${PERSISTENT_CONF}/configuration.yml"
if [ -f "$FILE" ] && [ "${OVERWRITE_CONFIGURATION:-false}" == "false" ]; then
    echo "$FILE already exists"
    cp -r -v "${PERSISTENT_CONF}/"* "${OPENCGA_CONF}"
else
    echo "Copying default configs (if any)"
    cp -r -L -v "${DEFAULT_CONF}"/* "${OPENCGA_CONF}"

    echo "Initialising configs"
    # Override Yaml configs
    python3 /opt/opencga/init/override_yaml.py --save

    # Copies the config files from our local directory into a
    # persistent volume to be shared by the other containers.
    echo "Copy final configs to persistent config volume"
    cp -r -v "${OPENCGA_CONF}"/* "${PERSISTENT_CONF}"

    echo "Copying final analysis folder"
    cp -r -v /opt/opencga/analysis/* /opt/volume/analysis
fi


CATALOG_STATUS=$(/opt/opencga/bin/opencga-admin.sh catalog status --log-level warn)
# wait for mongodb
echo "About to wait for MongoDB"
until [ "$(echo "$CATALOG_STATUS" | jq -r .mongodbStatus)" == "true" ]; do
    echo "Waiting for Mongo DB"
    CATALOG_STATUS=$(/opt/opencga/bin/opencga-admin.sh catalog status --log-level warn)
done

INSTALLED="$(echo "$CATALOG_STATUS" | jq -r .installed)"
DB_NAME="$(echo "$CATALOG_STATUS" | jq -r .catalogDBName)"
echo "DB Name: $INSTALLED"
echo "Catalog installed : $INSTALLED"

if [ "$INSTALLED" == false ]; then
    echo "Installing catalog"
    echo "${INIT_OPENCGA_PASS}" | /opt/opencga/bin/opencga-admin.sh catalog install
    echo "catalog installed"
#    echo "copying session files"
#    cp -r /opt/opencga/sessions/* /opt/volume/sessions
#    echo "copied session files"
else
    echo "DB '${DB_NAME}' already installed"
fi
