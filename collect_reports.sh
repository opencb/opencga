#!/bin/bash

CURRENT_DIR=$1
CURRENT_DIR=$1
# Variables
ROOT_DIR=$(pwd)
TEST_DIR="$ROOT_DIR/test"

# Crear directorio test si no existe
mkdir -p "$TEST_DIR"

# Borrar el contenido previo del directorio test
rm -rf "$TEST_DIR"/*

# Buscar y copiar todas las carpetas site a la carpeta test
find . -maxdepth 5 -type d -path '*/target/site' | while read -r site_dir; do
    module_dir=$(dirname "$(dirname "$site_dir")")
    module_name=$(basename "$module_dir")
    dest_dir="$TEST_DIR/$module_name-site"
    cp -r "$site_dir" "$dest_dir"
done

# Crear el fichero index.html
INDEX_FILE="$TEST_DIR/index.html"
echo "<html><body><h1>Opencga-enterprise Reports</h1><ul>" > "$INDEX_FILE"

# Añadir enlaces a los ficheros surefire-report.html
for site_dir in "$TEST_DIR"/*-site; do
    if [ -d "$site_dir" ]; then
        MODULE_NAME=$(basename "$site_dir" | sed 's/-site$//')
        REPORT_PATH="$site_dir/surefire-report.html"
        if [ -f "$REPORT_PATH" ]; then
            echo "<li><a href=\"$(basename "$site_dir")/surefire-report.html\">$MODULE_NAME surefire-report</a></li>" >> "$INDEX_FILE"
        fi
    fi
done

# Cerrar las etiquetas HTML
echo "</ul></body></html>" >> "$INDEX_FILE"

echo "Reports have been collected and index.html has been created in $TEST_DIR"
echo "Open $INDEX_FILE in your browser to see the reports."

