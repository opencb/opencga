#!/bin/bash

# Verificar que se pase el argumento necesario
if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <input_file>"
    exit 1
fi

# Variables
INPUT_FILE="$1"
ROOT_DIR=$(pwd)
REPORTS_DIR="$ROOT_DIR/reports"
TEST_DIR="$REPORTS_DIR/test"
IMAGES_DIR="$REPORTS_DIR/images"
LOGO_URL="$IMAGES_DIR/Group-3.webp"  # Cambia esto a la URL de tu logo

# Obtener la fecha de ejecución
EXECUTION_DATE=$(date +"%Y-%m-%d %H:%M:%S")

# Obtener el nombre y la versión del proyecto Maven padre
PARENT_POM="$ROOT_DIR/pom.xml"
if [ -f "$PARENT_POM" ]; then
    PROJECT_NAME=$(mvn help:evaluate -Dexpression=project.name -q -DforceStdout)
    PROJECT_VERSION=$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout)
else
    echo "No se encontró el archivo pom.xml en el directorio padre."
    exit 1
fi

TITLE="$PROJECT_NAME $PROJECT_VERSION - Test Reports"

# Crear directorio test si no existe
mkdir -p "$TEST_DIR"

# Borrar el contenido previo del directorio test
rm -rf "$TEST_DIR"/*

# Leer el fichero de entrada y procesar cada línea
while IFS= read -r line || [[ -n "$line" ]]; do
    # Obtener el directorio del proyecto y el nombre del proyecto
    PROJECT_DIR=$(echo "$line" | cut -d ' ' -f 1)
    PROJECT_NAME=$(echo "$line" | cut -d ' ' -f 2)

    # Buscar y copiar todas las carpetas site a la carpeta test, exceptuando el directorio reports
    find "$PROJECT_DIR" -maxdepth 5 -type d -path '*/target/site' ! -path '*/reports/*' ! -path '*/opencga-home/*' | while read -r site_dir; do
        module_dir=$(dirname "$(dirname "$site_dir")")
        module_name=$(basename "$module_dir")
        dest_dir="$TEST_DIR/${PROJECT_NAME}dir-$module_name-site"
        cp -r "$site_dir" "$dest_dir"
    done
done < "$INPUT_FILE"

# Crear el fichero index.html
INDEX_FILE="$TEST_DIR/index.html"
cat <<EOL > "$INDEX_FILE"
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>$TITLE</title>
    <style>
        body {
            font-family: Arial, sans-serif;
            margin: 0;
            padding: 0;
            display: flex;
            flex-direction: column;
            height: 100vh;
        }
        header {
            display: flex;
            justify-content: space-between;
            align-items: flex-end;
            padding: 10px;
            color: white;
            background-color: #000E30;
        }
        header .left {
            display: flex;
            align-items: center;
        }
        header img {
            height: 50px;
            margin-right: 20px;
        }
        header h1 {
            margin: 0;
            font-size: 1.5em;
        }
        header .date {
            font-size: 0.9em;
            align-self: flex-end;
        }
        main {
            display: flex;
            flex: 1;
        }
        nav {
            width: 200px;
            background-color: #343a40;
            color: white;
            padding: 20px;
            overflow-y: auto;
        }
        nav ul {
            list-style: none;
            padding: 0;
        }
        nav ul li {
            margin: 10px 0;
        }
        nav ul li a {
            color: white;
            text-decoration: none;
            cursor: pointer;
        }
        .submenu {
            display: none;
            padding-left: 20px;
        }
        .submenu li {
            margin: 5px 0;
        }
        #content {
            flex: 1;
            padding: 20px;
        }
        iframe {
            width: 100%;
            height: 100%;
            border: none;
        }
    </style>
    <script>
        function toggleSubmenu(id) {
            var submenu = document.getElementById(id);
            if (submenu.style.display === 'none' || submenu.style.display === '') {
                submenu.style.display = 'block';
            } else {
                submenu.style.display = 'none';
            }
        }
    </script>
</head>
<body>
    <header>
        <div class="left">
            <img src="$LOGO_URL" alt="Logo">
            <h1>$PROJECT_NAME - $PROJECT_VERSION</h1>
        </div>
        <div class="date">$EXECUTION_DATE</div>
    </header>
    <main>
        <nav>
            <ul>
EOL

# Leer el fichero de entrada nuevamente para generar el menú
while IFS= read -r line || [[ -n "$line" ]]; do
    # Obtener el directorio del proyecto y el nombre del proyecto
    PROJECT_DIR=$(echo "$line" | cut -d ' ' -f 1)
    PROJECT_NAME=$(echo "$line" | cut -d ' ' -f 2)

    echo "                <li>
                    <a onclick=\"toggleSubmenu('submenu-$PROJECT_NAME')\">$PROJECT_NAME</a>
                    <ul class=\"submenu\" id=\"submenu-$PROJECT_NAME\">" >> "$INDEX_FILE"

    # Añadir enlaces a los ficheros surefire-report.html en el menú
    for site_dir in "$TEST_DIR/${PROJECT_NAME}dir-"*-site; do
        if [ -d "$site_dir" ]; then
            MODULE_NAME=$(basename "$site_dir" | sed "s/${PROJECT_NAME}dir-//" | sed 's/-site$//')
            REPORT_PATH="$site_dir/surefire-report.html"
            if [ -f "$REPORT_PATH" ]; then
                echo "                        <li><a href=\"#\" onclick=\"document.getElementById('content').innerHTML='<iframe src=\'$(basename "$site_dir")/surefire-report.html\'></iframe>'\">$MODULE_NAME</a></li>" >> "$INDEX_FILE"
            fi
        fi
    done

    echo "                    </ul>
                </li>" >> "$INDEX_FILE"
done < "$INPUT_FILE"

# Cerrar las etiquetas HTML
cat <<EOL >> "$INDEX_FILE"
            </ul>
        </nav>
        <div id="content">
            <p>Select a report from the menu to view it here.</p>
        </div>
    </main>
</body>
</html>
EOL
cp -r "$IMAGES_DIR" "$TEST_DIR/images/"
echo "Reports have been collected and index.html has been created in $TEST_DIR"
echo "Open $INDEX_FILE in your browser to see the reports."
