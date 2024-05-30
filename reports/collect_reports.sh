#!/bin/bash

# Verify that the necessary argument is passed
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
LOGO_URL="$IMAGES_DIR/Group-3.webp"

# Get the execution date
EXECUTION_DATE=$(date +"%Y-%m-%d %H:%M:%S")

# Get the project name and version from the parent pom.xml
PARENT_POM="$ROOT_DIR/pom.xml"
if [ -f "$PARENT_POM" ]; then
    PROJECT_NAME=$(mvn help:evaluate -Dexpression=project.name -q -DforceStdout)
    PROJECT_VERSION=$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout)
else
    echo "No se encontró el archivo pom.xml en el directorio padre."
    exit 1
fi

TITLE="$PROJECT_NAME $PROJECT_VERSION - Test Reports"

# Create the test directory if it does not exist
mkdir -p "$TEST_DIR"

# Remove all files in the test directory
rm -rf "$TEST_DIR"/*

# Read the input file to copy the site directories to the test folder
while IFS= read -r line || [[ -n "$line" ]]; do
    # Get the project directory and project name
    PROJECT_DIR=$(echo "$line" | cut -d ' ' -f 1)
    PROJECT_NAME=$(echo "$line" | cut -d ' ' -f 2)

    # Find all site directories and copy them to the test directory
    find "$PROJECT_DIR" -maxdepth 5 -type d -path '*/target/site' ! -path '*/reports/*' ! -path '*/opencga-home/*' | while read -r site_dir; do
        module_dir=$(dirname "$(dirname "$site_dir")")
        module_name=$(basename "$module_dir")
        dest_dir="$TEST_DIR/${PROJECT_NAME}dir-$module_name-site"
        cp -r "$site_dir" "$dest_dir"
    done
done < "$INPUT_FILE"

# Create the index.html file
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
            padding: 40px;
            color: white;
            background-color: #343a40;
        }
        header .left {
            display: flex;
            align-items: center;
        }
        header img {
            height: 50px;
            margin-right: 20px;
        }
        header .right {
            align-content: space-between;
            align-self: flex-end;
            justify-content: flex-end
        }
        header h1 {
            margin: 0;
            font-size: 1.5em;
        }
        header .date {
            font-size: 0.9em;
            float:right;
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
        </div>
        <div class="right">
            <h1>$PROJECT_NAME - $PROJECT_VERSION</h1>
            <div class="date">$EXECUTION_DATE</div>
        </div>
    </header>
    <main>
        <nav>
            <ul>
EOL

# Read the input file again to generate the menu
while IFS= read -r line || [[ -n "$line" ]]; do
    # Get the project directory and project name
    PROJECT_DIR=$(echo "$line" | cut -d ' ' -f 1)
    PROJECT_NAME=$(echo "$line" | cut -d ' ' -f 2)

    echo "                <li>
                    <a onclick=\"toggleSubmenu('submenu-$PROJECT_NAME')\">$PROJECT_NAME</a>
                    <ul class=\"submenu\" id=\"submenu-$PROJECT_NAME\">" >> "$INDEX_FILE"

# Add links to surefire-report.html files in the menu
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

# Add the closing tags to the index.html file
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
