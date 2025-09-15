import requests
import sys
import re

PACKAGE_NAME = "pyopencga"
TESTPYPI_URL = f"https://pypi.org/pypi/{PACKAGE_NAME}/json"

def fetch_existing_versions():
    try:
        response = requests.get(TESTPYPI_URL)
        if response.status_code != 200:
            return []
        data = response.json()
        return list(data.get("releases", {}).keys())
    except Exception as e:
        print(f"Error fetching versions: {e}", file=sys.stderr)
        return []

def calculate_version(maven_version):

    if not maven_version.endswith("-SNAPSHOT"):
        # Release version
        return maven_version

    existing_versions = fetch_existing_versions()
    base_version = maven_version.replace("-SNAPSHOT", "")
    dev_versions = []
    pattern = re.compile(re.escape(base_version) + r"\.dev(\d+)")

    for v in existing_versions:
        match = pattern.fullmatch(v)
        if match:
            dev_versions.append(int(match.group(1)))

    if not dev_versions:
        next_dev = 0
    else:
        next_dev = max(dev_versions) + 1

    return f"{base_version}.dev{next_dev}"

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: script.py <maven_version>")
        sys.exit(1)

    maven_version = sys.argv[1]
    final_version = calculate_version(maven_version)
    print(final_version)
