#!/bin/bash

# Function to calculate the corresponding branch of Xetabase project
get_xetabase_branch() {
  # Input parameter (branch name)
  input_branch="$1"

  # If the branch begins with 'TASK' and exists in the opencga-enterprise repository, I return it
  if [[ $input_branch == TASK* ]]; then
    if [ "$(git ls-remote "https://$ZETTA_REPO_ACCESS_TOKEN@github.com/zetta-genomics/opencga-enterprise.git" "$input_branch" )" ] ; then
      echo $input_branch;
      return 0;
    fi
  fi

  # Check if the branch name is "develop" in that case return the same branch name
  if [[ "$input_branch" == "develop" ]]; then
    echo "develop"
    return 0
  fi

  # Check if the branch name starts with "release-" and follows the patterns "release-a.x.x" or "release-a.b.x"
  if [[ "$input_branch" =~ ^release-([0-9]+)\.x\.x$ ]] || [[ "$input_branch" =~ ^release-([0-9]+)\.([0-9]+)\.x$ ]]; then
    # Extract the MAJOR part of the branch name
    MAJOR=${BASH_REMATCH[1]}
    # Calculate the XETABASE_MAJOR by subtracting 1 from MAJOR of opencga
    XETABASE_MAJOR=$((MAJOR - 1))
    # Check if the XETABASE_MAJOR is negative
    if (( XETABASE_MAJOR < 0 )); then
      echo "Error: 'MAJOR' digit after subtraction results in a negative number."
      return 1
    fi
    # Construct and echo the new branch name
    echo "release-$XETABASE_MAJOR.${input_branch#release-$MAJOR.}"
    return 0
  fi

  # If the branch name does not match any of the expected patterns
  echo "Error: The branch name is not correct."
  return 1
}

# Check if the script receives exactly one argument
if [ "$#" -ne 1 ]; then
  echo "Usage: $0 <branch-name>"
  exit 1
fi

# Call the function with the input branch name
get_xetabase_branch "$1"