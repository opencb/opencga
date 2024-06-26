#!/bin/bash

# Function to calculate the corresponding branch of Xetabase project
get_xetabase_branch() {
  # Input parameter (branch name)
  input_branch="$1"

  # Check if the branch name is "develop" in that case return the same branch name
  if [[ "$input_branch" == "develop" ]]; then
    echo "develop"
    return 0
  fi

  # Check if the branch name starts with "release-" and follows the patterns "release-a.b.x" or "release-a.b.c.x"
  if [[ "$input_branch" =~ ^release-([0-9]+)\.([0-9]+)\.x$ ]] || [[ "$input_branch" =~ ^release-([0-9]+)\.([0-9]+)\.([0-9]+)\.x$ ]]; then
    # Extract the MAJOR part of the branch name
    MAJOR=${BASH_REMATCH[1]}
    # Calculate the XETABASE_MAJOR by subtracting 3 from MAJOR
    XETABASE_MAJOR=$((MAJOR - 3))
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
