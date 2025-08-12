# run_deseq2.py
import sys
import json
import subprocess
import os

def main():
    """
    Reads a JSON config file and executes the R script for DESeq2 analysis.
    """
    # --- 1. Get and Validate Config File Path ---
    if len(sys.argv) != 2:
        print("Usage: python run_deseq2.py <path_to_config.json>")
        sys.exit(1)

    config_path = sys.argv[1]
    if not os.path.exists(config_path):
        print(f"❌ Error: Config file not found at '{config_path}'")
        sys.exit(1)

    # --- 2. Load and Validate Config JSON ---
    with open(config_path, 'r') as f:
        config = json.load(f)

    # Basic validation of required file paths
    required_paths = ['countsFile', 'metadataFile']
    for path_key in required_paths:
        if not config.get('input', {}).get(path_key):
            print(f"Error: '{path_key}' is missing in the 'paths' section of the config file.")
            sys.exit(1)
        # Check if input files exist
        if path_key in ['countsFile', 'metadataFile'] and not os.path.exists(config['input'][path_key]):
            print(f"Error: Input file not found at '{config['input'][path_key]}'")
            sys.exit(1)

    print(f"Config file '{config_path}' loaded and validated.")

    # --- 3. Construct and Run the R Command ---
    try:
        rscript_path = "/analysis/deseq2/deseq2_script.R"
        if not os.path.exists(rscript_path):
            print(f"Error: The R script '{rscript_path}' was not found in the current directory.")
            sys.exit(1)

        command = ["Rscript", rscript_path, config_path]

        print("\nExecuting R script...")
        print(f"   Command: {' '.join(command)}\n")

        subprocess.run(command, check=True, text=True)

    except subprocess.CalledProcessError:
        print("Error: The R script failed. Check the R output above for details.")
        sys.exit(1)
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()