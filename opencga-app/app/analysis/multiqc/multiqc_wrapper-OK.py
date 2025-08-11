import argparse
import os
import subprocess
import logging
import sys
import json
import re

# --- GLOBAL LOGGER SETUP ---
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# --- Helper Functions ---
def check_file_exists(filepath, param_name):
    """Checks if a file exists and is readable."""
    if not os.path.exists(filepath) or not os.path.isfile(filepath):
        logger.error(f"ERROR: {param_name} file not found: {filepath}")
        sys.exit(1)
    if not os.access(filepath, os.R_OK):
        logger.error(f"ERROR: {param_name} file is not readable: {filepath}")
        sys.exit(1)
    logger.info(f"Parameter check: {param_name} '{filepath}' exists and is readable.")

def check_dir_writable(dirpath, param_name, create_if_not_exists=False):
    """Checks if a directory is writable. Can create if specified."""
    if not os.path.exists(dirpath):
        if create_if_not_exists:
            try:
                os.makedirs(dirpath, exist_ok=True)
                logger.info(f"Parameter check: Created output directory '{dirpath}'.")
            except OSError as e:
                logger.error(f"ERROR: Could not create {param_name} directory '{dirpath}': {e}")
                sys.exit(1)
        else:
            logger.error(f"ERROR: {param_name} directory not found: {dirpath}")
            sys.exit(1)
    if not os.path.isdir(dirpath):
        logger.error(f"ERROR: {param_name} is not a directory: {dirpath}")
        sys.exit(1)
    if not os.access(dirpath, os.W_OK):
        logger.error(f"ERROR: {param_name} directory is not writable: {dirpath}")
        sys.exit(1)
    logger.info(f"Parameter check: {param_name} '{dirpath}' exists and is writable.")

def check_tool_exists(tool_name):
    """Checks if a command-line tool is available in the system's PATH."""
    try:
        # We don't need to capture output to check for existence
        subprocess.run([tool_name, '--version'], check=True)
        logger.info(f"Tool check: '{tool_name}' found in PATH.")
    except (subprocess.CalledProcessError, FileNotFoundError):
        logger.error(f"ERROR: Tool '{tool_name}' not found or not executable. Please ensure it's in your system's PATH.")
        sys.exit(1)

def run_command(cmd, step_name):
    """Executes a shell command and handles errors."""
    logger.info(f"Running {step_name} command: {' '.join(cmd)}")

    try:
        process = subprocess.run(
            cmd,
            check=True,
            capture_output=True,
            text=True
        )
        logger.info(f"{step_name} completed successfully.")
        return process.stdout
    except subprocess.CalledProcessError as e:
        logger.error(f"ERROR: {step_name} failed with exit code {e.returncode}.")
        logger.error(f"Command: {' '.join(e.cmd)}")
        if e.stdout:
            logger.error(f"STDOUT:\n{e.stdout}")
        if e.stderr:
            logger.error(f"STDERR:\n{e.stderr}")
        sys.exit(1)
    except FileNotFoundError:
        logger.error(f"ERROR: Command '{cmd[0]}' not found for {step_name}. Is it in your PATH?")
        sys.exit(1)

# --- MultiQC Wrapper Class ---
class MultiQCWrapper:
    def __init__(self, params_file):
        self.params = self._load_params(params_file)
        self.output_dir = self.params['multiQcParams']['--outdir']

        # Now define the log file path
        self.log_file = os.path.join(self.output_dir, 'multiqc_wrapper.log')

        # Determine the final report name
        report_filename = self.params['multiQcParams'].get('--filename')
        if not report_filename:
            title = self.params['multiQcParams'].get('--title')
            if title:
                # Sanitize the title to create a valid filename
                sanitized_title = re.sub(r'[^a-zA-Z0-9_\-]', '', title.replace(' ', '-'))
                report_filename = f"{sanitized_title}_multiqc_report.html"
            else:
                report_filename = "multiqc_report.html"

        self.output_report_path = os.path.join(self.output_dir, report_filename)
        self._validate_params()

    def _load_params(self, params_file):
        """Loads and parses the JSON parameters file."""
        check_file_exists(params_file, "Parameters file")
        try:
            with open(params_file, 'r') as f:
                params = json.load(f)
            if not params:
                raise ValueError("Parameters file is empty.")
            logger.info(f"Parameters loaded from '{params_file}'.")
            return params
        except (json.JSONDecodeError, ValueError) as e:
            logger.error(f"ERROR: Could not parse parameters file '{params_file}': {e}")
            sys.exit(1)

    def _validate_params(self):
        """Validates parameters and files from the loaded JSON."""
        logger.info("Starting parameters validation...")

        # Check for required top-level parameters
        required_params = ['inputPaths', 'multiQcParams']
        for param in required_params:
            if param not in self.params:
                logger.error(f"ERROR: Required top-level parameter '{param}' is missing from the JSON.")
                sys.exit(1)

        # Check for required MultiQC parameters
        required_mqc_params = ['--outdir']
        for param in required_mqc_params:
            if param not in self.params['multiQcParams']:
                logger.error(f"ERROR: Required MultiQC parameter '{param}' is missing from the JSON.")
                sys.exit(1)

        # Validate input paths (can be files or directories)
        for path in self.params['inputPaths']:
            if not os.path.exists(path):
                logger.error(f"ERROR: Input path '{path}' specified in config not found.")
                sys.exit(1)
            if not os.access(path, os.R_OK):
                logger.error(f"ERROR: Input path '{path}' is not readable.")
                sys.exit(1)

        # Validate output directory
        check_dir_writable(self.params['multiQcParams']['--outdir'], "Output report directory", create_if_not_exists=True)

        # Check if MultiQC is installed
        check_tool_exists("multiqc")

        logger.info("Parameters validation complete. All parameters appear valid.")

    def run_multiqc(self):
        """Constructs and executes the multiqc command based on the parameters."""
        logger.info("Starting MultiQC report generation...")

        # Build the command from the parameters
        multiqc_cmd = ["multiqc"]

        # Add input paths first
        multiqc_cmd.extend(self.params['inputPaths'])

        # Iterate through the multiQcParams dictionary
        for key, value in self.params['multiQcParams'].items():
            multiqc_cmd.append(key)
            if value is not True: # Flags are handled by just appending the key
                # Handle lists of values (e.g., --exclude)
                if isinstance(value, list):
                    multiqc_cmd.extend(value)
                else:
                    multiqc_cmd.append(str(value))

        run_command(multiqc_cmd, "MultiQC report generation")
        self._validate_results()

    def _validate_results(self):
        """Checks for the existence and basic integrity of the MultiQC output file."""
        logger.info("Validating MultiQC output...")

        if not os.path.exists(self.output_report_path) or os.path.getsize(self.output_report_path) == 0:
            logger.error(f"ERROR: MultiQC report file not found or is empty at '{self.output_report_path}'.")
            sys.exit(1)

        logger.info("MultiQC report validation successful. The report is ready.")

# --- Main execution block ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="A Python wrapper for the MultiQC tool, configured via a JSON file.",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument(
        "-p", "--params",
        required=True,
        help="Path to the JSON parameters file for MultiQC."
    )
    args = parser.parse_args()

    wrapper = MultiQCWrapper(args.params)

    # Reconfigure logging to use the wrapper's output directory
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(wrapper.log_file),
            logging.StreamHandler(sys.stdout)
        ]
    )
    # Re-initialize the logger instance after re-configuring
    logger = logging.getLogger(__name__)

    wrapper.run_multiqc()
    logger.info("MultiQC wrapper execution finished.")