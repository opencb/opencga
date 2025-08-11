import argparse
import os
import sys
import re

# Get the path of the parent directory (one level up) and add it to the Python path to be able to import from the base_wrapper module
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)
from base_wrapper import BaseToolWrapper, logger

TOOL = "multiqc"

INPUT_PATHS = "inputPaths"
MULTIQC_PARAMS = "multiQcParams"

OUTDIR_PARAM = "--outdir"
FILENAME_PARAM = "--filename"

class MultiQCWrapper(BaseToolWrapper):
    """A wrapper for the MultiQC tool, configured via a JSON file."""

    def __init__(self, params_file):
        super().__init__()
        self.params = self._load_params(params_file)

        # Determine paths before validation
        self.output_dir = self.params['multiQcParams']['--outdir']
        self.log_file_dir = self.output_dir  # Log to the same directory as output

        # Add dynamic log file handler
        self._add_file_handler(self.log_file_dir)

        report_filename = self.params['multiQcParams'].get('--filename')
        if not report_filename:
            title = self.params['multiQcParams'].get('--title')
            if title:
                sanitized_title = re.sub(r'[^a-zA-Z0-9_\-]', '', title.replace(' ', '_'))
                report_filename = f"{sanitized_title}_multiqc_report.html"
            else:
                report_filename = "multiqc_report.html"

        self.output_report_path = os.path.join(self.output_dir, report_filename)
        self._validate_params()

    def _validate_params(self):
        """Validates parameters and files from the loaded JSON."""
        logger.info("Starting parameters validation...")

        required_params = ['inputPaths', 'multiQcParams']
        for param in required_params:
            if param not in self.params:
                logger.error(f"ERROR: Required top-level parameter '{param}' is missing from the JSON.")
                sys.exit(1)

        required_mqc_params = ['--outdir']
        for param in required_mqc_params:
            if param not in self.params['multiQcParams']:
                logger.error(f"ERROR: Required MultiQC parameter '{param}' is missing from the JSON.")
                sys.exit(1)

        for path in self.params['inputPaths']:
            if not os.path.exists(path) or not os.access(path, os.R_OK):
                logger.error(f"ERROR: Input path '{path}' specified in config not found or is not readable.")
                sys.exit(1)

        self._check_tool_exists("multiqc")

        logger.info("Parameters validation complete. All parameters appear valid.")

    def run_multiqc(self):
        """Constructs and executes the multiqc command based on the parameters."""
        logger.info("Starting MultiQC report generation...")

        multiqc_cmd = ["multiqc"]
        multiqc_cmd.extend(self.params['inputPaths'])

        for key, value in self.params['multiQcParams'].items():
            multiqc_cmd.append(key)
            if value is not True:
                if isinstance(value, list):
                    multiqc_cmd.extend(value)
                else:
                    multiqc_cmd.append(str(value))

        self._run_command(multiqc_cmd, "MultiQC report generation")
        self._validate_results()

    def _validate_results(self):
        """Checks for the existence and basic integrity of the MultiQC output file."""
        logger.info("Validating MultiQC output...")

        if not os.path.exists(self.output_report_path) or os.path.getsize(self.output_report_path) == 0:
            logger.error(f"ERROR: MultiQC report file not found or is empty at '{self.output_report_path}'.")
            sys.exit(1)

        logger.info("MultiQC report validation successful. The report is ready.")

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
    wrapper.run_multiqc()
    logger.info("MultiQC wrapper execution finished.")