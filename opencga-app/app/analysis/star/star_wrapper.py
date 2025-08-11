import argparse
import os
import sys
import re

# Get the path of the parent directory (one level up) and add it to the Python path to be able to import from the base_wrapper module
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)
from base_wrapper import BaseToolWrapper, logger

TOOL = "STAR"

STAR_PARAMS = "starParams"

# Define STAR parameters
RUN_MODE_PARAM = "--runMode"
READ_FILES_IN_PARAM = "--readFilesIn"
GENOME_DIR_PARAM = "--genomeDir"
OUT_FILE_NAME_PREFIX_PARAM = "--outFileNamePrefix"
GENOME_FASTA_FILES_PARAM = "--genomeFastaFiles"

# Define STAR parameter values
RUN_MODE_GENOME_GENERATE = "genomeGenerate"
RUN_MODE_ALIGN_READS = "alignReads"

class StarWrapper(BaseToolWrapper):
    """A wrapper for the STAR tool, configured via a JSON file."""

    def __init__(self, params_file):
        super().__init__()
        self.params = self._load_params(params_file)

        # Define and set up logging to the output directory
        self.run_mode = self.params[STAR_PARAMS].get(RUN_MODE_PARAM, RUN_MODE_ALIGN_READS) # Default to alignReads
        if self.run_mode == RUN_MODE_ALIGN_READS:
            self.output_dir = os.path.dirname(self.params[STAR_PARAMS][OUT_FILE_NAME_PREFIX_PARAM])
            self.star_out_prefix = self.params[STAR_PARAMS][OUT_FILE_NAME_PREFIX_PARAM]
        elif self.run_mode == RUN_MODE_GENOME_GENERATE:
            self.output_dir = self.params[STAR_PARAMS][GENOME_DIR_PARAM]
            self.star_out_prefix = None
        # else:
        #     logger.error(f"ERROR: Unsupported STAR runMode: {self.run_mode}")
        #     sys.exit(1)

        self.log_file_dir = self.output_dir
        self._add_file_handler(self.log_file_dir)

        self._validate_params()

    def _validate_params(self):
        """Validates parameters and files for STAR based on runMode."""
        logger.info(f"Starting STAR parameters validation for runMode: {self.run_mode}...")

        required_params = [STAR_PARAMS]
        for param in required_params:
            if param not in self.params:
                logger.error(f"ERROR: Required top-level parameter '{param}' is missing from the JSON.")
                sys.exit(1)

        if self.run_mode == RUN_MODE_GENOME_GENERATE:
            required_star_params = [GENOME_DIR_PARAM, GENOME_FASTA_FILES_PARAM]
            for param in required_star_params:
                if param not in self.params[STAR_PARAMS]:
                    logger.error(f"ERROR: Required STAR parameter '{param}' is missing for '{RUN_MODE_GENOME_GENERATE}' mode.")
                    sys.exit(1)

            # --genomeDir is an OUTPUT directory that must be writable
            self._check_dir_writable(self.params[STAR_PARAMS][GENOME_DIR_PARAM], "STAR genome output directory", create_if_not_exists=True)
            # --genomeFastaFiles is a required INPUT file, check all of them since multiple FASTA files (separated by blank) is allowed
            fasta_files = self.params[STAR_PARAMS][GENOME_FASTA_FILES_PARAM].split()
            for fasta_file in fasta_files:
                self._check_file_exists(fasta_file, f"FASTA file: {fasta_file}")

        elif self.run_mode == RUN_MODE_ALIGN_READS:
            required_star_params = [GENOME_DIR_PARAM, OUT_FILE_NAME_PREFIX_PARAM, READ_FILES_IN_PARAM]
            for param in required_star_params:
                if param not in self.params[STAR_PARAMS]:
                    logger.error(f"ERROR: Required STAR parameter '{param}' is missing for alignReads mode.")
                    sys.exit(1)

            # --genomeDir is an INPUT directory that must exist
            self._check_dir_exists(self.params[STAR_PARAMS][GENOME_DIR_PARAM], "Genome directory")
            # --outFileNamePrefix must be writable
            output_dir = os.path.dirname(self.params[STAR_PARAMS][OUT_FILE_NAME_PREFIX_PARAM])
            self._check_dir_writable(output_dir, f"Output directory (from '{OUT_FILE_NAME_PREFIX_PARAM}')", create_if_not_exists=True)
            # Check existence of read files
            read_files = self.params[STAR_PARAMS][READ_FILES_IN_PARAM].split()
            for read_file in read_files:
                self._check_file_exists(read_file, f"Read file: {read_file}")

        self._check_tool_exists(TOOL)
        logger.info("Parameters validation complete. All parameters appear valid.")

    def run_star(self):
        """Constructs and executes the STAR command based on the parameters."""
        logger.info(f"Starting STAR run in {self.run_mode} mode...")

        star_cmd = [TOOL]

        # Dynamically build the command from the star_params dictionary
        for key, value in self.params[STAR_PARAMS].items():
            star_cmd.append(key)
            if key == READ_FILES_IN_PARAM or key == GENOME_FASTA_FILES_PARAM:
                # Special handling for read and fasta files by splitting the string into a list of files
                star_cmd.extend(value.split())
            else:
                star_cmd.append(str(value))

        self._run_command(star_cmd, f"STAR {self.run_mode}")
        self._validate_results()

    def _validate_results(self):
        """Checks for the existence of key STAR output files based on runMode."""
        logger.info("Validating STAR output...")

        if self.run_mode == RUN_MODE_GENOME_GENERATE:
            genome_dir = self.params[STAR_PARAMS][GENOME_DIR_PARAM]
            expected_files = [
                os.path.join(genome_dir, 'SA'),
                os.path.join(genome_dir, 'SAindex'),
                os.path.join(genome_dir, 'Genome'),
                os.path.join(genome_dir, 'chrName.txt'),
                os.path.join(genome_dir, 'chrNameLength.txt'),
                os.path.join(genome_dir, 'chrLength.txt'),
                os.path.join(genome_dir, 'genomeParameters.txt'),
                os.path.join(genome_dir, 'Log.out')
            ]
        elif self.run_mode == RUN_MODE_ALIGN_READS:
            expected_files = [
                f"{self.star_out_prefix}Aligned.out.sam",
                f"{self.star_out_prefix}Log.final.out",
                f"{self.star_out_prefix}SJ.out.tab"
            ]

        for f in expected_files:
            if not os.path.exists(f):
                logger.error(f"ERROR: Expected STAR output file not found: {f}")
                sys.exit(1)

        logger.info(f"STAR output validation successful for {self.run_mode} mode.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="A Python wrapper for the STAR tool, configured via a JSON file.",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument(
        "-p", "--params",
        required=True,
        help="Path to the JSON parameters file for STAR."
    )
    args = parser.parse_args()

    wrapper = StarWrapper(args.params)
    wrapper.run_star()
    logger.info("STAR wrapper execution finished.")