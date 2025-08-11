import os
import subprocess
import logging
import sys
import json
import re

# --- GLOBAL LOGGER SETUP ---
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class BaseToolWrapper:
    """A base class for tool wrappers, providing common utility functions."""

    def __init__(self):
        # We can add a list of expected parameters to validate against
        self.expected_params = {}

    @staticmethod
    def _check_file_exists(filepath, param_name):
        """Checks if a file exists and is readable."""
        if not os.path.exists(filepath) or not os.path.isfile(filepath):
            logger.error(f"ERROR: {param_name} file not found: {filepath}")
            sys.exit(1)
        if not os.access(filepath, os.R_OK):
            logger.error(f"ERROR: {param_name} file is not readable: {filepath}")
            sys.exit(1)
        logger.info(f"Parameter check: {param_name} '{filepath}' file exists and is readable.")

    @staticmethod
    def _check_dir_exists(filepath, param_name):
        """Checks if a dir exists and is readable."""
        if not os.path.exists(filepath) or not os.path.isdir(filepath):
            logger.error(f"ERROR: {param_name} dir not found: {filepath}")
            sys.exit(1)
        if not os.access(filepath, os.R_OK):
            logger.error(f"ERROR: {param_name} dir is not readable: {filepath}")
            sys.exit(1)
        logger.info(f"Parameter check: {param_name} '{filepath}' dir exists and is readable.")

    @staticmethod
    def _check_dir_writable(dirpath, param_name, create_if_not_exists=False):
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

    @staticmethod
    def _check_tool_exists(tool_name):
        """Checks if a command-line tool is available in the system's PATH."""
        try:
            subprocess.run([tool_name, '--version'], check=True, capture_output=True)
            logger.info(f"Tool check: '{tool_name}' found in PATH.")
        except (subprocess.CalledProcessError, FileNotFoundError):
            logger.error(f"ERROR: Tool '{tool_name}' not found. Please ensure it's in your PATH.")
            sys.exit(1)

    @staticmethod
    def _run_command(cmd, step_name):
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

    @staticmethod
    def _load_params(params_file):
        """Loads and parses the JSON parameters file."""
        BaseToolWrapper._check_file_exists(params_file, "Parameters file")
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

    def _add_file_handler(self, log_dir):
        """Adds a file handler to the global logger."""
        self._check_dir_writable(log_dir, "Log directory", create_if_not_exists=True)
        log_file = os.path.join(log_dir, 'wrapper.log')

        # Check if handler already exists to prevent duplicate logs
        if not any(isinstance(h, logging.FileHandler) for h in logger.handlers):
            file_handler = logging.FileHandler(log_file, mode='a')
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)

        if not any(isinstance(h, logging.StreamHandler) for h in logger.handlers):
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setFormatter(formatter)
            logger.addHandler(console_handler)