#!/usr/bin/env python3

import argparse
import json
import logging
import subprocess
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def build_command_list(tool: str, params: dict) -> list:
    """
    Builds the command-line argument list from the tool name and a dictionary of parameters.

    Args:
        tool (str): The name of the tool to execute (e.g., 'star', 'samtools').
        params (dict): A dictionary containing the command, options, and input.

    Returns:
        list: A list of strings representing the full command-line arguments.
    """
    command_list = [tool]

    # Add optional sub-command if it exists
    if "command" in params and params["command"]:
        command_list.append(params["command"])

    # Add params
    if "options" in params and params["options"]:
        for key, value in params["options"].items():
            command_list.append(key)
            if isinstance(value, list):
                command_list.extend(value)
            elif isinstance(value, str) and value:
                command_list.append(value)

    # Add positional input parameters
    if "input" in params and params["input"]:
        command_list.extend(params["input"])

    return command_list


def execute_tool(tool: str, params_file: str) -> int:
    """
    Reads parameters from a JSON file, constructs a command, and executes it.

    Args:
        tool (str): The name of the tool to execute.
        params_file (str): The path to the JSON parameter file.

    Returns:
        int: The exit code of the executed tool.
    """
    try:
        with open(params_file, 'r') as f:
            params = json.load(f)
    except FileNotFoundError:
        logging.error(f"Parameter file not found: {params_file}")
        return 1
    except json.JSONDecodeError:
        logging.error(f"Error decoding JSON from file: {params_file}")
        return 1
    except Exception as e:
        logging.error(f"An unexpected error occurred while reading the JSON file: {e}")
        return 1

    full_command = build_command_list(tool, params)
    logging.info(f"Executing command: {' '.join(full_command)}")

    try:
        # We use a list for the command to avoid shell injection vulnerabilities
        # by passing `shell=False`.
        result = subprocess.run(
            full_command,
            check=False,
            stdout=sys.stdout,
            stderr=sys.stderr,
            text=True  # Capture output as text
        )
        if result.returncode != 0:
            logging.error(f"Tool '{tool}' failed with exit code: {result.returncode}")

        return result.returncode
    except FileNotFoundError:
        logging.error(f"Tool not found: '{tool}'. Please ensure it is in your system's PATH.")
        return 127 # Standard exit code for command not found
    except Exception as e:
        logging.error(f"An error occurred during command execution: {e}")
        return 1


def main():
    """Main function to parse arguments and run the script."""
    parser = argparse.ArgumentParser(
        description="A script to execute a command-line tool using parameters from a JSON file."
    )
    parser.add_argument(
        "-t", "--tool",
        required=True,
        help="The name of the tool to execute (e.g., 'star', 'samtools')."
    )
    parser.add_argument(
        "-p", "--params",
        required=True,
        help="Path to the JSON file containing the tool parameters."
    )

    args = parser.parse_args()

    exit_code = execute_tool(args.tool, args.params)
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
