import subprocess
from pathlib import Path

class BaseProcessor:
    """Base class for processing steps with common functionality."""

    def __init__(self, output: Path, logger=None):
        """
        Initialize BaseProcessor with common parameters.

        Parameters
        ----------
        output : Path
            Output directory path
        logger : logging.Logger, optional
            Logger instance for logging messages
        """
        self.output = output
        self.logger = logger


    def build_cli_params(self, parameters: dict, ignore: list) -> list[str]:
        """
        Convert a dictionary of parameters into a list of command-line arguments.
        Parameters
        ----------
        parameters : dict
            Dictionary of parameters to convert
        ignore : list
            List of parameter keys to ignore
        Returns
        -------
        list[str]
            List of command-line arguments
        """
        params_list: list[str] = []
        if isinstance(parameters, dict):
            for key, value in parameters.items():
                if key in ignore:
                    continue
                key_str = str(key).lstrip('-')
                flag = f"{'-' if len(key_str) == 1 else '--'}{key_str}"
                if isinstance(value, bool):
                    if value:
                        params_list.append(flag)
                elif value is None:
                    continue
                else:
                    params_list.extend([flag, str(value)])
        return params_list


    def run_command(self, cmd: list[str], check: bool = True) -> subprocess.CompletedProcess:
        """
        Execute a command with logging.

        Parameters
        ----------
        cmd : list[str]
            Command to execute as a list of strings
        check : bool, optional
            Whether to check return code (default: True)

        Returns
        -------
        subprocess.CompletedProcess
            Result of the command execution
        """
        self.logger.debug("Executing command: %s", " ".join(cmd))
        return subprocess.run(cmd, check=check)
