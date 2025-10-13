import subprocess
from abc import abstractmethod
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

    @abstractmethod
    def execute(self) -> any:
        pass

    def check_tool_availability(self, tool_name: str) -> bool:
        """
        Check if a command-line tool is available in the system PATH.

        Parameters
        ----------
        tool_name : str
            Name of the tool to check for availability

        Returns
        -------
        bool
            True if the tool is available, False otherwise
        """
        result = self.run_command(["which", tool_name], check=False)
        return result.returncode == 0

    def get_file_format(self, filename: str) -> str | None:
        """
        Determine the file format based on the file extension.

        Parameters
        ----------
        filename : str
            Name of the file

        Returns
        -------
        str
            File format (e.g., 'fastq', 'fasta', 'bam', 'vcf', or 'unknown')
        """
        if filename.endswith(".fq") or filename.endswith(".fastq") or filename.endswith(".fastq.gz") or filename.endswith(".fq.gz"):
            return "fastq"
        elif filename.endswith(".fa") or filename.endswith(".fasta") or filename.endswith(".fasta.gz") or filename.endswith(".fa.gz") or filename.endswith(".fna") or filename.endswith(".fna.gz"):
            return "fasta"
        elif filename.endswith(".bam"):
            return "bam"
        elif filename.endswith(".vcf") or filename.endswith(".vcf.gz"):
            return "vcf"
        else:
            return None


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


    def run_command(self, cmd: str | list[str], check: bool = True, shell: bool = False) -> subprocess.CompletedProcess:
        """
        Execute a command with logging.

        Parameters
        ----------
        cmd : list[str]
            Command to execute as a list of strings
        check : bool, optional
            Whether to check return code (default: True)
        shell : bool, optional

        Returns
        -------
        subprocess.CompletedProcess
            Result of the command execution
        """
        # Log the command being executed
        self.logger.debug("Executing command: %s", cmd if isinstance(cmd, str) else " ".join(cmd))

        # Execute the command and log output
        result = subprocess.run(cmd, check=check, capture_output=True, text=True, shell=shell)
        if result.stdout:
            self.logger.info("Command output: %s", result.stdout.strip())
        if result.stderr:
            self.logger.warning("Command stderr: %s", result.stderr.strip())
        return result
