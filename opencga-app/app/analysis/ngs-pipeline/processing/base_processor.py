import os
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

    def run_docker_command(self, docker_image: str, cmd: str | list[str], input_bindings: dict, output_bindings: dict, check: bool = True) -> subprocess.CompletedProcess:
        """
        Execute a command inside a Docker container with volume bindings.

        Parameters
        ----------
        docker_image : str
            Docker image to use
        cmd : list[str]
            Command to execute inside the container
        input_bindings : dict
            Dictionary of host paths to container paths for volume bindings
        output_bindings : dict
            Dictionary of host paths to container paths for volume bindings
        check : bool, optional
            Whether to check return code (default: True)

        Returns
        -------
        subprocess.CompletedProcess
            Result of the command execution
        """
        # Set up volume bindings
        binding_args = []
        for host_path, container_path in input_bindings.items():
            binding_args.extend(["--mount", f"type=bind,source={host_path},target={container_path},readonly"])

        for host_path, container_path in output_bindings.items():
            binding_args.extend(["--mount", f"type=bind,source={host_path},target={container_path}"])


        # Build Docker command
        docker_cmd = (["docker", "run", "--rm", f"--user={os.getuid()}:{os.getgid()}"]
                      + binding_args
                      + [docker_image]
                      + (cmd if isinstance(cmd, list) else [cmd]))

        # Log and execute the Docker command
        self.logger.debug("Executing Docker command: %s", ' '.join(docker_cmd))
        result = subprocess.run(docker_cmd, check=check, capture_output=True, text=True)
        if result.stdout:
            self.logger.info("Docker command output: %s", result.stdout.strip())
        if result.stderr:
            self.logger.warning("Docker command stderr: %s", result.stderr.strip())
        return result

