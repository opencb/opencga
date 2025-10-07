from pathlib import Path

from processing.base_processor import BaseProcessor


class QualityControl(BaseProcessor):

    def __init__(self, pipeline: dict, output: Path, logger=None):
        """
        Initialize QualityControl with config and output parameters.

        Parameters
        ----------
        config : dict
            Configuration dictionary
        output : dict
            Output dictionary
        """
        super().__init__(output, logger)
        self.pipeline = pipeline


    """
    A single quality-control step.

    Implement `run` with concrete checks. `execute` wraps `run` adding logging
    and common error handling.
    """
    def execute(self) -> dict:
        self.logger.info("Starting QualityControl step: %s", self.__class__.__name__)
        quality_control_config = next((s for s in self.pipeline.get("steps", []) if s.get("name") == "quality-control"), {})
        self.logger.debug("Configuration for QualityControl: %s", quality_control_config)

        ## Get the tool in the quality-control step of the pipeline dict
        input = self.pipeline.get("input")
        fastqc_tool_config = quality_control_config.get("tool")
        result = None
        if isinstance(fastqc_tool_config, dict):
            result = self.fastqc(input, fastqc_tool_config)
        elif not isinstance(fastqc_tool_config, dict):
            raise ValueError("Invalid tool configuration format in quality-control step")
        return result

    def fastqc(self, input: dict, fastqc_tool_config: dict) -> dict[str, str] | None:
        if len(input.get("files", [])) == 0:
            self.logger.error("No input files provided for FastQC")
            return {"error": "No input files provided for FastQC"}

        if input.get("type") == "fastq" or input.get("type") == "bam":
            files = input.get("files", [])

            ## Parse parameters and add them to cmd if any
            parameters = fastqc_tool_config.get("parameters") or {}
            params_list = self.build_cli_params(parameters, ["o", "outdir"])

            ## Run FastQC
            cmd = ["fastqc"] + params_list + ["-o", str(self.output)] + files
            self.run_command(cmd)

            ## Check if MultiQC is installed and run it
            multiqc_installed = self.run_command(["which", "multiqc"], check=False)
            if multiqc_installed.returncode == 0:
                cmd = ["multiqc"] + ["-o", str(self.output)] + [str(self.output / ".")]
                self.run_command(cmd)

        return None

