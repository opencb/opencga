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
    # @override
    def execute(self) -> None:
        self.logger.info("Starting QualityControl step: %s", self.__class__.__name__)
        quality_control_config = next((s for s in self.pipeline.get("steps", []) if s.get("id") == "quality-control"), {})
        self.logger.debug("Configuration for QualityControl: %s", quality_control_config)

        ## Get the tool in the quality-control step of the pipeline dict
        input = self.pipeline.get("input")
        fastqc_tool_config = quality_control_config.get("tool")

        if isinstance(fastqc_tool_config, dict):
            self.fastqc(input, fastqc_tool_config)
        elif not isinstance(fastqc_tool_config, dict):
            raise ValueError("Invalid tool configuration format in quality-control step")
        return None

    """ Run FastQC and MultiQC """
    def fastqc(self, input: dict, fastqc_tool_config: dict) -> None:
        if len(input.get("samples", [])) == 0:
            self.logger.error("No input samples provided for FastQC")
            raise ValueError("No input samples provided for FastQC")

        ## Run FastQC for each sample, only for FASTQ or BAM files
        for sample in input.get("samples", []):
            ## For each file in sample, run FastQC if type is fastq or bam
            for file in sample.get("files", []):
                self.logger.info("Running FastQC for sample: %s, file: %s", sample.get("sample"), file)
                file_type = self.get_file_format(file)
                self.logger.debug("File type detected: %s", file_type)
                if file_type == "fastq" or file_type == "bam":
                    files = sample.get("files", [])

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

