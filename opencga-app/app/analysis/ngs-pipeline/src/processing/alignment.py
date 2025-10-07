from pathlib import Path

from processing.base_processor import BaseProcessor


class Alignment(BaseProcessor):

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
    def execute(self) -> str:
        self.logger.info("Starting Alignment step: %s", self.__class__.__name__)
        alignment_config = next((s for s in self.pipeline.get("steps", []) if s.get("name") == "alignment"),{})
        self.logger.debug("Configuration for QualityControl: %s", alignment_config)

        ## Get the tool in the quality-control step of the pipeline dict
        input = self.pipeline.get("input")
        bwa_tool_config = alignment_config.get("tool")
        result = None
        if isinstance(bwa_tool_config, dict):
            result = self.bwa(input, bwa_tool_config)
        elif not isinstance(bwa_tool_config, dict):
            raise ValueError("Invalid tool configuration format in quality-control step")
        return result


    """ Run BWA alignment """
    def bwa(self, input: dict, bwa_tool_config: dict) -> str:
        if len(input.get("files", [])) == 0:
            self.logger.error("No input files provided for bwa")
            return {"error": "No input files provided for bwa"}

        if input.get("type") == "fastq":
            files = input.get("files", [])

            ## Parse parameters and add them to cmd if any
            parameters = bwa_tool_config.get("parameters") or {}
            params_list = self.build_cli_params(parameters, ["o", "outdir", "R"])

            ## Run BWA-MEM
            sam_file = Path(files[0]).stem + ".sam"
            cmd = ["bwa"] + ["mem"] + params_list + ["-R", "@RG\\tID:your_RG_ID\\tSM:" + input.get("sample")] + ["-o", str(self.output) + "/" + sam_file] + [bwa_tool_config.get("index", [])] + files
            self.run_command(cmd)

            sorted_bam = self.samtools_post_processing(sam_file)

        return sorted_bam


    """ Run BWA-MEM2 alignment """
    def bwa_mem2(self, input: dict, bwa_tool_config: dict) -> dict[str, str] | int:
        if len(input.get("files", [])) == 0:
            self.logger.error("No input files provided for bwa")
            return {"error": "No input files provided for bwa"}

        if input.get("type") == "fastq":
            files = input.get("files", [])

            ## Parse parameters and add them to cmd if any
            parameters = bwa_tool_config.get("parameters") or {}
            params_list = self.build_cli_params(parameters, ["o", "outdir"])

            ## Run BWA-MEM2
            cmd = ["bwa-mem2"] + params_list + [bwa_tool_config.get("index", [])] + files + ["-o", str(self.output)]
            self.run_command(cmd)

        return 0


    """ Run minimap2 alignment """
    def minimap2(self, input: dict, minimap2_tool_config: dict) -> dict[str, str] | int:
        if len(input.get("files", [])) == 0:
            self.logger.error("No input files provided for minimap2")
            return {"error": "No input files provided for minimap2"}

        if input.get("type") == "fastq" or input.get("type") == "bam":
            files = input.get("files", [])
            output_prefix = Path(files[0]).stem
            cmd = ["minimap2"] + ["-a"] + [minimap2_tool_config.get("index", [])] + files + [">", str(self.output) + "/" + output_prefix + ".sam"]
            self.run_command(cmd)

        return 0

    """ Post-process SAM file to sorted BAM and index using samtools """
    def samtools_post_processing(self, sam_file: str) -> str:
        if sam_file.endswith("sam"):
            bam_file = Path(sam_file).stem + ".bam"
            cmd = ["samtools"] + ["view", "-bS"] + ["-o", str(self.output) + "/" + bam_file] + [str(self.output) + "/" + sam_file]
            self.run_command(cmd)

            sorted_bam_file = Path(bam_file).stem + ".sorted.bam"
            cmd = ["samtools"] + ["sort"] + ["--threads", "1"] + ["-o", str(self.output) + "/" + sorted_bam_file] + [str(self.output) + "/" + bam_file]
            self.run_command(cmd)

            bai_file = Path(bam_file).stem + ".sorted.bam.bai"
            cmd = ["samtools"] + ["index", "-b"] + ["--threads", "2"] + ["-o", str(self.output) + "/" + bai_file] + [str(self.output) + "/" + sorted_bam_file]
            self.run_command(cmd)

        return sorted_bam_file
