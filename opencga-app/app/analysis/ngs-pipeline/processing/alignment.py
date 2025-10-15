from pathlib import Path

from .aligners.bowtie2_aligner import Bowtie2Aligner
from .aligners.bwa_aligner import BwaAligner
from .aligners.bwamem2_aligner import BwaMem2Aligner
from .aligners.minimap2_aligner import Minimap2Aligner
from .base_processor import BaseProcessor


class Alignment(BaseProcessor):

    def __init__(self, pipeline: dict, output: Path, logger=None):
        """
        Initialize QualityControl with config and output parameters.

        Parameters
        ----------
        pipeline : dict
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
    def execute(self) -> list[str]:
        ## 1. Check if input samples are provided
        input_config = self.pipeline.get("input")
        if len(input_config.get("samples", [])) == 0:
            self.logger.error("No input samples provided for bwa")
            raise ValueError("No input files provided for bwa")

        ## 2. Get alignment step configuration
        self.logger.info("Starting Alignment step: %s", self.__class__.__name__)
        alignment_config = next((s for s in self.pipeline.get("steps", []) if s.get("id") == "alignment"), {})
        tool_config = alignment_config.get("tool")
        self.logger.debug("Configuration for Alignment: %s", alignment_config)

        ## 3. Select aligner based on tool ID
        aligner = None
        match(tool_config.get("id")):
            case "bwa":
                self.logger.debug("Using BWA aligner")
                aligner = BwaAligner(self.output, self.logger)
            case "bwa-mem2":
                self.logger.debug("Using BWA-MEM2 aligner")
                aligner = BwaMem2Aligner(self.output, self.logger)
            case "minimap2":
                self.logger.debug("Using MINIMAP2 aligner")
                aligner = Minimap2Aligner(self.output, self.logger)
            case "bowtie2":
                self.logger.debug("Using BOWTIE2 aligner")
                aligner = Bowtie2Aligner(self.output, self.logger)
            case _:
                self.logger.error("Unsupported aligner tool: %s", tool_config.get("id"))
                raise ValueError(f"Unsupported aligner tool: {tool_config.get('id')}")

        ## 4. Run alignment and options
        sorted_bams = None
        if aligner is not None:
            ## Perform alignment
            sorted_bams = aligner.align(input_config, tool_config)

            ## Options:
            ## 1. Clean up intermediate files if specified in config
            if alignment_config.get("options", {}).get("clean", True):
                aligner.clean()

            ## 2. Check CRAM conversion option
            if alignment_config.get("options", {}).get("cram", False):
                index_dir = self.pipeline.get("input", {}).get("indexDir", "")
                aligner.create_cram(sorted_bams, index_dir)

            ## 3. Check is 'qc' options is set to True in config
            if alignment_config.get("options", {}).get("qc", True):
                aligner.qc(sorted_bams)
        else:
            self.logger.error("Aligner instance could not be created.")
            raise ValueError("Aligner instance could not be created.")
 
        return sorted_bams


    # """ Run minimap2 alignment """
    # def minimap2(self, input: dict, minimap2_tool_config: dict) -> dict[str, str] | int:
    #     if len(input.get("files", [])) == 0:
    #         self.logger.error("No input files provided for minimap2")
    #         return {"error": "No input files provided for minimap2"}
    #
    #     if input.get("type") == "fastq" or input.get("type") == "bam":
    #         files = input.get("files", [])
    #         output_prefix = Path(files[0]).stem
    #         cmd = ["minimap2"] + ["-a"] + [minimap2_tool_config.get("index", [])] + files + [">", str(self.output) + "/" + output_prefix + ".sam"]
    #         self.run_command(cmd)
    #
    #     return 0
