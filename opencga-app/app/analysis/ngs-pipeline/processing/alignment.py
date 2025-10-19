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
    A single alignment step.
    Implement `run` with concrete checks. `execute` wraps `run` adding logging
    and common error handling.
    """
    # @override
    def execute(self) -> list[str] | None:
        ## 1. Get alignment step configuration
        self.logger.info("Starting Alignment step: %s", self.__class__.__name__)
        alignment_config = self.pipeline.get("steps", {}).get("alignment", {})
        self.logger.debug("Configuration for Alignment: %s", alignment_config)

        ## 2. Check if step is defined and active
        if not alignment_config or not alignment_config.get("active", True):
            self.logger.warning("Alignment step is not defined or not active. Skipping.")
            return None

        ## 3. Select aligner based on tool ID
        tool_config = alignment_config.get("tool")
        self.logger.debug("Alignment tool configuration: %s", tool_config)
        tool_id = tool_config.get("id")
        aligner = None
        match(tool_id.upper()):
            case "BWA":
                self.logger.debug("Using BWA aligner")
                aligner = BwaAligner(self.output, self.logger)
            case "BWA-MEM2":
                self.logger.debug("Using BWA-MEM2 aligner")
                aligner = BwaMem2Aligner(self.output, self.logger)
            case "MINIMAP2":
                self.logger.debug("Using MINIMAP2 aligner")
                aligner = Minimap2Aligner(self.output, self.logger)
            case "BOWTIE2":
                self.logger.debug("Using BOWTIE2 aligner")
                aligner = Bowtie2Aligner(self.output, self.logger)
            case _:
                self.logger.error("Unsupported aligner tool: %s", tool_id)
                raise ValueError(f"Unsupported aligner tool: {tool_id}")

        ## 4. Run alignment and options
        sorted_bams = None
        if aligner is not None:
            ## Perform alignment
            sorted_bams = aligner.align(self.pipeline.get("input"), tool_config)

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
