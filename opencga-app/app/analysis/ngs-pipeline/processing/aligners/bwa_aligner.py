from __future__ import annotations

from pathlib import Path
from typing import override, Any

from .aligner import Aligner


class BwaAligner(Aligner):

    def __init__(self, output: Path, logger=None):
        super().__init__(output, logger)
        self.tool_name = "bwa"


    """ Run BWA alignment """
    @override
    def align(self, input_config: dict, bwa_tool_config: dict) -> list[Any]:
        if len(input_config.get("samples", [])) == 0:
            self.logger.error("No input samples provided for bwa")
            raise ValueError("No input files provided for bwa")

        ## Get reference index from tool or input config
        index_dir = self._get_aligner_index_dir(bwa_tool_config, input_config, self.tool_name)
        if not index_dir.exists() or not index_dir.is_dir():
            self.logger.error("Reference index directory %s does not exist or is not a directory", str(index_dir))
            raise ValueError(f"Reference index directory {str(index_dir)} does not exist or is not a directory")

        ## Find index prefix (assuming .fasta or .fa files)
        index_prefix = list(index_dir.glob("*.fasta")) + list(index_dir.glob("*.fa"))
        if not index_prefix:
            self.logger.error("No reference index files found in directory %s", str(index_dir))
            raise ValueError(f"No reference index files found in directory {str(index_dir)}")
        self.logger.debug("Index prefix files found: %s", str(index_prefix[0]))

        ## Run BWA for each sample, only for FASTQ files
        sorted_bams = []
        for sample in input_config.get("samples", []):
            files = sample.get("files", [])
            file_type = self.get_file_format(files[0]) if files else None
            self.logger.debug("Files '%s' type detected: %s", files, file_type)
            if file_type == "fastq":
                ## Parse parameters and add them to cmd if any
                parameters = bwa_tool_config.get("parameters") or {}
                params_list = self.build_cli_params(parameters, ["o", "outdir", "R"])

                ## Run BWA-MEM
                sam_file = self._build_sam_file_name(files)
                self.logger.debug("sam_file name: %s", sam_file)

                ## Construct command to run
                cmd = (["bwa"] + ["mem"] + params_list
                       + ["-R", "@RG\\tID:your_RG_ID\\tSM:" + sample.get("id")]
                       + ["-o", str(self.output) + "/" + sam_file]
                       + [str(index_prefix[0])] + files)
                self.run_command(cmd)

                ## Post-process SAM to sorted BAM and index
                sorted_bam = self._samtools_post_processing(sam_file)
                if sorted_bam:
                    sorted_bams.append(str(self.output / sorted_bam))
            else:
                self.logger.error("Unsupported input type %s for bwa", file_type)

        return sorted_bams
