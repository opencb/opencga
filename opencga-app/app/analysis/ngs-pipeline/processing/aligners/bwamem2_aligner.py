from __future__ import annotations

from pathlib import Path

from .aligner import Aligner


class BwaMem2Aligner(Aligner):
    def __init__(self, output: Path, logger=None):
        super().__init__(output, logger)
        self.tool_name = "bwa-mem2"


    """ Run BWA alignment """
    # @override
    def align(self, input_config: dict, tool_config: dict) -> list[str]:
        ## 1. Get reference index from tool or input config
        index_dir = self._get_aligner_index_dir(tool_config, input_config, self.tool_name)

        ## 2. Find index prefix, assuming BWA index files have .amb extension
        index_prefix = list(index_dir.glob("*.amb"))
        if not index_prefix:
            self.logger.error("No reference index files found in directory %s", str(index_dir))
            raise ValueError(f"No reference index files found in directory {str(index_dir)}")
        self.logger.debug("Index prefix files found: %s", str(index_prefix[0]))

        ## 3. Parse parameters and add them to cmd if any
        params_list = self.build_cli_params(tool_config.get("parameters") or {}, ["o", "outdir", "R"])
        self.logger.debug("BWA-MEM2 parameters: %s", params_list)

        ## 4. Run BWA-MEM2 for each sample, only for FASTQ files
        sorted_bams = []
        for sample in input_config.get("samples", []):
            files = sample.get("files", [])
            file_type = self.get_file_format(files[0]) if files else None
            self.logger.debug("Files '%s' type detected: %s", files, file_type)
            if file_type == "fastq":
                # Build SAM file name with the sample ID and the .sam extension
                sam_file = self._build_sam_file_name(files)
                self.logger.debug("sam_file name: %s", sam_file)

                ## Construct command to run
                cmd = (["bwa-mem2"] + ["mem"] + params_list
                       + ["-R", "\'@RG\\tID:zetta\\tSM:" + sample.get("id") + "\'"]
                       + ["-o", str(self.output) + "/" + sam_file]
                       + [str(index_prefix[0]).replace(".amb", "")] + files)
                self.run_command(cmd)

                ## Post-process SAM to sorted BAM and index
                sorted_bam = self._samtools_post_processing(sam_file)
                if sorted_bam:
                    sorted_bams.append(str(self.output / sorted_bam))
            else:
                self.logger.error("Unsupported input type %s for bwa", file_type)

        return sorted_bams
