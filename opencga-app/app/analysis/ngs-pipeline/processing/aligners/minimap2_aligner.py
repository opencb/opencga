from __future__ import annotations

from pathlib import Path

from .aligner import Aligner


class Minimap2Aligner(Aligner):

    def __init__(self, output: Path, logger=None):
        super().__init__(output, logger)
        self.tool_name = "minimap2"


    """ Run MINIMAP2 alignment """
    # @override
    def align(self, input_config: dict, tool_config: dict) -> list[str]:
        ## 1. Get reference index from tool or input config
        index_dir = self._get_aligner_index_dir(tool_config, input_config, self.tool_name)

        ## 2. Find index prefix, assuming MINIMAP2 index files have .mmi extension
        index_prefix = list(index_dir.glob("*.mmi"))
        if not index_prefix:
            self.logger.error("No reference index files found in directory %s", str(index_dir))
            raise ValueError(f"No reference index files found in directory {str(index_dir)}")
        self.logger.debug("Index prefix files found: %s", str(index_prefix[0]))

        ## 3. Parse parameters and add them to cmd if any
        # parameters = bwa_tool_config.get("parameters") or {}
        params_list = self.build_cli_params(tool_config.get("parameters") or {}, ["o", "outdir", "R"])
        self.logger.debug("MINIMAP2 parameters: %s", params_list)

        ## 4. Run MINIMAP2 for each sample, only for FASTQ files
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
                cmd = (["minimap2"] + params_list
                       + ["-a", "-x", "sr"]
                       + ["-t", "2"]  # Number of threads, can be parameterized
                       + ["-R", "@RG\\tID:your_RG_ID\\tSM:" + sample.get("id")]
                       + ["-o", str(self.output) + "/" + sam_file]
                       + [str(index_prefix[0])] + files)
                self.run_command(cmd)

                ## Post-process SAM to sorted BAM and index
                sorted_bam = self._samtools_post_processing(sam_file)
                if sorted_bam:
                    sorted_bams.append(str(self.output / sorted_bam))
            else:
                self.logger.error("Unsupported input type %s for minimap2", file_type)

        return sorted_bams
