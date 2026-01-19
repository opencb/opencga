from __future__ import annotations

from pathlib import Path

from .aligner import Aligner


class Bowtie2Aligner(Aligner):

    def __init__(self, output: Path, logger=None):
        super().__init__(output, logger)
        self.tool_name = "bowtie2"


    """ Run BOWTIE2 alignment """
    # @override
    def align(self, input_config: dict, tool_config: dict) -> list[str]:
        ## 1. Get reference index from tool or input config
        index_dir = self._get_aligner_index_dir(tool_config, input_config, self.tool_name)

        ## 2. Find index prefix, assuming BOWTIE2 index files have .rev.1.bt2 extension
        index_prefix = list(index_dir.glob("*.rev.1.bt2"))
        if not index_prefix:
            self.logger.error("No reference index files found in directory %s", str(index_dir))
            raise ValueError(f"No reference index files found in directory {str(index_dir)}")
        self.logger.debug("Index prefix files found: %s", str(index_prefix[0]))

        ## 3. Parse parameters and add them to cmd if any
        # parameters = bwa_tool_config.get("parameters") or {}
        params_list = self.build_cli_params(tool_config.get("parameters") or {}, ["o", "outdir", "R"])
        self.logger.debug("BOWTIE2 parameters: %s", params_list)

        ## 4. Run BOWTIE2 for each sample, only for FASTQ files
        sorted_bams = []
        for sample in input_config.get("samples", []):
            files = sample.get("files", [])
            file_type = self.get_file_format(files[0]) if files else None
            self.logger.debug("Files '%s' type detected: %s", files, file_type)
            if file_type == "fastq":
                # Build BOWTIE2 file name with the sample ID and the .sam extension
                sam_file = self._build_sam_file_name(files)
                self.logger.debug("sam_file name: %s", sam_file)

                ## Determine if single-end or paired-end
                if len(files) == 1:
                    # Single-end read
                    files_param = ["-U", files[0]]
                elif len(files) == 2:
                    # Paired-end reads
                    files_param = ["-1", files[0], "-2", files[1]]
                else:
                    self.logger.error("Unsupported number of files (%d) for sample %s", len(files), sample.get("id"))
                    continue

                ## Construct command to run
                cmd = (["bowtie2"] + params_list
                       + ["-p", "2"]  # Number of threads, can be parameterized
                       + ["-x", str(index_prefix[0]).replace(".rev.1.bt2", "")]
                       + ["-S", str(self.output / sam_file)]
                       + files_param)
                self.run_command(cmd)

                ## Post-process SAM to sorted BAM and index
                sorted_bam = self._samtools_post_processing(sam_file)
                if sorted_bam:
                    sorted_bams.append(str(self.output / sorted_bam))
            else:
                self.logger.error("Unsupported input type %s for bowtie2", file_type)

        return sorted_bams
