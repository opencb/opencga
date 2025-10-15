import os
import shlex
from abc import abstractmethod
from pathlib import Path

from processing.base_processor import BaseProcessor


class Aligner(BaseProcessor):

    def __init__(self, output: Path, logger=None):
        """
        Initialize Aligner with config and output parameters.
        Parameters
        ----------
        output : Path
            Output directory path
        logger : logging.Logger, optional
            Logger instance for logging messages
        """
        super().__init__(output, logger)


    @abstractmethod
    def align(self, input_config: dict, bwa_tool_config: dict) -> list[str]:
        pass


    """ Get reference index from tool or input config """
    def _get_aligner_index_dir(self, tool_config: dict, input_config: dict, tool_name: str) -> Path:
        index_dir = tool_config.get("index") or str(input_config.get("indexDir") + "/" + tool_name + "-index")
        if not index_dir:
            raise ValueError("Reference index not provided in tool or input configuration")
        return Path(index_dir)

    """ Build SAM file name based on input FASTQ files """
    def _build_sam_file_name(self, files) -> str:
        # Normalize filenames by removing all common FASTQ extensions
        def _base(name: str) -> str:
            lower = name.lower()
            for ext in (".fastq.gz", ".fq.gz", ".fastq", ".fq"):
                if lower.endswith(ext):
                    return name[: -len(ext)]
            return Path(name).stem  # fallback

        ## Create SAM file name based on input files
        if len(files) > 1:
            stems = [_base(Path(f).name) for f in files]
            common_prefix = os.path.commonprefix(stems).rstrip("._-")
            if not common_prefix:
                common_prefix = stems[0]
            sam_file = common_prefix + ".sam"
        else:
            sam_file = _base(Path(files[0]).name) + ".sam"
        return sam_file

    """ Post-process SAM file to sorted BAM and index using samtools """
    def _samtools_post_processing(self, sam_file: str) -> str:
        sorted_bam_file = None
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



    """ Clean up intermediate files such as SAM and unsorted BAM files """
    def clean(self):
        ## Check self.output exists and is a directory
        if self.output.exists() and self.output.is_dir():
            # Remove all SAM and not sorted BAM files in the output directory
            sam_files = list(self.output.glob("*.sam"))
            for sam_file in sam_files:
                try:
                    ## Remove SAM file
                    sam_file.unlink()
                    self.logger.debug("Removed SAM file: %s", sam_file)

                    ## Also remove corresponding BAM file if exists
                    bam_file = Path(sam_file).stem + ".bam"
                    bam_path = self.output / bam_file
                    if bam_path.exists():
                        bam_path.unlink()
                        self.logger.debug("Removed BAM file: %s", bam_path)
                except Exception as e:
                    self.logger.error("Error removing SAM file %s: %s", sam_file, str(e))
        else:
            self.logger.warning("Output directory %s does not exist or is not a directory", self.output)


    """ Run samtools stats on sorted BAM files """
    def qc(self, sorted_bams: list[str]) -> None:
        if sorted_bams:
            for sorted_bam in sorted_bams:
                sorted_bam_path = Path(sorted_bam)

                ## Check if sorted BAM file exists
                if not sorted_bam_path.is_file():
                    self.logger.error("Sorted BAM file %s does not exist for samtools stats", sorted_bam)
                    continue

                ## Run samtools stats
                stats_file = sorted_bam_path.stem + ".stats"
                cmd = f"samtools stats -@ 2 {shlex.quote(str(self.output / sorted_bam))} > {shlex.quote(str(self.output / stats_file))}"
                self.run_command(cmd, shell=True)

                ## Run samtools flagstat
                flagstat_file = sorted_bam_path.stem + ".flagstat"
                cmd = f"samtools flagstat -@ 2 {shlex.quote(str(self.output / sorted_bam))} > {shlex.quote(str(self.output / flagstat_file))}"
                self.run_command(cmd, shell=True)

                ## Check if MultiQC is installed and run it
                multiqc_installed = self.run_command(["which", "multiqc"], check=False)
                if multiqc_installed.returncode == 0:
                    cmd = ["multiqc"] + ["-o", str(self.output)] + [str(self.output / ".")]
                    self.run_command(cmd)


    def create_cram(self, sorted_bams: list[str], index_dir: str) -> list[str]:
        cram_files = []

        ## Find fasta file in the reference genome directory
        reference_path = None
        index_dir_path = Path(index_dir + "/" + "reference-genome-index")
        if index_dir_path.exists() and index_dir_path.is_dir():
            fasta_files = list(index_dir_path.glob("*.fa")) + list(index_dir_path.glob("*.fasta"))
            if fasta_files:
                reference_path = Path(str(fasta_files[0]))
            else:
                self.logger.error("No FASTA file found in reference directory %s", index_dir)

        ## Convert each sorted BAM to CRAM
        if sorted_bams and reference_path:
            for sorted_bam in sorted_bams:
                sorted_bam_path = Path(sorted_bam)

                ## Check if sorted BAM file exists
                if not sorted_bam_path.is_file():
                    self.logger.error("Sorted BAM file %s does not exist for CRAM conversion", sorted_bam)
                    continue

                ## Convert sorted BAM to CRAM
                cram_file = sorted_bam_path.stem + ".cram"
                cmd = ["samtools", "view", "-C", "-T", str(reference_path), "-o", str(self.output / cram_file), str(self.output / sorted_bam)]
                self.run_command(cmd)

                ## Index the CRAM file
                cmd = ["samtools", "index", str(self.output / cram_file)]
                self.run_command(cmd)

                ## Add to list of created CRAM files
                cram_files.append(str(self.output / cram_file))

        return cram_files
