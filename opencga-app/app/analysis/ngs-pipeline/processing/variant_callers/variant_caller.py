import os
import shlex
from abc import abstractmethod
from pathlib import Path

from processing.base_processor import BaseProcessor


class VariantCaller(BaseProcessor):

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
    def call(self, input_config: dict, tool_config: dict) -> list[str]:
        pass


    """ Get reference index from tool or input config """
    def _get_aligner_index_dir(self, tool_config: dict, input_config: dict, tool_name: str) -> Path:
        index_dir = tool_config.get("index") or str(input_config.get("indexDir") + "/" + tool_name + "-index")
        if not index_dir:
            self.logger.error("Reference index not provided in tool or input configuration")
            raise ValueError("Reference index not provided in tool or input configuration")

        if not Path(index_dir).exists() or not Path(index_dir).is_dir():
            self.logger.error("Reference index directory %s does not exist or is not a directory", str(index_dir))
            raise ValueError(f"Reference index directory {index_dir} does not exist or is not a directory")

        return Path(index_dir)

    def _get_reference_index_dir(self, tool_config: dict, input_config: dict) -> Path:
        reference_index_dir = tool_config.get("reference") or str(
            input_config.get("indexDir") + "/" + "reference-genome-index")

        ## If reference index is not provided, raise error
        if not reference_index_dir:
            raise ValueError("Reference index not provided in tool or input configuration")

        ## Get the fasta file
        fasta_list = list(Path(reference_index_dir).glob("*.fasta")) + list(
            Path(reference_index_dir).glob("*.fa")) + list(Path(reference_index_dir).glob("*.fna"))
        reference_path = list(fasta_list)[0] if fasta_list else None
        if not reference_path:
            raise ValueError("No FASTA file found in the reference index directory")
        return reference_path

    """ Build SAM file name based on input FASTQ files """
    def _build_vcf_file_name(self, files) -> str:
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
    def _vcf_post_processing(self, vcf_file: str) -> str:
        if vcf_file.endswith("vcf"):
            ## 1. Convert VCF to bgzip compressed VCF
            bgzip_vcf = vcf_file + ".gz"
            cmd = ["bgzip"] + ["--force"] + ["-o", str(self.output) + "/" + bgzip_vcf] + [str(self.output) + "/" + vcf_file]
            self.run_command(cmd)

            ## Index bgzip compressed VCF using bcftools
            cmd = ["bcftools"] + ["index", "--tbi"] + ["--threads", "2"] + ["-o", str(self.output) + "/" + bgzip_vcf + ".tbi"] + [str(self.output) + "/" + bgzip_vcf]
            self.run_command(cmd)

        return vcf_file


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

