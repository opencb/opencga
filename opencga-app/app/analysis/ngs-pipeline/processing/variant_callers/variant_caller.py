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
    def _get_reference_index_dir(self, tool_config: dict, input_config: dict) -> Path:
        reference_index_dir = tool_config.get("reference") or str(input_config.get("indexDir") + "/" + "reference-genome-index")

        ## If reference index is not provided, raise error
        if not reference_index_dir:
            raise ValueError("Reference index not provided in tool or input configuration")

        ## Get the fasta file
        fasta_list = (list(Path(reference_index_dir).glob("*.fasta"))
                      + list(Path(reference_index_dir).glob("*.fa"))
                      + list(Path(reference_index_dir).glob("*.fna")))
        reference_path = list(fasta_list)[0] if fasta_list else None
        if not reference_path:
            raise ValueError("No FASTA file found in the reference index directory")
        return reference_path

    """ Build SAM file name based on input FASTQ files """
    def _build_vcf_file_name(self, bam_file_path: Path, suffix: str) -> str:
        vcf_file = bam_file_path.stem.replace(".sorted", "") + suffix
        return vcf_file

    """ Post-process VCF file: compress and index """
    def _vcf_post_processing(self, vcf_file: str) -> str | None:
        bgzip_vcf = None
        if vcf_file.endswith("vcf"):
            ## 1. Convert VCF to bgzip compressed VCF
            bgzip_vcf = vcf_file + ".gz"
            cmd = ["bgzip"] + ["--force"] + [str(self.output) + "/" + vcf_file]
            self.run_command(cmd)

            ## Index bgzip compressed VCF using bcftools
            cmd = ["bcftools"] + ["index", "--tbi"] + ["--threads", "2"] + ["-o", str(self.output) + "/" + bgzip_vcf + ".tbi"] + [str(self.output) + "/" + bgzip_vcf]
            self.run_command(cmd)
        return bgzip_vcf


    """ Clean up intermediate files such as SAM and unsorted BAM files """
    def clean(self):
        ## Check self.output exists and is a directory
        if self.output.exists() and self.output.is_dir():
            self.logger.debug("Remove intermediate files")
        else:
            self.logger.warning("Output directory %s does not exist or is not a directory", self.output)


    """ Run samtools stats on sorted BAM files """
    def qc(self, vcf_files: list[str]) -> None:
        if vcf_files:
            for vcf_file in vcf_files:
                vcf_file_path = Path(vcf_file)

                ## Check if sorted BAM file exists
                if not vcf_file_path.is_file():
                    self.logger.error("Sorted BAM file %s does not exist for samtools stats", vcf_file)
                    continue

                ## Run vcftools stats
                stats_file = vcf_file_path.stem + ".bcftools.stats"
                cmd = f"bcftools stats --threads 2 {shlex.quote(str(self.output / vcf_file_path.name))} > {shlex.quote(str(self.output / stats_file))}"
                self.run_command(cmd, shell=True)

                ## Check if MultiQC is installed and run it
                multiqc_installed = self.run_command(["which", "multiqc"], check=False)
                if multiqc_installed.returncode == 0:
                    cmd = ["multiqc"] + ["-o", str(self.output)] + [str(self.output / ".")]
                    self.run_command(cmd)

