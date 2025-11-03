from pathlib import Path

from .aligners.bowtie2_aligner import Bowtie2Aligner
from .aligners.bwa_aligner import BwaAligner
from .aligners.bwamem2_aligner import BwaMem2Aligner
from .aligners.minimap2_aligner import Minimap2Aligner
from .base_processor import BaseProcessor


class AffymetrixMicroarray(BaseProcessor):

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
    def execute(self) -> list[str]:
        ## Execute
        # 1.
        # ~/bin/affy-power-tools/bin/apt-genotype-axiom --analysis-files-path axiom --cel-files data/cel_list.txt --gender-file data/gender_list.txt --chip-type Axiom_KU8 --spf-file axiom/Axiom_KU8.r2.spf --snp-priors-input-file axiom/Axiom_KU8.r2.generic_prior.txt --snp-posteriors-output --out-dir /tmp/axiom2
        #
        # 2.
        # bcftools +affy2vcf --calls /tmp/axiom2/Axiom_KU8.calls.txt --confidences /tmp/axiom2/Axiom_KU8.confidences.txt --fasta-ref ~/soft/index-dir/reference-genome-index/Homo_sapiens.GRCh38.dna.primary_assembly.fa --snp /tmp/axiom2/Axiom_KU8.snp-posteriors.txt --csv axiom/Axiom_KU8.na36.r1.a2.annot.csv --output /tmp/axiom2/Axiom_KU8.vcf
        #
        # 3.
        # bcftools sort Axiom_KU8.vcf > Axiom_KU8.sorted.vcf
        # bcftools norm -d exact Axiom_KU8.sorted.vcf > Axiom_KU8.sorted.nodup.vcf
        # bcftools view -e 'REF ~ "D" || ALT ~ "D" || REF ~ "I" || ALT ~ "I"' Axiom_KU8.sorted.nodup.vcf -Ov -o filtered.vcf

        ## 1. Create cel_list.txt file input.samples.files
        self.create_cel_list_file()

        ## 2. Execute apt-genotype-axiom,
        # example: # apt-genotype-axiom --analysis-files-path axiom --cel-files data/cel_list.txt --gender-file data/gender_list.txt --chip-type Axiom_KU8 --spf-file axiom/Axiom_KU8.r2.spf --snp-priors-input-file axiom/Axiom_KU8.r2.generic_prior.txt --snp-posteriors-output --out-dir /tmp/axiom2
        # Construct command to run
        index_dir = self.pipeline.get("input", {}).get("indexDir", "axiom")
        cmd = (["apt-genotype-axiom"]
               + ["--analysis-files-path", index_dir]
               + ["--cel-files", str(self.output / "cel_files.txt")]
               # + ["--gender-file", "data/gender_list.txt"]
               + ["--chip-type", "Axiom_KU8"]
               + ["--spf-file", index_dir + "/Axiom_KU8.r2.spf"]
               + ["--snp-priors-input-file", index_dir + "/Axiom_KU8.r2.generic_prior.txt"]
               + ["--snp-posteriors-output"]
               + ["--out-dir", str(self.output)])
        self.run_command(cmd)

        ## 3. Call to bcftools +affy2vcf
        # example: bcftools +affy2vcf --calls /tmp/axiom2/Axiom_KU8.calls.txt --confidences /tmp/axiom2/Axiom_KU8.confidences.txt --fasta-ref ~/soft/index-dir/reference-genome-index/Homo_sapiens.GRCh38.dna.primary_assembly.fa --snp /tmp/axiom2/Axiom_KU8.snp-posteriors.txt --csv axiom/Axiom_KU8.na36.r1.a2.annot.csv --output /tmp/axiom2/Axiom_KU8.vcf
        # Construct command to run
        cmd = (["bcftools"] + ["+affy2vcf"]
               + ["--calls", str(self.output / "Axiom_KU8.calls.txt")]
               + ["--confidences", str(self.output / "Axiom_KU8.confidences.txt")]
               + ["--fasta-ref", index_dir + "/Homo_sapiens.GRCh38.dna.primary_assembly.fa"]
               + ["--snp", str(self.output / "Axiom_KU8.snp-posteriors.txt")]
               + ["--csv", index_dir + "/Axiom_KU8.na36.r1.a2.annot.csv"]
               + ["--output", str(self.output / "Axiom_KU8.vcf")])
        self.run_command(cmd)

        ## 4. Call to post_process
        self.post_process()

        return [str(self.output / "Axiom_KU8.vcf")]

    """
    Create a one-column file with all .CEL files and header line 'cel_files'
    """
    def create_cel_list_file(self):
        ## 1. Loop all input.samples.files and write a file
        # Open a file called cel_files.txt in the outdir

        cel_list_path = self.output / "cel_files.txt"
        # cel_list_path.parent.mkdir(parents=True, exist_ok=True)

        cel_files: list[str] = []
        for sample in self.pipeline.get("input", {}).get("samples", []):
            for f in sample.get("files", []):
                if isinstance(f, str) and f.lower().endswith(".cel"):
                    cel_files.append(f)

        with cel_list_path.open("w", encoding="utf-8") as fh:
            fh.write("cel_files\n")
            for f in cel_files:
                fh.write(f"{f}\n")

        return cel_list_path

    def post_process(self):
        # bcftools sort Axiom_KU8.vcf > Axiom_KU8.sorted.vcf
        # bcftools norm -d exact Axiom_KU8.sorted.vcf > Axiom_KU8.sorted.nodup.vcf
        # bcftools view -e 'REF ~ "D" || ALT ~ "D" || REF ~ "I" || ALT ~ "I"' Axiom_KU8.sorted.nodup.vcf -Ov -o Axiom_KU8.sorted.nodup.filtered.vcf
        # bcftools view -e 'REF ~ "[()]" || ALT ~ "[()]' Axiom_KU8.sorted.nodup.filtered.vcf -Oz -o Axiom_KU8.sorted.nodup.filtered.fixed.vcf.gz
        # bcftools index no_brackets.vcf.gz
        ## 1. Sort VCF
        cmd = (["bcftools"] + ["sort"]
               + [str(self.output / "Axiom_KU8.vcf")]
               + ["-Ov", "-o", str(self.output / "Axiom_KU8.sorted.vcf")])
        self.run_command(cmd)

        ## 2. Remove duplicates
        cmd = (["bcftools"] + ["norm", "-d", "exact"]
               + [str(self.output / "Axiom_KU8.sorted.vcf")]
               + ["-Ov", "-o", str(self.output / "Axiom_KU8.sorted.nodup.vcf")])
        self.run_command(cmd)

        ## 3. Filter indels
        cmd = (["bcftools"] + ["view", "-e", 'REF ~ "D" || ALT ~ "D" || REF ~ "I" || ALT ~ "I"']
               + [str(self.output / "Axiom_KU8.sorted.nodup.vcf")]
               + ["-Ov", "-o", str(self.output / "Axiom_KU8.sorted.nodup.filtered.vcf")])
        self.run_command(cmd)

        ## 4. Remove brackets from alleles
        cmd = (["bcftools"] + ["view", "-e", 'REF ~ "[()]" || ALT ~ "[()]"']
               + [str(self.output / "Axiom_KU8.sorted.nodup.filtered.vcf")]
               + ["-Oz", "-o", str(self.output / "Axiom_KU8.sorted.nodup.filtered.fixed.vcf.gz")])
        self.run_command(cmd)

        ## 5. Index final VCF
        cmd = (["bcftools"] + ["index"]
               + [str(self.output / "Axiom_KU8.sorted.nodup.filtered.fixed.vcf.gz")])
        self.run_command(cmd)

        return str(self.output / "Axiom_KU8.sorted.nodup.filtered.fixed.vcf.gz")
