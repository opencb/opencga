import sys
import argparse
import subprocess
from variant_walker import VariantWalker

class Regenie(VariantWalker):
    def setup(self, *args):
        # parser = argparse.ArgumentParser()
        # parser.add_argument('--length', default=10, help='The length to trim each line to.')
        # args = parser.parse_args(args)
        # self.length = int(args.length)
        self.vcf_file = self.get_tempfile(prefix="data_", suffix=".vcf")
        self.vcf_annotated_file = self.get_tempfile(prefix="data_", suffix=".vcf.gz")
        self.gwas_results = self.getTmpdir() + "/gwas_results"
        # self.write(f"vcf_file = {self.vcf_file}")
        # self.write(f"vcf_annotated_file = {self.vcf_annotated_file}")

    def header(self, header):
        self.fwrite_lines(self.vcf_file, header, "w")
        pass

    def map(self, line):
        self.fwrite_line(self.vcf_file, line, "a")
        pass

    def cleanup(self):
        self.run_regenie_step2()
        out_file = self.gwas_results + "_PHENO.regenie"
        # out_file = self.vcf_file
        for line in self.fread_lines(out_file):
            self.write(line.rstrip())
        pass

    def run_regenie_step2(self):
        try:
            # 1. bcftools annotate
            bcftools_cmd = [
                "bcftools", "annotate",
                "--set-id", "%CHROM:%POS:%REF:%FIRST_ALT",
                self.vcf_file,
                "-Oz", "-o", self.vcf_annotated_file
            ]
            # self.write(f"Running bcftools annotate {bcftools_cmd}...")
            subprocess.run(bcftools_cmd, check=True, stderr=subprocess.PIPE)

            # 2. plink1.9 --vcf to --bed
            plink_file = self.vcf_annotated_file.removesuffix(".vcf.gz")
            plink_cmd = [
                "plink1.9",
                "--vcf", self.vcf_annotated_file,
                "--make-bed",
                "--out", plink_file,
                "--silent"  # Suppress plink's verbose output
            ]
            # self.write(f"Running PLINK1.9 {plink_cmd}...")
            subprocess.run(plink_cmd, check=True, stderr=subprocess.PIPE)

            # 3. regenie step 2
            regenie_cmd = [
                "regenie",
                "--step", "2",
                "--bed", plink_file,
                "--bsize", "1000",
                "--out", self.gwas_results,
                "--phenoFile", "/regenie_walker/phenotype.txt",
                "--pred", "/regenie_walker/step1/step1_pred.list",
                "--bt"
            ]
            # self.write(f"Running regenie {regenie_cmd}...")
            subprocess.run(regenie_cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)

        except subprocess.CalledProcessError as e:
            print(f"Command failed: {e.cmd}", file=sys.stderr)
            print(f"Error message: {e.stderr.decode()}", file=sys.stderr)
            sys.exit(1)
        except FileNotFoundError as e:
            print(f"Required tool not found: {e}", file=sys.stderr)
            sys.exit(1)
        except Exception as e:
            print(f"Unexpected error: {str(e)}", file=sys.stderr)
            sys.exit(1)