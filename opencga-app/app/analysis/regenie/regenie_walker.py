import argparse
import os
import subprocess
import sys

from variant_walker import VariantWalker
from typing import List, Iterable, Union, Iterator


class Regenie(VariantWalker):
    def setup(self, *args):
        self.args = args;
        self.vcf_filename = self.get_tempfile(prefix="data_", suffix=".vcf")
        self.f_vcf = open(self.vcf_filename, "w")
        print(f"## at setup: {self.vcf_filename}", file=sys.stderr)
        self.regenie_results = self.getTmpdir() + "/regenie_results"
        print(f"## at setup: {self.regenie_results}", file=sys.stderr)
        self.vcf_body_lines = 0
        self.last_body_line = None

    def header(self, header):
        # write the number of lines in the header
        if not header:
            print("## at header: no header provided", file=sys.stderr)
            return
        print(f"## at header: VCF header lines = {len(header)}", file=sys.stderr)
        self.f_vcf.writelines(f"{line}\n" for line in header)

    def map(self, line):
        self.vcf_body_lines += 1
        self.last_body_line = line
        self.f_vcf.write(f"{line}\n")

    def cleanup(self):
        # Close the VCF file
        self.f_vcf.close()

        first_chars = ""
        num_fields = 0
        if self.vcf_body_lines > 0:
            first_chars = str(self.last_body_line or '')[:50]
            fields = self.last_body_line.split()
            num_fields = len(fields)
        print(f"## at cleanup: num. variants = {self.vcf_body_lines}; last variant with {num_fields} fields, (first 50 chars): {first_chars} ", file=sys.stderr)

        if self.vcf_body_lines == 0:
            print("## at cleanup: skipping regenie step 2 since there is no variants", file=sys.stderr)
            self.write("CHROM GENPOS ID ALLELE0 ALLELE1 A1FREQ N TEST BETA SE CHISQ LOG10P EXTRA")
            return

        if self.vcf_body_lines == 1:
            print("## at cleanup: skipping regenie step 2 since there is only one variant", file=sys.stderr)
            self.write("CHROM GENPOS ID ALLELE0 ALLELE1 A1FREQ N TEST BETA SE CHISQ LOG10P EXTRA")
            return

        # Otherwise, run regenie step 2
        self.run_regenie_step2()
        out_filename = self.regenie_results + "_PHENO.regenie"
        if os.path.exists(out_filename):
            print(f"## at cleanup: dumping the content of {out_filename}", file=sys.stderr)
            with open(out_filename, "r") as f:
                for line in f:
                    self.write(line.rstrip())

    def run_regenie_step2(self):
        try:
            print(f"## at run_regenie_step2: num. variants = {self.vcf_body_lines}", file=sys.stderr)

            # 1. plink1.9 --vcf to --bed
            plink_filename = self.vcf_filename.removesuffix(".vcf")
            plink_cmd = [
                "plink1.9",
                "--vcf", self.vcf_filename,
                "--make-bed",
                "--out", plink_filename,
                "--silent"
            ]
            print(f"## at run_regenie_step2: {plink_cmd}", file=sys.stderr)
            subprocess.run(plink_cmd, check=True, stdout=sys.stderr, stderr=sys.stderr)

            # 2. regenie step 2
            regenie_cmd = [
                "regenie",
                "--step", "2",
                "--bed", plink_filename,
                "--out", self.regenie_results
            ]
            regenie_cmd.extend(self.args)
            print(f"## at run_regenie_step2: {regenie_cmd}", file=sys.stderr)
            subprocess.run(regenie_cmd, check=True, stdout=sys.stderr, stderr=sys.stderr)

        except subprocess.CalledProcessError as e:
            print(f"## at run_regenie_step2: command failed; error message: {str(e)}", file=sys.stderr)
            raise
        except FileNotFoundError as e:
            print(f"## at run_regenie_step2: file not found error: {str(e)}", file=sys.stderr)
            raise
        except Exception as e:
            print(f"## at run_regenie_step2: unexpected error: {str(e)}", file=sys.stderr)
            raise

    def get_tempfile(self, prefix: str = "", suffix: str = "") -> str:
        """
            Create a temporary file path (does not create the file).

        Args:
            prefix: Filename prefix
            suffix: Filename suffix (e.g., '.txt')
        """
        import tempfile
        return os.path.join(self.getTmpdir(), f"{prefix}{next(tempfile._get_candidate_names())}{suffix}")