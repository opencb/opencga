import argparse
import os
import subprocess
import sys

from variant_walker import VariantWalker


class Regenie(VariantWalker):
    def setup(self, *args):
        self.args = args;
        self.vcf_file = self.get_tempfile(prefix="data_", suffix=".vcf")
        self.regenie_results = self.getTmpdir() + "/regenie_results"
        print(f"## setup: {self.vcf_file}", file=sys.stderr)
        # print(f"## setup: {self.vcf_annotated_file}", file=sys.stderr)
        print(f"## setup: {self.regenie_results}", file=sys.stderr)
        self.vcf_body_lines = 0

    def header(self, header):
        # write the number of lines in the header
        if not header:
            print("## header: no header provided", file=sys.stderr)
            return
        print(f"## header: {len(header)} lines", file=sys.stderr)
        self.fwrite_lines(self.vcf_file, header, "w")
        pass

    def map(self, line):
        self.vcf_body_lines += 1
        self.fwrite_line(self.vcf_file, line, "a")
        pass

    def cleanup(self):
        print(f"## cleanup: {self.vcf_body_lines} lines in VCF file; next run_regenie_step2", file=sys.stderr)
        self.run_regenie_step2()
        out_file = self.regenie_results + "_PHENO.regenie"
        print(f"## cleanup: dumping the content of {out_file}", file=sys.stderr)
        for line in self.fread_lines(out_file):
            self.write(line.rstrip())
        pass

    def run_regenie_step2(self):
        try:
            print(f"## run_regenie_step2: VCF file {self.vcf_file}", file=sys.stderr)
            print(f"## run_regenie_step2: VCF body lines: {self.vcf_body_lines}", file=sys.stderr)

            # Check if the VCF file exists physically
            if not os.path.exists(self.vcf_file):
                print(f"## run_regenie_step2: VCF file {self.vcf_file} does not exist", file=sys.stderr)
                sys.exit(1)

            # 1. plink1.9 --vcf to --bed
            plink_file = self.vcf_file.removesuffix(".vcf")
            plink_cmd = [
                "plink1.9",
                "--vcf", self.vcf_file,
                "--make-bed",
                "--out", plink_file,
                "--silent"  # Suppress plink's verbose output
            ]
            print(f"## run_regenie_step2: {plink_cmd}", file=sys.stderr)
            subprocess.run(plink_cmd, check=True, stderr=subprocess.PIPE)

            # 2. regenie step 2
            regenie_cmd = [
                "regenie",
                "--step", "2",
                "--bed", plink_file,
                "--out", self.regenie_results
            ]
            regenie_cmd.extend(self.args)
            print(f"## run_regenie_step2: {regenie_cmd}", file=sys.stderr)

            subprocess.run(regenie_cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)

        except subprocess.CalledProcessError as e:
            print(f"## run_regenie_step2: command failed, {e.cmd}", file=sys.stderr)
            print(f"## run_regenie_step2: error message, {str(e)}", file=sys.stderr)
            sys.exit(1)
        except FileNotFoundError as e:
            print(f"## run_regenie_step2: file not found error, {str(e)}", file=sys.stderr)
            sys.exit(1)
        except Exception as e:
            print(f"## run_regenie_step2: unexpected error, {str(e)}", file=sys.stderr)
            sys.exit(1)