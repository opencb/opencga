import argparse
import os
import subprocess
import sys

from variant_walker import VariantWalker
from typing import List, Iterable, Union, Iterator


class Regenie(VariantWalker):
    def setup(self, *args):
        self.args = args;
        self.vcf_file = self.get_tempfile(prefix="data_", suffix=".vcf")
        self.regenie_results = self.getTmpdir() + "/regenie_results"
        print(f"## at setup: {self.vcf_file}", file=sys.stderr)
        print(f"## at setup: {self.regenie_results}", file=sys.stderr)
        self.vcf_body_lines = 0
        self.last_body_line = None

    def header(self, header):
        # write the number of lines in the header
        if not header:
            print("## at header: no header provided", file=sys.stderr)
            return
        print(f"## at header: VCF header lines = {len(header)}", file=sys.stderr)
        self.fwrite_lines(self.vcf_file, header, "w")
        pass

    def map(self, line):
        self.vcf_body_lines += 1
        self.last_body_line = line
        self.fwrite_line(self.vcf_file, line, "a")
        pass

    def cleanup(self):
        if self.vcf_body_lines == 0:
            print("## at cleanup: skipping regenie step 2 since there is no variants", file=sys.stderr)
            return
        first_chars = str(self.last_body_line or '')[:50]
        fields = self.last_body_line.split()
        num_fields = len(fields)
        print(f"## at cleanup: before running regenie step2; num. variants = {self.vcf_body_lines}; last variant with {num_fields} fields, (first 50 chars): {first_chars} ", file=sys.stderr)
        if self.vcf_body_lines == 1:
            print("## at cleanup: skipping regenie step 2 since there is only one variant", file=sys.stderr)
            return

        # Otherwise, run regenie step 2
        self.run_regenie_step2()
        out_file = self.regenie_results + "_PHENO.regenie"
        if os.path.exists(out_file):
            print(f"## at cleanup: dumping the content of {out_file}", file=sys.stderr)
            for line in self.fread_lines(out_file):
                self.write(line.rstrip())
        pass

    def run_regenie_step2(self):
        try:
            # 1. plink1.9 --vcf to --bed
            plink_file = self.vcf_file.removesuffix(".vcf")
            plink_cmd = [
                "plink1.9",
                "--vcf", self.vcf_file,
                "--make-bed",
                "--out", plink_file,
                "--silent"  # Suppress plink's verbose output
            ]
            print(f"## at run_regenie_step2: {plink_cmd}", file=sys.stderr)
            # subprocess.run(plink_cmd, check=True, stderr=subprocess.PIPE)
            subprocess.run(plink_cmd, check=True, stdout=sys.stderr, stderr=sys.stderr)

            # 2. regenie step 2
            regenie_cmd = [
                "regenie",
                "--step", "2",
                "--bed", plink_file,
                "--out", self.regenie_results
            ]
            regenie_cmd.extend(self.args)
            print(f"## at run_regenie_step2: {regenie_cmd}", file=sys.stderr)

            # subprocess.run(regenie_cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
            subprocess.run(regenie_cmd, check=True, stdout=sys.stderr, stderr=sys.stderr)

        except subprocess.CalledProcessError as e:
            print(f"## at run_regenie_step2: command failed; error message: {str(e)}", file=sys.stderr)
            sys.exit(1)
        except FileNotFoundError as e:
            print(f"## at run_regenie_step2: file not found error: {str(e)}", file=sys.stderr)
            sys.exit(1)
        except Exception as e:
            print(f"## at run_regenie_step2: unexpected error: {str(e)}", file=sys.stderr)
            sys.exit(1)

    def fwrite_line(self, filename: str, line: str, mode: str = "a") -> None:
        """
        Append a line to a file (with newline).

        Args:
            filename: Path to the file
            line: Text to append
            mode: File mode ('a' for append by default)
        """
        with open(filename, mode) as f:
            f.write(f"{line}\n")

    def fwrite_lines(self, filename: str, lines: Union[List[str], Iterable[str]], mode: str = "w") -> None:
        """
        Write an array/iterable of lines to a file.

        Args:
            filename: Path to the target file
            lines: List/iterable of strings to write
            mode: File mode ('w' for overwrite, 'a' for append)

        Example:
            parent.write_lines("log.txt", ["Line 1", "Line 2"], mode="a")
        """
        with open(filename, mode) as f:
            f.writelines(
                f"{line}\n"
                for line in lines
            )

    def fread_lines(self, filename: str) -> Iterator[str]:
        """
        Read file line-by-line (memory-efficient for large files).

        Usage:
            for line in parent.read_lines("file.txt"):
                print(line.strip())
        """
        with open(filename, "r") as f:
            yield from f

    def get_tempfile(self, prefix: str = "", suffix: str = "") -> str:
        """
            Create a temporary file path (does not create the file).

        Args:
            prefix: Filename prefix
            suffix: Filename suffix (e.g., '.txt')
        """
        import tempfile
        return os.path.join(self.getTmpdir(), f"{prefix}{next(tempfile._get_candidate_names())}{suffix}")