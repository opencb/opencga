"""PharmCAT runner: execute PharmCAT via local JAR or Docker container."""

from __future__ import annotations

import logging
import os
import subprocess
from dataclasses import dataclass
from pathlib import Path

logger = logging.getLogger(__name__)


@dataclass
class PharmcatOutput:
    """Paths to PharmCAT output files for a single sample."""
    sample_id: str
    match_json: Path | None = None
    phenotype_json: Path | None = None
    report_json: Path | None = None
    report_html: Path | None = None


class PharmcatRunner:
    def __init__(
        self,
        pharmcat_path: str | None = None,
        docker_image: str = "pgkb/pharmcat",
        timeout: int = 600,
    ):
        self.pharmcat_path = pharmcat_path
        self.docker_image = docker_image
        self.timeout = timeout

    def run(self, vcf_file: Path, outdir: Path) -> list[PharmcatOutput]:
        """Run PharmCAT on a VCF file and return paths to output files."""
        vcf_file = vcf_file.resolve()
        outdir = outdir.resolve()
        outdir.mkdir(parents=True, exist_ok=True)

        if self.pharmcat_path:
            return self._run_local(vcf_file, outdir)
        return self._run_docker(vcf_file, outdir)

    def _run_local(self, vcf_file: Path, outdir: Path) -> list[PharmcatOutput]:
        """Run PharmCAT using local JAR file."""
        cmd = [
            "java", "-jar", self.pharmcat_path,
            "-vcf", str(vcf_file),
            "-o", str(outdir),
            "-reporterJson",
        ]
        logger.debug("Running PharmCAT: %s", " ".join(cmd))

        result = subprocess.run(cmd, capture_output=True, text=True, timeout=self.timeout)
        if result.returncode != 0:
            logger.error("PharmCAT stderr: %s", result.stderr)
            raise RuntimeError(f"PharmCAT failed with exit code {result.returncode}: {result.stderr}")

        logger.info("PharmCAT completed successfully")
        return self._collect_outputs(outdir, vcf_file.stem)

    def _run_docker(self, vcf_file: Path, outdir: Path) -> list[PharmcatOutput]:
        """Run PharmCAT using Docker container."""
        input_dir = vcf_file.parent
        vcf_name = vcf_file.name

        cmd = [
            "docker", "run", "--rm",
            "--user", f"{os.getuid()}:{os.getgid()}",
            "-v", f"{input_dir}:/data:ro",
            "-v", f"{outdir}:/output",
            self.docker_image,
            "java", "-jar", "/pharmcat/pharmcat.jar",
            "-vcf", f"/data/{vcf_name}",
            "-o", "/output",
            "-reporterJson",
        ]
        logger.debug("Running PharmCAT Docker: %s", " ".join(cmd))

        result = subprocess.run(cmd, capture_output=True, text=True, timeout=self.timeout)
        if result.returncode != 0:
            logger.error("PharmCAT Docker stderr: %s", result.stderr)
            raise RuntimeError(f"PharmCAT Docker failed with exit code {result.returncode}: {result.stderr}")

        logger.info("PharmCAT Docker completed successfully")
        return self._collect_outputs(outdir, vcf_file.stem)

    def _collect_outputs(self, outdir: Path, base_name: str) -> list[PharmcatOutput]:
        """Collect PharmCAT output files from the output directory."""
        outputs = []

        # PharmCAT generates files with pattern: <base_name>.<suffix>
        match_json = outdir / f"{base_name}.match.json"
        phenotype_json = outdir / f"{base_name}.phenotype.json"
        report_json = outdir / f"{base_name}.report.json"
        report_html = outdir / f"{base_name}.report.html"

        output = PharmcatOutput(
            sample_id=base_name,
            match_json=match_json if match_json.exists() else None,
            phenotype_json=phenotype_json if phenotype_json.exists() else None,
            report_json=report_json if report_json.exists() else None,
            report_html=report_html if report_html.exists() else None,
        )

        if output.report_json or output.phenotype_json or output.match_json:
            outputs.append(output)
        else:
            logger.warning("No PharmCAT output files found for %s in %s", base_name, outdir)

        return outputs
