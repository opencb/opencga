"""Comparison benchmark: compare AlleleTyper results with TrueMark official results.

Produces a report matching the format from the Java AlleleTyperTest comparison.
"""

from __future__ import annotations

import logging
import re
from pathlib import Path

from pharmacogenomics.core.models import AlleleTyperResult

logger = logging.getLogger(__name__)

CONTROL_PREFIXES = ("NTC", "CALT", "CHET", "CREF")


class ComparisonReport:
    """Compare obtained AlleleTyperResult list against TrueMark detailed results file."""

    def run(
        self,
        results: list[AlleleTyperResult],
        expected_file: Path,
        report_file: Path,
    ) -> None:
        # Build obtained: sample -> gene -> list of diplotypes
        obtained: dict[str, dict[str, list[str]]] = {}
        renamed: dict[str, dict[str, list[str]]] = {}
        for result in results:
            gene_map: dict[str, list[str]] = {}
            renamed_map: dict[str, list[str]] = {}
            for star in result.allele_typer_results:
                gene_map.setdefault(star.gene, []).append(star.diplotype)
                if star.renamed_diplotype:
                    renamed_map.setdefault(star.gene, []).append(star.renamed_diplotype)
            obtained[result.sample_id] = gene_map
            renamed[result.sample_id] = renamed_map

        # Parse expected results
        expected = self._parse_detailed_tsv(expected_file)

        # Collect all genes from obtained
        all_genes: list[str] = []
        seen = set()
        for gene_map in obtained.values():
            for g in gene_map:
                if g not in seen:
                    all_genes.append(g)
                    seen.add(g)

        # Write report
        self._write_report(all_genes, obtained, renamed, expected, report_file)

    def _parse_detailed_tsv(self, path: Path) -> dict[str, dict[str, str]]:
        """Parse TrueMark detailed results file (tab-separated, possibly CR-only line endings)."""
        content = path.read_text(encoding="utf-8", errors="replace")
        lines = re.split(r"\r\n|\r|\n", content)

        # Find header line containing 'sample ID'
        header_line = None
        header_idx = -1
        for i, line in enumerate(lines):
            if "sample ID" in line:
                header_line = line
                header_idx = i
                break

        if header_line is None:
            logger.warning("'sample ID' column not found in detailed results: %s", path)
            return {}

        headers = header_line.split("\t")

        # Find sample ID index and gene columns (between 'sample ID' and 'Notes')
        sample_id_idx = -1
        notes_idx = -1
        for i, h in enumerate(headers):
            h = h.strip()
            if h.lower() == "sample id":
                sample_id_idx = i
            elif h.lower() == "notes" and sample_id_idx >= 0:
                notes_idx = i
                break

        if sample_id_idx < 0:
            logger.warning("'sample ID' index not found in headers")
            return {}
        if notes_idx < 0:
            notes_idx = len(headers)

        gene_names = [headers[i].strip() for i in range(sample_id_idx + 1, notes_idx)]
        logger.info("Detailed results gene columns (%d): %s", len(gene_names), gene_names)

        results: dict[str, dict[str, str]] = {}
        for line_idx in range(header_idx + 1, len(lines)):
            line = lines[line_idx]
            if not line.strip():
                continue
            fields = line.split("\t")
            if len(fields) <= sample_id_idx:
                continue
            sample_id = fields[sample_id_idx].strip()
            if not sample_id or sample_id.startswith(CONTROL_PREFIXES):
                continue

            gene_results: dict[str, str] = {}
            for i, gene in enumerate(gene_names):
                col_idx = sample_id_idx + 1 + i
                if col_idx < len(fields):
                    gene_results[gene] = fields[col_idx].strip()
            results[sample_id] = gene_results

        logger.info("Parsed %d samples from detailed results", len(results))
        return results

    def _write_report(
        self,
        genes: list[str],
        obtained: dict[str, dict[str, list[str]]],
        renamed: dict[str, dict[str, list[str]]],
        expected: dict[str, dict[str, str]],
        report_file: Path,
    ) -> None:
        separator = "-" * 170

        with open(report_file, "w") as f:
            f.write("=== COMPREHENSIVE COMPARISON WITH OFFICIAL RESULTS ===\n\n")

            total_genes = 0
            total_matches = 0
            total_mismatches = 0
            total_no_translation = 0

            for gene in genes:
                f.write(f"\n=== {gene} Results Comparison ===\n")
                f.write(f"{'Sample':<15} {'Expected':<45} {'Obtained':<45} {'Renamed':<45} {'Match'}\n")
                f.write(f"{separator}\n")

                gene_matches = 0
                gene_mismatches = 0
                gene_no_translation = 0
                gene_samples = 0

                for sample_id, sample_expected in expected.items():
                    exp_val = sample_expected.get(gene)
                    sample_obtained = obtained.get(sample_id)
                    if sample_obtained is None:
                        continue

                    gene_samples += 1
                    obt_val = _join_diplotypes(sample_obtained.get(gene))

                    # Get renamed
                    sample_renamed = renamed.get(sample_id, {})
                    ren_val = _join_diplotypes(sample_renamed.get(gene))

                    exp_no = not exp_val or exp_val == "no translation available"
                    obt_no = not obt_val or obt_val == "no translation available"

                    exp_str = "no translation available" if exp_no else exp_val
                    obt_str = "no translation available" if obt_no else obt_val

                    if exp_no and obt_no:
                        match_status = "CORRECT (no translation)"
                        gene_no_translation += 1
                    elif exp_str == obt_str:
                        match_status = "EXACT"
                        gene_matches += 1
                    elif not exp_no and not obt_no and _normalized_match(exp_str, obt_str):
                        match_status = "MATCH (normalized)"
                        gene_matches += 1
                    else:
                        match_status = "MISMATCH"
                        gene_mismatches += 1

                    f.write(
                        f"{sample_id:<15} "
                        f"{_truncate(exp_str, 45):<45} "
                        f"{_truncate(obt_str, 45):<45} "
                        f"{_truncate(ren_val, 45):<45} "
                        f"{match_status}\n"
                    )

                if gene_samples > 0:
                    f.write(f"{separator}\n")
                    gene_total = gene_matches + gene_no_translation
                    accuracy = 100.0 * gene_total / gene_samples
                    f.write(
                        f"Total: {gene_samples}, Exact matches: {gene_matches}, "
                        f"No translation: {gene_no_translation}, Accuracy: {accuracy:.1f}%\n"
                    )
                f.write("\n")

                total_genes += 1
                total_matches += gene_matches
                total_mismatches += gene_mismatches
                total_no_translation += gene_no_translation

            f.write("\n=== OVERALL SUMMARY ===\n")
            overall_total = total_matches + total_no_translation + total_mismatches
            overall_correct = total_matches + total_no_translation
            accuracy = 100.0 * overall_correct / overall_total if overall_total > 0 else 0.0
            f.write(
                f"Genes: {total_genes}, Total comparisons: {overall_total}, "
                f"Matches: {total_matches}, No translation: {total_no_translation}, "
                f"Mismatches: {total_mismatches}, Accuracy: {accuracy:.1f}%\n"
            )

        # Also print to stdout
        print(report_file.read_text())


def _join_diplotypes(diplotypes: list[str] | None) -> str:
    if not diplotypes:
        return ""
    if len(diplotypes) == 1:
        return diplotypes[0]
    return "{" + ", ".join(diplotypes) + "}"


def _normalized_match(expected: str, obtained: str) -> bool:
    return _normalize(expected) == _normalize(obtained)


def _normalize(diplotype: str) -> str:
    if not diplotype:
        return ""
    clean = re.sub(r"[{}]", "", diplotype).strip()
    parts = clean.split(",")
    normalized = []
    for part in parts:
        trimmed = part.strip()
        alleles = trimmed.split("/")
        if len(alleles) == 2:
            alleles.sort()
            normalized.append(f"{alleles[0]}/{alleles[1]}")
        else:
            normalized.append(trimmed)
    normalized.sort()
    return str(normalized)


def _truncate(s: str, max_len: int) -> str:
    if not s or len(s) <= max_len:
        return s or ""
    return s[: max_len - 3] + "..."
