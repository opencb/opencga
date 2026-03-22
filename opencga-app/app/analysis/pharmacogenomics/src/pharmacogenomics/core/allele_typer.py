"""AlleleTyper: infer star alleles from ThermoFisher OpenArray genotyping data.

Port of the Java AlleleTyper class.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path

from pharmacogenomics.core.models import (
    AlleleCall,
    AlleleTyperResult,
    AssayDefinition,
    Genotype,
    StarAlleleResult,
    TranslationInfo,
)

logger = logging.getLogger(__name__)


@dataclass
class HaplotypeDefinition:
    gene: str
    allele: str
    assay_values: dict[str, str] = field(default_factory=dict)


@dataclass
class DiplotypePair:
    h1: HaplotypeDefinition
    h2: HaplotypeDefinition

    def __str__(self) -> str:
        a1, a2 = self.h1.allele, self.h2.allele
        if _compare_star_alleles(a1, a2) <= 0:
            return f"{a1}/{a2}"
        return f"{a2}/{a1}"


def _compare_star_alleles(a1: str, a2: str) -> int:
    """Compare alleles with biologically meaningful ordering."""
    n1 = _extract_star_number(a1)
    n2 = _extract_star_number(a2)
    if n1 >= 0 and n2 >= 0:
        if n1 != n2:
            return n1 - n2
        return (a1 > a2) - (a1 < a2)

    # RS-based: wt/Ref before Alt
    a1_ref = "wt" in a1 or "Ref" in a1
    a2_ref = "wt" in a2 or "Ref" in a2
    a1_alt = "Alt" in a1
    a2_alt = "Alt" in a2
    if a1_ref and a2_alt:
        return -1
    if a1_alt and a2_ref:
        return 1

    return (a1 > a2) - (a1 < a2)


def _extract_star_number(allele: str) -> int:
    if not allele or not allele.startswith("*"):
        return -1
    end = 1
    while end < len(allele) and allele[end].isdigit():
        end += 1
    if end > 1:
        return int(allele[1:end])
    return -1


class AlleleTyper:
    def __init__(self):
        self.gene_haplotypes: dict[str, list[HaplotypeDefinition]] = {}
        self.gene_relevant_assays: dict[str, set[str]] = {}
        self.cnv_column_names: set[str] = set()
        self.assay_to_rsid: dict[str, str] = {}
        self.assay_to_column: dict[str, int] = {}
        self.sample_cnv_data: dict[str, dict[str, int]] = {}
        self.allele_renames: dict[str, dict[str, str]] = {}

    # --- Translation parsing ---

    def parse_translation_file(self, path: Path) -> None:
        logger.info("Parsing translation file: %s", path)
        with open(path) as f:
            self._parse_translation(f)

    def parse_translation_string(self, content: str) -> None:
        self._parse_translation(content.splitlines(keepends=True))

    def _parse_translation(self, lines) -> None:
        line_number = 0
        rs_ids: list[str] = []
        assay_ids: list[str] = []

        for line in lines:
            if isinstance(line, str):
                line = line.rstrip("\n\r")
            line_number += 1

            if line_number == 1:
                continue
            fields = line.split("\t")
            if line_number == 2:
                rs_ids = fields
                continue
            if line_number == 3:
                assay_ids = fields
                self._build_assay_maps(assay_ids, rs_ids)
                continue
            if len(fields) >= 3:
                self._parse_haplotype_definition(fields, assay_ids)

        self._compute_gene_relevant_assays()
        logger.info("Parsed %d genes, %d CNV columns", len(self.gene_haplotypes), len(self.cnv_column_names))

    def _build_assay_maps(self, assay_ids: list[str], rs_ids: list[str]) -> None:
        min_len = min(len(assay_ids), len(rs_ids))
        for i in range(min_len):
            assay_id = assay_ids[i].strip()
            rs_id = rs_ids[i].strip()
            is_cnv = assay_id.lower().endswith("_cn")
            is_assay = assay_id.startswith("C_") or assay_id.startswith("ANGZ") \
                or assay_id.startswith("ANM") or assay_id.startswith("ANER")
            if assay_id and (is_assay or is_cnv):
                self.assay_to_column[assay_id] = i
                if rs_id:
                    self.assay_to_rsid[assay_id] = rs_id
                if is_cnv:
                    self.cnv_column_names.add(assay_id)
        logger.info("Mapped %d assays, %d CNV columns", len(self.assay_to_column), len(self.cnv_column_names))

    def _parse_haplotype_definition(self, fields: list[str], assay_ids: list[str]) -> None:
        gene = fields[0].strip()
        allele = fields[1].strip()
        if not gene or not allele:
            return
        assay_values = {}
        for assay_id, col_idx in self.assay_to_column.items():
            if col_idx < len(fields):
                assay_values[assay_id] = fields[col_idx].strip()
        hap = HaplotypeDefinition(gene=gene, allele=allele, assay_values=assay_values)
        self.gene_haplotypes.setdefault(gene, []).append(hap)

    def _compute_gene_relevant_assays(self) -> None:
        for gene, haps in self.gene_haplotypes.items():
            relevant = set()
            for hap in haps:
                for assay_id, value in hap.assay_values.items():
                    if value:
                        relevant.add(assay_id)
            self.gene_relevant_assays[gene] = relevant

    # --- CNV parsing ---

    def parse_cnv_file(self, path: Path) -> None:
        logger.info("Parsing CNV file: %s", path)
        with open(path) as f:
            self._parse_cnv(f)

    def _parse_cnv(self, lines) -> None:
        sample_name_idx = cn_predicted_idx = target_idx = -1
        header_parsed = False
        for line in lines:
            if isinstance(line, str):
                line = line.rstrip("\n\r")
            if not line.strip():
                header_parsed = False
                continue
            fields = line.split("\t")
            if not header_parsed:
                sample_name_idx = cn_predicted_idx = target_idx = -1
                for i, h in enumerate(fields):
                    h = h.strip()
                    if h == "Sample Name":
                        sample_name_idx = i
                    elif h == "CN Predicted":
                        cn_predicted_idx = i
                    elif h == "Target":
                        target_idx = i
                if sample_name_idx >= 0 and cn_predicted_idx >= 0:
                    header_parsed = True
                continue
            if sample_name_idx < len(fields) and cn_predicted_idx < len(fields):
                sample = fields[sample_name_idx].strip()
                cn_str = fields[cn_predicted_idx].strip()
                target = fields[target_idx].strip() if target_idx >= 0 and target_idx < len(fields) else ""
                if sample and cn_str:
                    try:
                        self.sample_cnv_data.setdefault(sample, {})[target] = int(cn_str)
                    except ValueError:
                        pass
        logger.info("Parsed CNV data for %d samples", len(self.sample_cnv_data))

    # --- Rename file parsing ---

    def parse_rename_file(self, path: Path) -> None:
        logger.info("Parsing rename file: %s", path)
        header_skipped = False
        with open(path) as f:
            for line in f:
                line = line.rstrip("\n\r")
                if not line.strip():
                    continue
                if not header_skipped:
                    header_skipped = True
                    continue
                fields = line.split("\t")
                if len(fields) >= 3:
                    gene, orig, new_name = fields[0].strip(), fields[1].strip(), fields[2].strip()
                    if gene and orig and new_name:
                        self.allele_renames.setdefault(gene, {})[orig] = new_name
        logger.info("Parsed allele renames for %d genes", len(self.allele_renames))

    def apply_renames(self, results: list[AlleleTyperResult]) -> None:
        if not self.allele_renames:
            return
        for result in results:
            for star in result.allele_typer_results:
                gene_renames = self.allele_renames.get(star.gene)
                if not gene_renames:
                    continue
                if star.allele_calls:
                    for call in star.allele_calls:
                        renamed = gene_renames.get(call.allele)
                        if renamed:
                            call.renamed_allele = renamed
                    if len(star.allele_calls) == 2:
                        c1, c2 = star.allele_calls
                        r1 = c1.renamed_allele if c1.renamed_allele else c1.allele
                        r2 = c2.renamed_allele if c2.renamed_allele else c2.allele
                        star.renamed_diplotype = f"{r1}/{r2}"

    # --- Genotyping parsing and result building ---

    def build_allele_typer_results(self, genotyping_file: Path) -> list[AlleleTyperResult]:
        with open(genotyping_file) as f:
            return self._build_results_from_reader(f)

    def build_allele_typer_results_from_string(self, content: str) -> list[AlleleTyperResult]:
        return self._build_results_from_reader(content.splitlines(keepends=True))

    def _build_results_from_reader(self, lines) -> list[AlleleTyperResult]:
        sample_genotypes = self._parse_genotypes(lines)
        logger.info("Parsed genotypes for %d samples", len(sample_genotypes))

        results = []
        for sample_id, genotypes in sample_genotypes.items():
            cnv_data = self.sample_cnv_data.get(sample_id, {})
            result = self._build_sample_result(sample_id, genotypes, cnv_data)
            results.append(result)
        return results

    def _parse_genotypes(self, lines) -> dict[str, dict[str, str]]:
        sample_genotypes: dict[str, dict[str, str]] = {}
        header_parsed = False
        sample_id_idx = assay_name_idx = call_idx = -1

        for line in lines:
            if isinstance(line, str):
                line = line.rstrip("\n\r")
            if line.startswith("#") or not line.strip():
                continue
            fields = line.split("\t")
            if not header_parsed:
                for i, h in enumerate(fields):
                    h = h.strip()
                    if h == "Sample ID":
                        sample_id_idx = i
                    elif h == "Assay Name":
                        assay_name_idx = i
                    elif h == "Call":
                        call_idx = i
                header_parsed = True
                continue
            if all(idx >= 0 and idx < len(fields) for idx in [sample_id_idx, assay_name_idx, call_idx]):
                sample = fields[sample_id_idx].strip()
                assay = fields[assay_name_idx].strip()
                call = fields[call_idx].strip()
                if sample and assay:
                    sample_genotypes.setdefault(sample, {})[assay] = call
        return sample_genotypes

    def _build_sample_result(
        self, sample_id: str, genotypes: dict[str, str], cnv_data: dict[str, int]
    ) -> AlleleTyperResult:
        star_results = []
        for gene, haplotypes in self.gene_haplotypes.items():
            relevant = self.gene_relevant_assays.get(gene, set())
            gene_variants = list(relevant)
            pairs = self._call_diplotype(gene, haplotypes, relevant, genotypes, cnv_data)
            if not pairs:
                star_results.append(StarAlleleResult(
                    gene=gene, diplotype="no translation available", allele_calls=[], variants=gene_variants,
                ))
            else:
                for pair in pairs:
                    calls = [AlleleCall(allele=pair.h1.allele), AlleleCall(allele=pair.h2.allele)]
                    star_results.append(StarAlleleResult(
                        gene=gene, diplotype=str(pair), allele_calls=calls, variants=gene_variants,
                    ))

        geno_list = [Genotype(variant=k, genotype=v) for k, v in genotypes.items()]
        translation_list = self._build_translation_info()
        return AlleleTyperResult(
            sample_id=sample_id, source="openarray",
            allele_typer_results=star_results, genotypes=geno_list, translation=translation_list,
        )

    def _build_translation_info(self) -> list[TranslationInfo]:
        result = []
        for gene, haps in self.gene_haplotypes.items():
            gene_assays: dict[str, str] = {}
            for h in haps:
                for aid, val in h.assay_values.items():
                    if aid not in gene_assays:
                        gene_assays[aid] = val
            assay_defs = [AssayDefinition(id=aid, allele=val) for aid, val in gene_assays.items()]
            result.append(TranslationInfo(gene=gene, assays=assay_defs))
        return result

    # --- Diplotype calling ---

    def _call_diplotype(
        self, gene: str, haplotypes: list[HaplotypeDefinition], relevant_assays: set[str],
        observed: dict[str, str], cnv_data: dict[str, int],
    ) -> list[DiplotypePair]:
        snp_assays = {a for a in relevant_assays if a not in self.cnv_column_names}
        cnv_assays = {a for a in relevant_assays if a in self.cnv_column_names}
        gene_has_cnv = bool(cnv_assays)

        compatible = []
        for i in range(len(haplotypes)):
            for j in range(i, len(haplotypes)):
                h1, h2 = haplotypes[i], haplotypes[j]
                if self._is_pair_compatible(h1, h2, snp_assays, cnv_assays, observed, cnv_data, gene_has_cnv):
                    compatible.append(DiplotypePair(h1, h2))
        return compatible

    def _is_pair_compatible(
        self, h1: HaplotypeDefinition, h2: HaplotypeDefinition,
        snp_assays: set[str], cnv_assays: set[str],
        observed: dict[str, str], cnv_data: dict[str, int], gene_has_cnv: bool,
    ) -> bool:
        for assay in snp_assays:
            obs = observed.get(assay, "")
            exp1 = h1.assay_values.get(assay, "")
            exp2 = h2.assay_values.get(assay, "")
            if not self._is_assay_compatible(obs, exp1, exp2, gene_has_cnv):
                return False
        if cnv_assays and cnv_data:
            if not self._check_cnv_constraints(h1, h2, cnv_assays, cnv_data):
                return False
        return True

    def _is_assay_compatible(self, observed: str, exp1: str, exp2: str, gene_has_cnv: bool) -> bool:
        if not observed or observed == "UND":
            return True
        if observed.lower() == "noamp":
            if gene_has_cnv:
                return (not exp1 or exp1.lower() == "noamp") and (not exp2 or exp2.lower() == "noamp")
            return True
        if not exp1 and not exp2:
            return True
        if not exp1:
            return self._wildcard_matches_side(observed, exp2)
        if not exp2:
            return self._wildcard_matches_side(observed, exp1)
        if exp1.lower() == "noamp" and exp2.lower() == "noamp":
            return False

        alleles = observed.split("/")
        if len(alleles) == 2:
            a, b = alleles[0].strip(), alleles[1].strip()
            if a == b:  # homozygous
                if self._allele_eq(exp1, a) and self._allele_eq(exp2, a):
                    return True
                if self._allele_eq(exp1, a) and exp2.lower() == "noamp":
                    return True
                if exp1.lower() == "noamp" and self._allele_eq(exp2, a):
                    return True
                return False
            # heterozygous
            return (self._allele_eq(exp1, a) and self._allele_eq(exp2, b)) \
                or (self._allele_eq(exp1, b) and self._allele_eq(exp2, a))

        # single allele
        if self._allele_eq(exp1, observed) and self._allele_eq(exp2, observed):
            return True
        if self._allele_eq(exp1, observed) and exp2.lower() == "noamp":
            return True
        if exp1.lower() == "noamp" and self._allele_eq(exp2, observed):
            return True
        return False

    def _wildcard_matches_side(self, observed: str, defined: str) -> bool:
        if defined.lower() == "noamp":
            return True
        for obs in observed.split("/"):
            if self._allele_eq(defined, obs.strip()):
                return True
        return False

    @staticmethod
    def _allele_eq(expected: str, observed: str) -> bool:
        if not expected or not observed:
            return False
        return expected.lower() == observed.lower()

    def _check_cnv_constraints(
        self, h1: HaplotypeDefinition, h2: HaplotypeDefinition,
        cnv_assays: set[str], cnv_data: dict[str, int],
    ) -> bool:
        for assay in cnv_assays:
            exp1 = h1.assay_values.get(assay, "")
            exp2 = h2.assay_values.get(assay, "")
            if not exp1 and not exp2:
                continue
            observed_cn = cnv_data.get(assay)
            if observed_cn is None:
                continue
            min_sum = 0
            has_ge = False
            for exp in [exp1, exp2]:
                if exp:
                    if exp.startswith(">="):
                        has_ge = True
                        min_sum += int(exp[2:].strip() or "0")
                    else:
                        try:
                            min_sum += int(exp.strip())
                        except ValueError:
                            pass
            if has_ge:
                if observed_cn < min_sum:
                    return False
            elif observed_cn != min_sum:
                return False
        return True
