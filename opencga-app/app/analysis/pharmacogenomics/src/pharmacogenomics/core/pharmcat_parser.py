"""Parse PharmCAT JSON output into AlleleTyperResult format."""

from __future__ import annotations

import json
import logging
from pathlib import Path

from pharmacogenomics.core.models import AlleleCall, AlleleTyperResult, Genotype, StarAlleleResult
from pharmacogenomics.core.pharmcat_runner import PharmcatOutput

logger = logging.getLogger(__name__)


class PharmcatParser:
    """Parse PharmCAT report/phenotype/match JSON into AlleleTyperResult."""

    def parse(self, outputs: list[PharmcatOutput]) -> list[AlleleTyperResult]:
        results = []
        for output in outputs:
            result = self._parse_single(output)
            if result:
                results.append(result)
        return results

    def _parse_single(self, output: PharmcatOutput) -> AlleleTyperResult | None:
        # Prefer report.json (most complete), then phenotype.json, then match.json
        if output.report_json and output.report_json.exists():
            return self._parse_report_json(output.report_json, output.sample_id)
        if output.phenotype_json and output.phenotype_json.exists():
            return self._parse_phenotype_json(output.phenotype_json, output.sample_id)
        if output.match_json and output.match_json.exists():
            return self._parse_match_json(output.match_json, output.sample_id)
        logger.warning("No parseable output for sample %s", output.sample_id)
        return None

    def _parse_report_json(self, path: Path, sample_id: str) -> AlleleTyperResult:
        logger.info("Parsing PharmCAT report JSON: %s", path)
        with open(path) as f:
            data = json.load(f)

        star_results = []
        genotypes = []
        additional_info = {
            "pharmcatVersion": data.get("pharmcatVersion", ""),
            "dataVersion": data.get("dataVersion", ""),
            "title": data.get("title", ""),
        }

        # Parse gene reports
        genes = data.get("genes", {})
        for gene_symbol, gene_data in genes.items():
            source_diplotypes = gene_data.get("sourceDiplotypes", [])

            if not source_diplotypes:
                star_results.append(StarAlleleResult(
                    gene=gene_symbol, diplotype="no translation available",
                ))
                continue

            for dip in source_diplotypes:
                allele1 = dip.get("allele1", "")
                allele2 = dip.get("allele2", "")
                label = dip.get("label", "")

                calls = []
                if allele1:
                    calls.append(AlleleCall(allele=allele1))
                if allele2:
                    calls.append(AlleleCall(allele=allele2))

                diplotype = label if label else f"{allele1}/{allele2}"

                star_results.append(StarAlleleResult(
                    gene=gene_symbol,
                    diplotype=diplotype,
                    allele_calls=calls,
                ))

            # Parse variants for genotypes
            variants = gene_data.get("variants", [])
            for var in variants:
                rsid = var.get("rsid", "")
                call = var.get("call", "")
                if rsid and call:
                    genotypes.append(Genotype(variant=rsid, genotype=call))

        # Store PharmCAT drug annotations in additionalInformation
        drugs = data.get("drugs", {})
        if drugs:
            additional_info["pharmcatDrugs"] = drugs

        return AlleleTyperResult(
            sample_id=sample_id,
            source="ngs",
            allele_typer_results=star_results,
            genotypes=genotypes,
            additional_information=additional_info,
        )

    def _parse_phenotype_json(self, path: Path, sample_id: str) -> AlleleTyperResult:
        logger.info("Parsing PharmCAT phenotype JSON: %s", path)
        with open(path) as f:
            data = json.load(f)

        star_results = []
        gene_reports = data.get("geneReports", {})

        for gene_symbol, gene_data in gene_reports.items():
            source_diplotypes = gene_data.get("sourceDiplotypes", [])
            if not source_diplotypes:
                star_results.append(StarAlleleResult(gene=gene_symbol, diplotype="no translation available"))
                continue
            for dip in source_diplotypes:
                allele1 = dip.get("allele1", "")
                allele2 = dip.get("allele2", "")
                label = dip.get("label", f"{allele1}/{allele2}")
                calls = []
                if allele1:
                    calls.append(AlleleCall(allele=allele1))
                if allele2:
                    calls.append(AlleleCall(allele=allele2))
                star_results.append(StarAlleleResult(gene=gene_symbol, diplotype=label, allele_calls=calls))

        return AlleleTyperResult(sample_id=sample_id, source="ngs", allele_typer_results=star_results)

    def _parse_match_json(self, path: Path, sample_id: str) -> AlleleTyperResult:
        logger.info("Parsing PharmCAT match JSON: %s", path)
        with open(path) as f:
            data = json.load(f)

        star_results = []
        for gene_result in data.get("results", []):
            gene = gene_result.get("gene", "")
            diplotypes = gene_result.get("diplotypes", [])

            if not diplotypes:
                star_results.append(StarAlleleResult(gene=gene, diplotype="no translation available"))
                continue

            for dip in diplotypes:
                name = dip.get("name", "")
                h1 = dip.get("haplotype1", {}).get("name", "")
                h2 = dip.get("haplotype2", {}).get("name", "")
                calls = []
                if h1:
                    calls.append(AlleleCall(allele=h1))
                if h2:
                    calls.append(AlleleCall(allele=h2))
                star_results.append(StarAlleleResult(gene=gene, diplotype=name, allele_calls=calls))

        return AlleleTyperResult(sample_id=sample_id, source="ngs", allele_typer_results=star_results)
