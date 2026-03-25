"""CPIC annotation for pharmacogenomics results.

Queries the CPIC REST API (https://api.cpicpgx.org/v1) to annotate
diplotypes with phenotype, drug recommendations, and allele function info.
"""

from __future__ import annotations

import logging
from typing import Any

import requests

from pharmacogenomics.core.models import (
    AlleleTyperResult,
    CpicAlleleAnnotation,
    CpicAlleleInfo,
    CpicDiplotypeAnnotation,
    CpicDiplotypeInfo,
    CpicDrug,
    CpicDrugRecommendation,
)

logger = logging.getLogger(__name__)


class CpicAnnotator:
    """Annotate AlleleTyperResult with CPIC diplotype and drug data."""

    def __init__(self, base_url: str = "https://api.cpicpgx.org/v1"):
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        # Cache to avoid redundant API calls
        self._diplotype_cache: dict[str, list[dict]] = {}
        self._allele_cache: dict[str, list[dict]] = {}
        self._pair_cache: dict[str, list[dict]] = {}
        self._recommendation_cache: dict[str, list[dict]] = {}
        self._drug_name_cache: dict[str, str] = {}  # drugid -> name

    def annotate_results(self, results: list[AlleleTyperResult]) -> None:
        for result in results:
            if result.sample_id == "NTC":
                continue
            self._annotate_sample(result)

    def _annotate_sample(self, result: AlleleTyperResult) -> None:
        for star in result.allele_typer_results:
            if not star.allele_calls or len(star.allele_calls) != 2:
                continue
            if star.diplotype == "no translation available":
                continue

            gene = star.gene
            allele1 = star.allele_calls[0].allele
            allele2 = star.allele_calls[1].allele

            annotation = self._annotate_diplotype(gene, allele1, allele2)
            if annotation:
                star.diplotype_annotation = annotation

    def _annotate_diplotype(self, gene: str, allele1: str, allele2: str) -> CpicDiplotypeAnnotation | None:
        # Query diplotype info
        diplotype_info = self._query_diplotype_info(gene, allele1, allele2)

        # Query allele info
        allele_annotations = []
        for allele in [allele1, allele2]:
            allele_info = self._query_allele_info(gene, allele)
            allele_annotations.append(CpicAlleleAnnotation(allele=allele, allele_info=allele_info))

        # Query drug pairs and recommendations
        drugs = self._query_drugs(gene)

        if not diplotype_info and not drugs:
            return None

        return CpicDiplotypeAnnotation(
            gene=gene,
            diplotype=f"{allele1}/{allele2}",
            diplotype_info=diplotype_info,
            alleles=allele_annotations,
            drugs=drugs,
        )

    def _query_diplotype_info(self, gene: str, allele1: str, allele2: str) -> CpicDiplotypeInfo | None:
        cache_key = f"{gene}:{allele1}/{allele2}"
        if cache_key not in self._diplotype_cache:
            params = {"genesymbol": f"eq.{gene}", "diplotype": f"eq.{allele1}/{allele2}"}
            data = self._get("/diplotype", params)
            if not data:
                # Try reversed order
                params["diplotype"] = f"eq.{allele2}/{allele1}"
                data = self._get("/diplotype", params)
            self._diplotype_cache[cache_key] = data or []

        items = self._diplotype_cache[cache_key]
        if not items:
            return None

        item = items[0]
        return CpicDiplotypeInfo(
            gene_symbol=item.get("genesymbol", ""),
            diplotype=item.get("diplotype", ""),
            function1=item.get("ehrfunction1", ""),
            function2=item.get("ehrfunction2", ""),
            activity_value1=str(item.get("activityvalue1", "")),
            activity_value2=str(item.get("activityvalue2", "")),
            total_activity_score=str(item.get("totalactivityscore", "")),
            description=item.get("description", ""),
            gene_result=item.get("generesult", ""),
            ehr_priority=item.get("ehrpriority", ""),
            consultation_text=item.get("consultationtext", ""),
            lookup_key=item.get("lookupkey", {}),
        )

    def _query_allele_info(self, gene: str, allele: str) -> CpicAlleleInfo | None:
        cache_key = f"{gene}:{allele}"
        if cache_key not in self._allele_cache:
            params = {"genesymbol": f"eq.{gene}", "name": f"eq.{allele}"}
            data = self._get("/allele", params)
            self._allele_cache[cache_key] = data or []

        items = self._allele_cache[cache_key]
        if not items:
            return None

        item = items[0]
        return CpicAlleleInfo(
            gene_symbol=item.get("genesymbol", ""),
            name=item.get("name", ""),
            functional_status=item.get("functionalstatus", ""),
            clinical_functional_status=item.get("clinicalfunctionalstatus", ""),
            activity_value=str(item.get("activityvalue", "")),
            strength=item.get("strength", ""),
            findings=item.get("findings", ""),
            frequency=item.get("frequency", {}),
        )

    def _query_drugs(self, gene: str) -> list[CpicDrug]:
        if gene not in self._pair_cache:
            params = {"genesymbol": f"eq.{gene}"}
            data = self._get("/pair", params)
            self._pair_cache[gene] = data or []

        drugs = []
        for pair in self._pair_cache[gene]:
            drug_id = pair.get("drugid") or ""
            guideline_id = pair.get("guidelineid")

            # Resolve drug name from /drug endpoint
            drug_name = self._resolve_drug_name(drug_id)

            # Query recommendations only if both drugid and guidelineid are available
            recs = self._query_recommendations(drug_id, guideline_id) if drug_id and guideline_id else []

            drug = CpicDrug(
                drug_id=str(drug_id),
                drug_name=drug_name,
                gene_symbol=pair.get("genesymbol", ""),
                guideline_id=str(guideline_id or ""),
                cpic_level=pair.get("cpiclevel", ""),
                pgkb_ca_level=pair.get("clinpgxlevel", ""),
                pgx_testing=pair.get("pgxtesting", "") or "",
                used_for_recommendation=pair.get("usedforrecommendation", False),
                recommendations=recs,
            )
            drugs.append(drug)
        return drugs

    def _resolve_drug_name(self, drug_id: str) -> str:
        if not drug_id:
            return ""
        if drug_id not in self._drug_name_cache:
            params = {"drugid": f"eq.{drug_id}"}
            data = self._get("/drug", params)
            if data:
                self._drug_name_cache[drug_id] = data[0].get("name", "")
            else:
                self._drug_name_cache[drug_id] = ""
        return self._drug_name_cache[drug_id]

    def _query_recommendations(self, drug_id: str, guideline_id: str) -> list[CpicDrugRecommendation]:
        cache_key = f"{drug_id}:{guideline_id}"
        if cache_key not in self._recommendation_cache:
            params = {"drugid": f"eq.{drug_id}", "guidelineid": f"eq.{guideline_id}"}
            data = self._get("/recommendation", params)
            self._recommendation_cache[cache_key] = data or []

        recs = []
        for item in self._recommendation_cache[cache_key]:
            rec_drug_id = str(item.get("drugid", ""))
            rec = CpicDrugRecommendation(
                source="CPIC",
                drug_id=rec_drug_id,
                drug_name=self._resolve_drug_name(rec_drug_id),
                guideline_id=str(item.get("guidelineid", "")),
                drug_recommendation=item.get("drugrecommendation", ""),
                classification=item.get("classification", ""),
                implications=item.get("implications", {}),
                phenotypes=item.get("phenotypes", {}),
                population=item.get("population", ""),
                comments=item.get("comments", ""),
            )
            recs.append(rec)
        return recs

    def _get(self, endpoint: str, params: dict[str, str] | None = None) -> list[dict[str, Any]]:
        url = f"{self.base_url}{endpoint}"
        try:
            resp = self.session.get(url, params=params, timeout=30)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            logger.warning("CPIC API request failed: %s %s -> %s", endpoint, params, e)
            return []
