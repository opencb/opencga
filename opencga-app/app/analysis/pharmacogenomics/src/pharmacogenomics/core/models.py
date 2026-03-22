"""Data models for pharmacogenomics analysis results.

Port of the Java AlleleTyperResult model with extensions for NGS/PharmCAT support.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class AssayDefinition:
    id: str = ""
    allele: str = ""


@dataclass
class TranslationInfo:
    gene: str = ""
    assays: list[AssayDefinition] = field(default_factory=list)


@dataclass
class Genotype:
    variant: str = ""
    genotype: str = ""


@dataclass
class CpicDrugRecommendation:
    source: str = ""
    drug_id: str = ""
    drug_name: str = ""
    guideline_id: str = ""
    drug_recommendation: str = ""
    classification: str = ""
    implications: dict[str, str] = field(default_factory=dict)
    phenotypes: dict[str, str] = field(default_factory=dict)
    population: str = ""
    dosing_information: bool = False
    alternate_drug_available: bool = False
    comments: str = ""


@dataclass
class CpicDrug:
    drug_id: str = ""
    drug_name: str = ""
    gene_symbol: str = ""
    guideline_id: str = ""
    cpic_level: str = ""
    pgkb_ca_level: str = ""
    pgx_testing: str = ""
    used_for_recommendation: bool = False
    recommendations: list[CpicDrugRecommendation] = field(default_factory=list)


@dataclass
class CpicAlleleInfo:
    gene_symbol: str = ""
    name: str = ""
    functional_status: str = ""
    clinical_functional_status: str = ""
    activity_value: str = ""
    strength: str = ""
    findings: str = ""
    frequency: dict[str, float] = field(default_factory=dict)


@dataclass
class CpicAlleleAnnotation:
    allele: str = ""
    allele_info: CpicAlleleInfo | None = None


@dataclass
class CpicDiplotypeInfo:
    gene_symbol: str = ""
    diplotype: str = ""
    function1: str = ""
    function2: str = ""
    activity_value1: str = ""
    activity_value2: str = ""
    total_activity_score: str = ""
    description: str = ""
    gene_result: str = ""
    ehr_priority: str = ""
    consultation_text: str = ""
    diplotype_key: dict[str, Any] = field(default_factory=dict)
    lookup_key: dict[str, str] = field(default_factory=dict)


@dataclass
class CpicDiplotypeAnnotation:
    gene: str = ""
    diplotype: str = ""
    diplotype_info: CpicDiplotypeInfo | None = None
    alleles: list[CpicAlleleAnnotation] = field(default_factory=list)
    drugs: list[CpicDrug] = field(default_factory=list)


@dataclass
class StarAlleleAnnotation:
    source: str = ""
    version: str = ""
    date: str = ""
    drugs: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class AlleleCall:
    allele: str = ""
    renamed_allele: str | None = None
    annotation: StarAlleleAnnotation | None = None


@dataclass
class StarAlleleResult:
    gene: str = ""
    diplotype: str = ""
    renamed_diplotype: str | None = None
    allele_calls: list[AlleleCall] = field(default_factory=list)
    variants: list[str] = field(default_factory=list)
    diplotype_annotation: CpicDiplotypeAnnotation | None = None


@dataclass
class AlleleTyperResult:
    sample_id: str = ""
    source: str = ""  # "openarray" or "ngs"
    allele_typer_results: list[StarAlleleResult] = field(default_factory=list)
    genotypes: list[Genotype] = field(default_factory=list)
    translation: list[TranslationInfo] = field(default_factory=list)
    additional_information: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict:
        """Convert to dict for JSON serialization."""
        return _to_dict(self)


def _to_dict(obj) -> Any:
    """Recursively convert dataclass instances to dicts."""
    if isinstance(obj, list):
        return [_to_dict(item) for item in obj]
    if isinstance(obj, dict):
        return {k: _to_dict(v) for k, v in obj.items()}
    if hasattr(obj, "__dataclass_fields__"):
        result = {}
        for f_name in obj.__dataclass_fields__:
            value = getattr(obj, f_name)
            if value is not None:
                converted = _to_dict(value)
                # Convert snake_case to camelCase for JSON compatibility
                camel = _to_camel(f_name)
                result[camel] = converted
        return result
    return obj


def _to_camel(name: str) -> str:
    """Convert snake_case to camelCase."""
    parts = name.split("_")
    return parts[0] + "".join(p.capitalize() for p in parts[1:])
