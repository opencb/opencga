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
class CpicSequenceLocation:
    name: str = ""
    dbsnp_id: str = ""
    chromosome: str = ""
    position: int = 0
    gene_symbol: str = ""


@dataclass
class CpicAlleleLocationValue:
    name: str = ""
    value: str = ""
    sequence_location: CpicSequenceLocation | None = None


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
    location: list[CpicAlleleLocationValue] = field(default_factory=list)


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
class CustomDiplotypeAnnotation:
    gene: str = ""
    allele1: str = ""
    allele2: str = ""
    function: str = ""
    description: str = ""
    type: str = ""


@dataclass
class CpicDiplotypeAnnotation:
    gene: str = ""
    diplotype: str = ""
    diplotype_info: CpicDiplotypeInfo | None = None
    alleles: list[CpicAlleleAnnotation] = field(default_factory=list)
    drugs: list[CpicDrug] = field(default_factory=list)
    custom_annotation: CustomDiplotypeAnnotation | None = None


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


@dataclass
class SummaryDrugRecommendation:
    drug_name: str = ""
    recommendation: str = ""
    classification: str = ""
    implications: str = ""
    population: str = ""


@dataclass
class ActionableResult:
    gene: str = ""
    diplotype: str = ""
    renamed_diplotype: str | None = None
    phenotype: str = ""
    activity_score: str = ""
    cpic_level: str = ""
    pgkb_level: str = ""
    drugs: list[SummaryDrugRecommendation] = field(default_factory=list)


@dataclass
class NormalResult:
    gene: str = ""
    diplotype: str = ""
    phenotype: str = ""


@dataclass
class PharmacogenomicsSummary:
    sample_id: str = ""
    source: str = ""
    date: str = ""
    total_genes_analyzed: int = 0
    total_genes_with_results: int = 0
    total_actionable_genes: int = 0
    total_drugs_affected: int = 0
    actionable_results: list[ActionableResult] = field(default_factory=list)
    informative_results: list[ActionableResult] = field(default_factory=list)
    normal_results: list[NormalResult] = field(default_factory=list)
    no_translation_genes: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return _to_dict(self)


# Actionable CPIC levels (A, B) and PharmGKB levels (1A, 1B, 2A, 2B)
ACTIONABLE_CPIC_LEVELS = {"A", "B"}
ACTIONABLE_PGKB_LEVELS = {"1A", "1B", "2A", "2B"}


def build_summary(result: AlleleTyperResult) -> PharmacogenomicsSummary:
    """Build a PharmacogenomicsSummary from an AlleleTyperResult."""
    from datetime import date

    summary = PharmacogenomicsSummary(
        sample_id=result.sample_id,
        source=result.source,
        date=date.today().isoformat(),
    )

    seen_genes: set[str] = set()
    genes_with_results: set[str] = set()
    actionable_drug_names: set[str] = set()

    for star in result.allele_typer_results:
        gene = star.gene

        # Track unique genes
        if gene not in seen_genes:
            seen_genes.add(gene)
            summary.total_genes_analyzed += 1

        # No translation or undetermined
        if star.diplotype in ("no translation available", "No ha sido posible obtener un genotipo"):
            if gene not in summary.no_translation_genes:
                summary.no_translation_genes.append(gene)
            continue

        genes_with_results.add(gene)

        annotation = star.diplotype_annotation
        if not annotation:
            # No annotation — add as normal result
            phenotype = ""
            summary.normal_results.append(NormalResult(gene=gene, diplotype=star.diplotype, phenotype=phenotype))
            continue

        # Extract phenotype and activity score from diplotype info
        phenotype = ""
        activity_score = ""
        if annotation.diplotype_info:
            phenotype = annotation.diplotype_info.gene_result or ""
            activity_score = annotation.diplotype_info.total_activity_score or ""

        # Classify drugs by CPIC level
        actionable_drugs: list[SummaryDrugRecommendation] = []
        informative_drugs: list[SummaryDrugRecommendation] = []
        best_cpic_level = ""
        best_pgkb_level = ""

        for drug in annotation.drugs:
            cpic_level = drug.cpic_level or ""
            pgkb_level = drug.pgkb_ca_level or ""

            # Track best level for this gene
            if cpic_level in ACTIONABLE_CPIC_LEVELS and (not best_cpic_level or cpic_level < best_cpic_level):
                best_cpic_level = cpic_level
            if pgkb_level in ACTIONABLE_PGKB_LEVELS and (not best_pgkb_level or pgkb_level < best_pgkb_level):
                best_pgkb_level = pgkb_level

            for rec in drug.recommendations:
                summary_drug = SummaryDrugRecommendation(
                    drug_name=drug.drug_name,
                    recommendation=rec.drug_recommendation,
                    classification=rec.classification,
                    implications=_first_value(rec.implications),
                    population=rec.population,
                )
                if cpic_level in ACTIONABLE_CPIC_LEVELS or pgkb_level in ACTIONABLE_PGKB_LEVELS:
                    actionable_drugs.append(summary_drug)
                else:
                    informative_drugs.append(summary_drug)

        is_normal = phenotype.lower() in ("normal metabolizer", "extensive metabolizer", "normal function", "")

        if actionable_drugs and not is_normal:
            # Only count drugs as affected when the phenotype is non-normal
            for d in actionable_drugs:
                actionable_drug_names.add(d.drug_name)
            summary.actionable_results.append(ActionableResult(
                gene=gene, diplotype=star.diplotype, renamed_diplotype=star.renamed_diplotype,
                phenotype=phenotype, activity_score=activity_score,
                cpic_level=best_cpic_level, pgkb_level=best_pgkb_level, drugs=actionable_drugs,
            ))
        elif informative_drugs or (actionable_drugs and is_normal):
            all_drugs = actionable_drugs + informative_drugs
            summary.informative_results.append(ActionableResult(
                gene=gene, diplotype=star.diplotype, renamed_diplotype=star.renamed_diplotype,
                phenotype=phenotype, activity_score=activity_score,
                cpic_level=best_cpic_level, pgkb_level=best_pgkb_level, drugs=all_drugs,
            ))
        else:
            summary.normal_results.append(NormalResult(gene=gene, diplotype=star.diplotype, phenotype=phenotype))

    summary.total_genes_with_results = len(genes_with_results)
    summary.total_actionable_genes = len(summary.actionable_results)
    summary.total_drugs_affected = len(actionable_drug_names)

    return summary


def _first_value(d: dict[str, str]) -> str:
    """Get first value from a dict, or empty string."""
    if d:
        return next(iter(d.values()), "")
    return ""


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
