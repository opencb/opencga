"""Smoke tests for pharmacogenomics tool."""

import pytest

from pharmacogenomics import __version__


def test_version():
    assert __version__ == "0.1.0"


def test_cli_help():
    from pharmacogenomics.cli import parse_args

    with pytest.raises(SystemExit) as exc_info:
        parse_args(["--help"])
    assert exc_info.value.code == 0


def test_cli_version():
    from pharmacogenomics.cli import parse_args

    with pytest.raises(SystemExit) as exc_info:
        parse_args(["--version"])
    assert exc_info.value.code == 0


def test_cli_openarray_args():
    from pharmacogenomics.cli import parse_args

    args = parse_args(["openarray", "--snv-file", "snv.txt", "--translation-file", "trans.txt", "--annotate"])
    assert args.command == "openarray"
    assert args.snv_file == "snv.txt"
    assert args.translation_file == "trans.txt"
    assert args.annotate is True
    assert args.cnv_file is None
    assert args.rename_file is None


def test_cli_ngs_args():
    from pharmacogenomics.cli import parse_args

    args = parse_args(["ngs", "--vcf-file", "sample.vcf", "--pharmcat", "/opt/pharmcat.jar"])
    assert args.command == "ngs"
    assert args.vcf_file == "sample.vcf"
    assert args.pharmcat == "/opt/pharmcat.jar"
    assert args.annotate is False


def test_cli_ngs_docker_default():
    from pharmacogenomics.cli import parse_args

    args = parse_args(["ngs", "--vcf-file", "sample.vcf"])
    assert args.pharmcat is None  # will use Docker


def test_config_defaults():
    from pharmacogenomics.config import ToolConfig

    config = ToolConfig()
    assert config.cpic_base_url == "https://api.cpicpgx.org/v1"
    assert config.pharmcat_docker_image == "pgkb/pharmcat"
    assert config.pharmcat_timeout == 600


def test_models_to_dict():
    from pharmacogenomics.core.models import AlleleCall, AlleleTyperResult, StarAlleleResult

    result = AlleleTyperResult(
        sample_id="SAMPLE1",
        source="openarray",
        allele_typer_results=[
            StarAlleleResult(
                gene="CYP2D6",
                diplotype="*1/*4",
                allele_calls=[AlleleCall(allele="*1"), AlleleCall(allele="*4")],
            )
        ],
    )
    d = result.to_dict()
    assert d["sampleId"] == "SAMPLE1"
    assert d["source"] == "openarray"
    assert len(d["alleleTyperResults"]) == 1
    assert d["alleleTyperResults"][0]["gene"] == "CYP2D6"
    assert d["alleleTyperResults"][0]["diplotype"] == "*1/*4"


def test_allele_ordering():
    from pharmacogenomics.core.allele_typer import _compare_star_alleles

    assert _compare_star_alleles("*5", "*22") < 0
    assert _compare_star_alleles("*22", "*5") > 0
    assert _compare_star_alleles("*1", "*1") == 0
    # Ref before Alt
    assert _compare_star_alleles("rs2108622 wt (C)", "rs2108622 Alt (T)") < 0
    assert _compare_star_alleles("Ref", "Alt") < 0
