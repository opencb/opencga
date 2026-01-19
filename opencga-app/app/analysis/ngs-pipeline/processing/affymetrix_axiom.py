import os
from pathlib import Path
from typing import Set

from .base_processor import BaseProcessor


class AffymetrixAxiom(BaseProcessor):

    def __init__(self, pipeline: dict, output: Path, logger=None):
        """
        Initialize QualityControl with config and output parameters.

        Parameters
        ----------
        pipeline : dict
            Configuration dictionary
        output : dict
            Output dictionary
        """
        super().__init__(output, logger)
        self.pipeline = pipeline

    """
    This is all based https://documents.thermofisher.com/TFS-Assets/LSG/manuals/MAN0018363-AxiomDataAnalysis-UG-RUO.pdf
    """

    """
    A single alignment step.
    Implement `run` with concrete checks. `execute` wraps `run` adding logging
    and common error handling.
    """
    # @override
    def execute(self) -> list[str]:
        ## Prepare input parameters
        data_dir_path = Path(self.pipeline.get("input", {}).get("dataDir", ""))
        index_dir = self.pipeline.get("input", {}).get("indexDir", "")

        ## 1. Create cel_list.txt file input.samples.files
        cel_list_path = self.create_cel_list_file()

        ## 2. Execute apt-geno-qc-axiom, example:
        cmd = (["apt-geno-qc-axiom"]
               + ["--out-dir", str(self.output)]
               + ["--out-file", str(self.output / "apt-geno-qc-axiom.txt")]
               + ["--analysis-files-path", index_dir]
               + ["--arg-file", index_dir + "/Axiom_KU8.r2.apt-geno-qc.AxiomQC1.xml"]
               + ["--cel-files", str(self.output / cel_list_path.name)]
               + ["--log-file", str(self.output / "apt-geno-qc-axiom.log")])
        self.run_command(cmd)

        ## 2. Remove samples that did not pass QC from cel_files.txt
        # We need to parse this file 'apt-geno-qc-axiom.txt' and remove samples with a DQC value (column 18) less than the default DQC threshold of 0.82.
        apt_geno_qc_axiom_path = self.output / "apt-geno-qc-axiom.txt"
        cel_files_passed_qc: list[str] = []
        with apt_geno_qc_axiom_path.open("r", encoding="utf-8") as fh:
            file_name_index = 0
            dqc_index = 17
            for line in fh:
                ## We ignore the lines starting with #
                if line.strip().startswith("#"):
                    continue
                if line.strip().startswith("cel_files"):
                    header = line.strip().split("\t")
                    file_name_index = header.index("cel_files")
                    dqc_index = header.index("axiom_dishqc_DQC")
                    self.logger.debug(f"Header found. file_name_index: {file_name_index}, dqc_index: {dqc_index}")
                else:
                    fields = line.strip().split("\t")
                    file_id = fields[file_name_index]
                    dqc_value = float(fields[dqc_index])
                    if dqc_value >= 0.82:
                        cel_files_passed_qc.append(str (data_dir_path / file_id))
                    else:
                        self.logger.info(f"Sample {file_id} did not pass QC with DQC value {dqc_value}")

        # Create cel_list2.txt file
        cel_list2_path = self.output / "cel_files2.txt"
        with cel_list2_path.open("w", encoding="utf-8") as fh:
            fh.write("cel_files\n")
            for f in cel_files_passed_qc:
                fh.write(f"{f}\n")


        ## 3. Execute apt-genotype-axiom, example:
        cmd = (["apt-genotype-axiom"]
               + ["--arg-file", index_dir + "/Axiom_KU8_96orMore_Step1.r2.apt-genotype-axiom.AxiomGT1.apt2.xml"]
               + ["--analysis-files-path", index_dir]
               + ["--out-dir", str(self.output / "step1")]
               + ["--dual-channel-normalization", "true"]
               + ["--table-output", "false"]
               + ["--cel-files", str(self.output / cel_list2_path.name)]
               + ["--artifact-reduction-output-trustcheck", "true"]
               ## Additional parameters:
               + ["--genotyping-node:snp-priors-input-file", index_dir + "/Axiom_KU8.r2.AxiomGT1.models"]
               + ["--probeset-ids", index_dir + "/Axiom_KU8.r2.step1.ps"]
               + ["--chip-type", "Axiom_KU8"]
               + ["--artifact-reduction-clip", "0.4"]
               + ["--artifact-reduction-open", "2"]
               + ["--artifact-reduction-close", "2"]
               + ["--artifact-reduction-fringe", "4"]
               + ["--artifact-reduction-cc", "2"]
               + ["--sketch-target-scale-value", "1000"]
               + ["--sketch-size", "50000"]
               + ["--sketch-target-input-file", index_dir + "/Axiom_KU8.r2.AxiomGT1.sketch"]
               + ["--genotyping-node:brlmmp-CM", "1"]
               + ["--genotyping-node:brlmmp-bins", "100"]
               + ["--genotyping-node:brlmmp-mix", "1"]
               + ["--genotyping-node:brlmmp-bic", "2"]
               + ["--genotyping-node:brlmmp-lambda", "1.0"]
               + ["--genotyping-node:brlmmp-HARD", "3"]
               + ["--genotyping-node:brlmmp-SB", "0.75"]
               + ["--genotyping-node:brlmmp-transform", "MVA"]
               + ["--genotyping-node:brlmmp-copyqc", "0.00000"]
               + ["--genotyping-node:brlmmp-wobble", "0.05"]
               + ["--genotyping-node:brlmmp-MS", "0.15"]
               + ["--genotyping-node:brlmmp-copytype", "-1"]
               + ["--genotyping-node:brlmmp-clustertype", "2"]
               + ["--genotyping-node:brlmmp-ocean", "0.00001"]
               + ["--genotyping-node:brlmmp-CSepPen", "0.1"]
               + ["--genotyping-node:brlmmp-CSepThr", "4"]
               + ["--cdf-file", "Axiom_KU8.r2.cdf"]
               + ["--special-snps", index_dir + "/Axiom_KU8.r2.specialSNPs"]
               + ["--x-probes-file", index_dir + "/Axiom_KU8.r2.chrXprobes"]
               + ["--y-probes-file", index_dir + "/Axiom_KU8.r2.chrYprobes"]
               + ["--igender-female-threshold", "0.65"]
               + ["--igender-male-threshold", "0.95"]
               )
        self.run_command(cmd)

        ## 3.1. Execute apt-genotype-axiom, example:
        cmd = (["apt-genotype-axiom"]
               + ["--arg-file", index_dir + "/Axiom_KU8_96orMore_Step1.r2.apt-genotype-axiom.AxiomGT1.apt2.xml"]
               + ["--analysis-files-path", index_dir]
               + ["--out-dir", str(self.output / "step1" / "SMN")]
               + ["--dual-channel-normalization", "true"]
               # + ["--table-output", "false"]
               + ["--cel-files", str(self.output / cel_list2_path.name)]
               # + ["--artifact-reduction-output-trustcheck", "true"]
               ## Additional parameters:
               + ["--genotyping-node:snp-priors-input-file", index_dir + "/Axiom_KU8.r2.AxiomGT1.models"]
               + ["--probeset-ids", index_dir + "/Axiom_KU8.r2.step1.ps"]
               + ["--chip-type", "Axiom_KU8"]
               + ["--artifact-reduction-clip", "0.4"]
               + ["--artifact-reduction-open", "2"]
               + ["--artifact-reduction-close", "2"]
               + ["--artifact-reduction-fringe", "4"]
               + ["--artifact-reduction-cc", "2"]
               + ["--sketch-target-scale-value", "1000"]
               + ["--sketch-size", "50000"]
               + ["--sketch-target-input-file", index_dir + "/Axiom_KU8.r2.AxiomGT1.sketch"]
               + ["--genotyping-node:brlmmp-CM", "1"]
               + ["--genotyping-node:brlmmp-bins", "100"]
               + ["--genotyping-node:brlmmp-mix", "1"]
               + ["--genotyping-node:brlmmp-bic", "2"]
               + ["--genotyping-node:brlmmp-lambda", "1.0"]
               + ["--genotyping-node:brlmmp-HARD", "3"]
               + ["--genotyping-node:brlmmp-SB", "0.75"]
               + ["--genotyping-node:brlmmp-transform", "MVA"]
               + ["--genotyping-node:brlmmp-copyqc", "0.00000"]
               + ["--genotyping-node:brlmmp-wobble", "0.05"]
               + ["--genotyping-node:brlmmp-MS", "0.15"]
               + ["--genotyping-node:brlmmp-copytype", "-1"]
               + ["--genotyping-node:brlmmp-clustertype", "2"]
               + ["--genotyping-node:brlmmp-ocean", "0.00001"]
               + ["--genotyping-node:brlmmp-CSepPen", "0.1"]
               + ["--genotyping-node:brlmmp-CSepThr", "4"]
               + ["--cdf-file", "Axiom_KU8.r2.cdf"]
               + ["--special-snps", index_dir + "/Axiom_KU8.r2.specialSNPs"]
               + ["--x-probes-file", index_dir + "/Axiom_KU8.r2.chrXprobes"]
               + ["--y-probes-file", index_dir + "/Axiom_KU8.r2.chrYprobes"]
               + ["--igender-female-threshold", "0.65"]
               + ["--igender-male-threshold", "0.95"]
               + ["--summaries-only", "false"]
               + ["--summaries", "true"]
               + ["--summary-a5-output", "true"]
               + ["--report", "true"]
               )
        self.run_command(cmd)

        ## 4. Execute apt-genotype-axiom, example:
        cmd = (["apt-genotype-axiom"]
               + ["--arg-file", index_dir + "/Axiom_KU8_96orMore_Step2.r2.apt-genotype-axiom.mm.SnpSpecificPriors.AxiomGT1.apt2.xml"]
               + ["--analysis-files-path", index_dir]
               + ["--out-dir", str(self.output / "step2")]
               + ["--dual-channel-normalization", "true"]
               + ["--table-output", "true"]
               + ["--allele-summaries", "true"]
               + ["--multi-genotyping-node:multi-posteriors-output", "true"]
               + ["--genotyping-node:snp-posteriors-output", "true"]
               + ["--batch-folder", str(self.output / "step2")]
               + ["--cel-files", str(self.output / cel_list2_path.name)]
               + ["--artifact-reduction-output-trustcheck", "true"]
               ## Additional parameters:
               + ["--snp-priors-input-file", index_dir + "/Axiom_KU8.r2.AxiomGT1.models"]
               + ["--genotyping-node:snp-priors-input-file", index_dir + "/Axiom_KU8.r2.AxiomGT1.models"]
               + ["--multi-allele-background-node:snp-priors-input-file", index_dir + "/Axiom_KU8.r2.AxiomGT1.mmb.multimodels_background"]
               + ["--multi-allele-pairwise-node:snp-priors-input-file", index_dir + "/Axiom_KU8.r2.AxiomGT1.mmp.multimodels_pairwise"]
               + ["--multi-priors-input-file", index_dir + "/Axiom_KU8.r2.AxiomGT1.mm.multimodels"]
               + ["--probeset-ids", index_dir + "/Axiom_KU8.r2.step2.ps"]
               + ["--chip-type", "Axiom_KU8"]
               + ["--artifact-reduction-clip", "0.4"]
               + ["--artifact-reduction-close", "2"]
               + ["--artifact-reduction-open", "2"]
               + ["--artifact-reduction-fringe", "4"]
               + ["--artifact-reduction-cc", "2"]
               + ["--sketch-target-scale-value", "1000"]
               + ["--sketch-size", "50000"]
               + ["--sketch-target-input-file", index_dir + "/Axiom_KU8.r2.AxiomGT1.sketch"]

               + ["--genotyping-node:brlmmp-CM", "1"]
               + ["--genotyping-node:brlmmp-bins", "100"]
               + ["--genotyping-node:brlmmp-mix", "1"]
               + ["--genotyping-node:brlmmp-bic", "2"]
               + ["--genotyping-node:brlmmp-lambda", "1.0"]
               + ["--genotyping-node:brlmmp-HARD", "3"]
               + ["--genotyping-node:brlmmp-SB", "0.75"]
               + ["--genotyping-node:brlmmp-transform", "MVA"]
               + ["--genotyping-node:brlmmp-copyqc", "0.00000"]
               + ["--genotyping-node:brlmmp-wobble", "0.05"]
               + ["--genotyping-node:brlmmp-MS", "0.15"]
               + ["--genotyping-node:brlmmp-copytype", "-1"]
               + ["--genotyping-node:brlmmp-clustertype", "2"]
               + ["--genotyping-node:brlmmp-ocean", "0.00001"]
               + ["--genotyping-node:brlmmp-CSepPen", "0.1"]
               + ["--genotyping-node:brlmmp-CSepThr", "4"]

               + ["--multi-allele-background-node:brlmmp-CM", "1"]
               + ["--multi-allele-background-node:brlmmp-bins", "100"]
               + ["--multi-allele-background-node:brlmmp-mix", "1"]
               + ["--multi-allele-background-node:brlmmp-bic", "2"]
               + ["--multi-allele-background-node:brlmmp-lambda", "1.0"]
               + ["--multi-allele-background-node:brlmmp-HARD", "3"]
               + ["--multi-allele-background-node:brlmmp-SB", "0.75"]
               + ["--multi-allele-background-node:brlmmp-transform", "MVA"]
               + ["--multi-allele-background-node:brlmmp-copyqc", "0.00000"]
               + ["--multi-allele-background-node:brlmmp-wobble", "0.05"]
               + ["--multi-allele-background-node:brlmmp-MS", "0.15"]
               + ["--multi-allele-background-node:brlmmp-copytype", "-1"]
               + ["--multi-allele-background-node:brlmmp-clustertype", "2"]
               + ["--multi-allele-background-node:brlmmp-ocean", "0.00001"]
               + ["--multi-allele-background-node:brlmmp-CSepPen", "0.1"]
               + ["--multi-allele-background-node:brlmmp-CSepThr", "4"]

               + ["--multi-allele-pairwise-node:brlmmp-CM", "1"]
               + ["--multi-allele-pairwise-node:brlmmp-bins", "100"]
               + ["--multi-allele-pairwise-node:brlmmp-mix", "1"]
               + ["--multi-allele-pairwise-node:brlmmp-bic", "2"]
               + ["--multi-allele-pairwise-node:brlmmp-lambda", "1.0"]
               + ["--multi-allele-pairwise-node:brlmmp-HARD", "3"]
               + ["--multi-allele-pairwise-node:brlmmp-SB", "0.75"]
               + ["--multi-allele-pairwise-node:brlmmp-transform", "MVA"]
               + ["--multi-allele-pairwise-node:brlmmp-copyqc", "0.00000"]
               + ["--multi-allele-pairwise-node:brlmmp-wobble", "0.05"]
               + ["--multi-allele-pairwise-node:brlmmp-MS", "0.15"]
               + ["--multi-allele-pairwise-node:brlmmp-copytype", "-1"]
               + ["--multi-allele-pairwise-node:brlmmp-clustertype", "2"]
               + ["--multi-allele-pairwise-node:brlmmp-ocean", "0.00001"]
               + ["--multi-allele-pairwise-node:brlmmp-CSepPen", "0.1"]
               + ["--multi-allele-pairwise-node:brlmmp-CSepThr", "4"]

               + ["--summaries", "true"]
               + ["--blob-format-version", "2.0"]
               + ["--process-multi-alleles", "true"]
               + ["--use-copynumber-call-codes", "false"]
               + ["--multi-shell-barrier-0to1", "0.05"]
               + ["--multi-shell-barrier-1to2", "0.05"]
               + ["--multi-copy-distance-0to1", "1.5"]
               + ["--multi-copy-distance-1to2", "0.2"]
               + ["--multi-freq-flag", "false"]
               + ["--multi-inflate-PRA", "0.0"]
               + ["--multi-lambda-P", "0.0"]
               + ["--multi-wobble", "0.05"]
               + ["--multi-ocean", "0.00001"]
               + ["--cdf-file", index_dir + "/Axiom_KU8.r2.cdf"]
               + ["--special-snps", index_dir + "/Axiom_KU8.r2.specialSNPs"]
               + ["--x-probes-file", index_dir + "/Axiom_KU8.r2.chrXprobes"]
               + ["--y-probes-file", index_dir + "/Axiom_KU8.r2.chrYprobes"]
               + ["--snp-specific-param-file", index_dir + "/Axiom_KU8.r2.probeset_genotyping_parameters.txt"]
               + ["--igender-female-threshold", "0.65"]
               + ["--igender-male-threshold", "0.95"]
               + ["--do-rare-het-adjustment", "true"]
               + ["--max-rare-het-count", "3"]
               + ["--@low-sdDist-minor-cutoff-4", "-10"]
               + ["--@high-sdDist-minor-cutoff-4", "5000"]
               + ["--@low-sdDist-major-cutoff-4", "-100"]
               + ["--@high-sdDist-major-cutoff-4", "5000"])
        self.run_command(cmd)

        ## 5. Call to CNV calling, example:
        cmd = (["apt-genotype-axiom"]
               + ["--analysis-files-path", index_dir]
               + ["--arg-file", index_dir + "/Axiom_KU8.r2.apt-genotype-axiom.AxiomCN_PS1.apt2.xml"]
               + ["--cel-files", str(self.output / cel_list2_path.name)]
               + ["--out-dir", str(self.output / "step2" / "summary")]
               + ["--log-file", str(self.output / "step2" / "summary" / "apt2-axiom.log")])
        self.run_command(cmd)

        ## 6. Call CNV, example:
        cmd = (["apt-copynumber-axiom-cnvmix"]
               + ["--analysis-files-path", index_dir]
               + ["--arg-file", index_dir + "/Axiom_KU8.r2.apt-copynumber-axiom-cnvmix.AxiomCNVmix.apt2.xml"]
               + ["--reference-file", index_dir + "/Axiom_KU8.r2.cn_models"]
               + ["--mapd-max", "0.35"]
               + ["--waviness-sd-max", "0.1"]
               + ["--summary-file", str(self.output / "step2" / "summary" / "AxiomGT1.summary.a5")]
               + ["--report-file", str(self.output / "step2" / "summary" / "AxiomGT1.report.txt")]
               + ["--out-dir", str(self.output / "step2" / "cn")]
               + ["--log-file", str(self.output / "step2" / "cn" / "apt-copynumber-axiom.log")]
               ## Additional parameters:
               + ["--cn-priors-file", index_dir + "/Axiom_KU8.r2.cn_priors"]
               + ["--plate-effect-lower-limit", "-0.3"]
               + ["--plate-effect-upper-limit", "0.3"]
               + ["--compute-plate-corrected-CN-QC", "false"]
               + ["--chip-type", "Axiom_KU8"]
               + ["--loh-error-rate", "0.05"]
               + ["--loh-beta", "0.001"]
               + ["--loh-alpha", "0.01"]
               + ["--loh-separation", "1000000"]
               + ["--loh-min-marker-count", "10"]
               + ["--loh-no-call-threshold", "0.05"]
               + ["--loh-min-genomic-span", "1000000"]
               + ["--use-wave-correction", "true"]
               + ["--wave-count", "-1"]
               + ["--wave-bandwidth", "101"]
               + ["--wave-bin-count", "25"]
               + ["--wave-smooth", "false"]
               + ["--ap-baf-step", "25"]
               + ["--ap-baf-window", "150"]
               + ["--ap-baf-point-count", "129"]
               + ["--ap-baf-bandwidth", "0.3"]
               + ["--ap-baf-cutoff", "0"]
               + ["--ap-baf-threshold", "0.3"]
               + ["--ap-baf-height-threshold", "3"]
               + ["--ap-baf-height-threshold-bound", "1"]
               + ["--ap-baf-symmetry", "true"]
               + ["--shrink-factor-3", "0.5"]
               + ["--shrink-factor-4", "0.6"]
               + ["--fld-weights-value", "100"]
               + ["--transform-data", "true"]
               + ["--cnvmix-density-point-count", "512"]
               + ["--model-prob-min-thr-candidate", "0.000100"]
               + ["--model-prob-med-thr-candidate", "0.000100"]
               + ["--plate-effect-lower-limit", "-0.300000"]
               + ["--plate-effect-upper-limit", "0.300000"]
               + ["--use-text-format-summary-file", "false"])
        self.run_command(cmd)

        ## 6.1 Filter CNV region calls to remove invalid regions (bug workaround).
        # First we need to rename the file with .bkp extension to avoid overwriting during filtering.
        os.rename(str(self.output / "step2" / "cn" / "AxiomCNVMix.cnregioncalls.txt"), str(self.output / "step2" / "cn"/ "AxiomCNVMix.cnregioncalls.txt.bkp"))
        self.filter_cnregion_calls(str(self.output / "step2" / "cn" / "AxiomCNVMix.cnregioncalls.txt.bkp"), str(self.output / "step2" / "cn"/ "AxiomCNVMix.cnregioncalls.txt"))

        ## 7. Execute ps-metrics, example:
        cmd = (["ps-metrics"]
               + ["--posterior-file", str(self.output / "step2" / "AxiomGT1.snp-posteriors.txt")]
               + ["--call-file", str(self.output / "step2" / "AxiomGT1.calls.txt")]
               + ["--report-file", str(self.output / "step2" / "AxiomGT1.report.txt")]
               + ["--summary-file", str(self.output / "step2" / "AxiomGT1.summary.txt")]
               + ["--special-snps", index_dir + "/Axiom_KU8.r2.specialSNPs"]
               + ["--metrics-file", str(self.output / "step2" / "SNPolisher" / "metrics.txt")])
        self.run_command(cmd)

        ## 8. Execute ps-classification, example:
        cmd = (["ps-classification"]
               + ["--species-type", "Diploid"]
               + ["--metrics-file", str(self.output / "step2" / "SNPolisher" / "metrics.txt")]
               + ["--ps2snp-file", index_dir + "/Axiom_KU8.r2.ps2snp_map.ps"]
               + ["--output-dir", str(self.output / "step2" / "SNPolisher")])
        self.run_command(cmd)

        ## 9. Execute apt-format-result to generate VCF, example:
        cmd = (["apt-format-result"]
               + ["--calls-file", str(self.output / "step2" / "AxiomGT1.calls.txt")]
               + ["--export-confidence", "true"]
               + ["--export-log-ratio", "true"]
               + ["--export-allele-signals", "true"]
               + ["--annotation-file", index_dir + "/Axiom_KU8.na36.r1.a2.annot.db"]
               + ["--snp-list-file", str(self.output / "step2" / "SNPolisher" / "Recommended.ps")]
               + ["--export-chr-shortname", "true"]
               + ["--cn-region-calls-file", str(self.output / "step2" / "cn" / "AxiomCNVMix.cnregioncalls.txt")]
               + ["--cn-region-calls-use-cn-raw", "true"]
               + ["--cnv-cndata-file", str(self.output / "step2" / "cn" / "AxiomCNVMix.cnv.a5")]
               + ["--exclude-unknown-chr", "true"]
               + ["--export-vcf-file", str(self.output / "Axiom_KU8.vcf")]
               + ["--export-chr-shortname", "true"])
        self.run_command(cmd)

        """
            apt2-smn-ab --smn-ab-probesets /home/imedina/projects/thermofisher/pre-marital/axiom-ku8/r2/Axiom_KU8.r2.SMN.AB_probesets.txt --summary-a5-file step1/SMN/AxiomGT1.summary.a5  --out-dir /tmp/comeon
        """
        ## Implement this command
        cmd = (["apt2-smn-ab"]
               + ["--smn-ab-probesets", index_dir + "/Axiom_KU8.r2.SMN.AB_probesets.txt"]
               + ["--summary-a5-file", str(self.output / "step2" / "summary" / "AxiomGT1.summary.a5")]
               + ["--out-dir", str(self.output / "step2" / "SMN")])
        self.run_command(cmd)

        """
            apt2-smn-status --qc-gt-report step1/SMN/AxiomGT1.report.txt --qc-cn-report step2/cn/AxiomCNVMix.report.txt --smn-cn-regions step2/cn/AxiomCNVMix.cnregioncalls.txt --smn-ab-report /tmp/comeon/SMN_ABreport.txt --recommended-ps step2/SNPolisher/Recommended.ps --blob-folder step2/AxiomAnalysisSuiteData/ --carrier-thresholds /home/imedina/projects/thermofisher/pre-marital/axiom-ku8/r2/Axiom_KU8.r2.SMN.carrier_thresholds.txt --silent-marker-file /home/imedina/projects/thermofisher/pre-marital/axiom-ku8/r2/Axiom_KU8.r2.SMN.snplist.txt --output-dir /tmp/comeon/ --qc-geno-results apt-geno-qc-axiom.txt
        """
        ## Implement this command
        cmd = (["apt2-smn-status"]
               + ["--qc-gt-report", str(self.output / "step1" / "SMN" / "AxiomGT1.report.txt")]
               + ["--qc-cn-report", str(self.output / "step2" / "cn" / "AxiomCNVMix.report.txt")]
               + ["--smn-cn-regions", str(self.output / "step2" / "cn" / "AxiomCNVMix.cnregioncalls.txt")]
               + ["--smn-ab-report", str(self.output / "step2" / "SMN" / "SMN_ABreport.txt")]
               + ["--recommended-ps", str(self.output / "step2" / "SNPolisher" / "Recommended.ps")]
               + ["--blob-folder", str(self.output / "step2" / "AxiomAnalysisSuiteData")]
               + ["--carrier-thresholds", index_dir + "/Axiom_KU8.r2.SMN.carrier_thresholds.txt"]
               + ["--silent-marker-file", index_dir + "/Axiom_KU8.r2.SMN.snplist.txt"]
               + ["--output-dir", str(self.output / "step2" / "SMN")]
               + ["--qc-geno-results", str(self.output / "apt-geno-qc-axiom.txt")])
        self.run_command(cmd)

        ## 10. Execute:
        ## bcftools view         -e 'REF ~ "[()]" || ALT ~ "[()]" || REF ~ "D" || ALT ~ "D" || REF ~ "I" || ALT ~ "I" || REF="." || ALT="."' "$f" -Oz -o "cleaned/${base}.clean.vcf.gz";     bcftools index "cleaned/${base}.clean.vcf.gz"
        cmd = (["bcftools", "view"]
               + ["-e", 'REF ~ "[()]" || ALT ~ "[()]" || REF ~ "D" || ALT ~ "D" || REF ~ "I" || ALT ~ "I" || REF="." || ALT="."']
               + [str(self.output / "Axiom_KU8.vcf")]
               + ["-Oz", "-o", str(self.output / "Axiom_KU8.clean.vcf.gz")])
        self.run_command(cmd)
        
        ## Rename original VCF file
        os.rename(str(self.output / "Axiom_KU8.vcf"), str(self.output / "Axiom_KU8.vcf.bkp"))

        ## 4. Call to post_process
        # self.post_process()

        return [str(self.output / "Axiom_KU8.vcf")]

    def smn(self, smn_report: str):
        path = Path(smn_report)
        """
        #%array_type=Axiom_KU8,Axiom_KU8.r2,Axiom_KU8
        #%software_version=1.1
        #%analysis-date=Fri Dec  5 15:33:10 2025
        #%DQC_threshold=0.88
        #%QC_call_rate_threshold=98.5
        Sample	Status	DQC	QC_call_rate	CN_pass_QC	SMN1_CN	SMN1:Exon7	SMN1:Exon8	SMN2_CN	SMN1_g.27134T>G	SMN1_g.27134T>G_call
        A01_plate_1_sample_48217Date_18_11_2025.CEL	Normal	0.98814	99.69000	yes	1.99	2.15	1.95	1.01	T/T	REF/REF
        A02_plate_1_sample_11534Date_18_11_2025.CEL	Normal	0.99407	99.93500	yes	2.04	2.04	1.96	1.96	T/T	REF/REF
        A03_plate_1_sample_11535Date_18_11_2025.CEL	Normal	0.98617	99.93000	yes	2.01	2.17	1.90	0.99	T/T	REF/REF
        A04_plate_1_sample_32590Date_18_11_2025.CEL	Normal	0.98617	99.93000	yes	1.99	2.02	2.02	2.01	T/T	REF/REF
        """

        name_to_id = [
            {
                "id": "status",
                "name": "Status",
            },
            {
                "id": "dqc",
                "name": "DQC",
            },
            {
                "id": "qc_call_rate",
                "name": "QC_call_rate",
            },
            {
                "id": "cn_pass_qc",
                "name": "CN_pass_QC",
            },
            {
                "id": "smn1_cn",
                "name": "SMN1_CN",
            },
            {
                "id": "smn1_exon7",
                "name": "SMN1:Exon7"
            },
            {
                "id": "smn1_exon8",
                "name": "SMN1:Exon8",
            },
            {
                "id": "smn2_cn",
                "name": "SMN2_CN",
            },
            {
                "id": "smn1_variant",
                "name": "SMN1_g.27134T>G",
            },
            {
                "id": "smn1_variant_call",
                "name": "SMN1_g.27134T>G_call",
            }
        ]
        smn_calls: dict[str, dict[str, str]] = {}
        with path.open("r", encoding="utf-8") as fh:
            header: list[str] = []
            for line in fh:
                if line.startswith("#%"):
                    continue
                elif line.startswith("Sample"):
                    header = line.strip().split("\t")
                else:
                    fields = line.strip().split("\t")
                    sample_id = fields[0]
                    smn_calls[sample_id] = {}

                    ## We must use the comment above to translate header names to ID
                    for item in name_to_id:
                        try:
                            index = header.index(item["name"])
                            smn_calls[sample_id][item["id"]] = fields[index]
                        except ValueError:
                            self.logger.warning(f"Header name '{item['name']}' not found in SMN report.")

                    # for i in range(1, len(header)):
                    #     smn_calls[sample_id][header[i]] = fields[i]

        return smn_calls


    """
    Create a one-column file with all .CEL files and header line 'cel_files'
    """
    def create_cel_list_file(self):
        ## 1. Find all CEL files in the directory
        cel_files: list[str] = []
        data_dir_path = Path(self.pipeline.get("input", {}).get("dataDir", ""))
        if data_dir_path.is_dir():
            # Files are *.CEL file names in the directory
            cel_files = list(data_dir_path.glob("*.CEL"))

        ## 2. Create cel_files.txt file
        cel_list_path = self.output / "cel_files.txt"
        with cel_list_path.open("w", encoding="utf-8") as fh:
            fh.write("cel_files\n")
            for f in cel_files:
                fh.write(f"{f}\n")

        return cel_list_path

    """
    6.1 Filter CNV region calls to remove invalid regions (bug workaround).
    """
    def filter_cnregion_calls(self, input_filepath: str, output_filepath: str) -> None:
        """
        Filters a TSV file, removing data lines where the second column ('CN_Region')
        is not defined in the header as a '#%region-info-N=...' entry.

        Args:
            input_filepath: The path to the input file (e.g., 'AxiomCNVMix.cnregioncalls.txt').
            output_filepath: The path to write the filtered output file.

        Raises:
            FileNotFoundError: If the input file is not found.
            IOError: If an error occurs during file processing.
        """

        valid_regions: Set[str] = set()

        # 1. First pass: Collect all valid CN_Region names from the header
        try:
            with open(input_filepath, 'r') as infile:
                for line in infile:
                    line_stripped = line.strip()

                    if not line_stripped.startswith('#'):
                        # Stop processing header once data lines start
                        break

                    # Identify the region definition lines
                    if line_stripped.startswith('#%region-info-'):
                        # Format: #%region-info-N=CN_Region:rest_of_info
                        if '=' in line_stripped:
                            # Get the 'CN_Region:rest_of_info' part
                            region_definition = line_stripped.split('=', 1)[1]

                            # Get the CN_Region name (part before the first colon)
                            cn_region_name = region_definition.split(':', 1)[0]
                            valid_regions.add(cn_region_name)

        except FileNotFoundError:
            raise FileNotFoundError(f"Input file not found: {input_filepath}")

        # 2. Second pass: Filter and write lines to the output file
        self.logger.debug(f"Found {len(valid_regions)} valid regions in the header.")
        self.logger.debug(f"Writing filtered data to {output_filepath}...")

        try:
            with open(input_filepath, 'r') as infile, open(output_filepath, 'w') as outfile:
                for line in infile:
                    line_stripped = line.strip()

                    # Keep all header/comment lines (lines starting with #)
                    if line_stripped.startswith('#') or line_stripped.startswith('cel_files'):
                        outfile.write(line)
                        continue

                    # Process data lines
                    if line_stripped:
                        # Data lines are typically tab-separated (\t)
                        columns = line.split('\t')

                        # Check if the line has at least 2 columns
                        if len(columns) > 1:
                            # The second column is at index 1
                            cn_region_to_check = columns[1].strip()

                            # Only keep the line if the second column is a valid region
                            if cn_region_to_check in valid_regions:
                                outfile.write(line)
                        # Data lines with < 2 columns or an invalid region are discarded.

                    # Preserve entirely empty lines that contain only a newline character
                    elif not line_stripped and line == '\n':
                        outfile.write(line)

        except Exception as e:
            raise IOError(f"Error processing files: {e}")


    def post_process(self):
        # bcftools sort Axiom_KU8.vcf > Axiom_KU8.sorted.vcf
        # bcftools norm -d exact Axiom_KU8.sorted.vcf > Axiom_KU8.sorted.nodup.vcf
        # bcftools view -e 'REF ~ "D" || ALT ~ "D" || REF ~ "I" || ALT ~ "I"' Axiom_KU8.sorted.nodup.vcf -Ov -o Axiom_KU8.sorted.nodup.filtered.vcf
        # bcftools view -e 'REF ~ "[()]" || ALT ~ "[()]' Axiom_KU8.sorted.nodup.filtered.vcf -Oz -o Axiom_KU8.sorted.nodup.filtered.fixed.vcf.gz
        # bcftools index no_brackets.vcf.gz
        ## 1. Sort VCF
        cmd = (["bcftools"] + ["sort"]
               + [str(self.output / "Axiom_KU8.vcf")]
               + ["-Ov", "-o", str(self.output / "Axiom_KU8.sorted.vcf")])
        self.run_command(cmd)

        ## 2. Remove duplicates
        cmd = (["bcftools"] + ["norm", "-d", "exact"]
               + [str(self.output / "Axiom_KU8.sorted.vcf")]
               + ["-Ov", "-o", str(self.output / "Axiom_KU8.sorted.nodup.vcf")])
        self.run_command(cmd)

        ## 3. Filter indels
        cmd = (["bcftools"] + ["view", "-e", 'REF ~ "D" || ALT ~ "D" || REF ~ "I" || ALT ~ "I"']
               + [str(self.output / "Axiom_KU8.sorted.nodup.vcf")]
               + ["-Ov", "-o", str(self.output / "Axiom_KU8.sorted.nodup.filtered.vcf")])
        self.run_command(cmd)

        ## 4. Remove brackets from alleles
        cmd = (["bcftools"] + ["view", "-e", 'REF ~ "[()]" || ALT ~ "[()]"']
               + [str(self.output / "Axiom_KU8.sorted.nodup.filtered.vcf")]
               + ["-Oz", "-o", str(self.output / "Axiom_KU8.sorted.nodup.filtered.fixed.vcf.gz")])
        self.run_command(cmd)

        ## 5. Index final VCF
        cmd = (["bcftools"] + ["index"]
               + [str(self.output / "Axiom_KU8.sorted.nodup.filtered.fixed.vcf.gz")])
        self.run_command(cmd)

        return str(self.output / "Axiom_KU8.sorted.nodup.filtered.fixed.vcf.gz")
