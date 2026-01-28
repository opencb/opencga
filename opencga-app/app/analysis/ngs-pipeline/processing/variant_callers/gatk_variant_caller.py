from __future__ import annotations

from pathlib import Path

from processing.variant_callers.variant_caller import VariantCaller


class GatkVariantCaller(VariantCaller):

    def __init__(self, output: Path, logger=None):
        super().__init__(output, logger)
        self.tool_name = "gatk"
        self.docker_image = "broadinstitute/gatk:4.6.2.0"


    """ Run GATK variant calling """
    # @override
    def call(self, input_config: dict, tool_config: dict) -> list[str]:
        reference_path = self._get_reference_index_dir(tool_config, input_config)

        bam_file = list(self.output.parent.parent.glob("alignment/*.sorted.bam"))
        vcf_files = []
        for bam_file in bam_file:
            bam_file_path = Path(str(bam_file))

            if bam_file_path.is_file():

                dedup_bam_file = self._apply_best_practices(bam_file_path, input_config, tool_config)
                dedup_bam_file_path = Path(dedup_bam_file)
                vcf_file = self._build_vcf_file_name(dedup_bam_file_path, ".gatk.vcf")

                # Use Docker to run GATK HaplotypeCaller
                self._run_gatk_haplotype_caller(reference_path, dedup_bam_file_path, vcf_file)

                # cmd = (["gatk", "HaplotypeCaller", "-R", str(reference_path)]
                #        + ["-I", str(dedup_bam_file_path)]
                #        + ["-O", str(self.output / vcf_file)])
                # self.run_command(cmd)

                ## Post-process SAM to sorted BAM and index
                vcf_processed = self._vcf_post_processing(vcf_file)
                if vcf_processed:
                    vcf_files.append(str(self.output / vcf_processed))
            else:
                self.logger.error("BAM file %s does not exist for GATK variant calling", str(bam_file))

        return vcf_files

    def _run_gatk_haplotype_caller(self, reference_path: Path, bam_file_path: Path, vcf_file: str):
        """Run GATK HaplotypeCaller using Docker"""
        input_bindings = {
            reference_path.parent: "/reference",
            bam_file_path.parent: "/input",
        }
        self.run_docker_command(self.docker_image,
                                ["gatk", "HaplotypeCaller",
                                 "-R", f"/reference/{reference_path.name}",
                                 "-I", f"/input/{bam_file_path.name}",
                                 "-O", f"/output/{vcf_file}"],
                                input_bindings,
                                {self.output: "/output"})


    def _apply_best_practices(self, bam_file_path: Path, input_config: dict, tool_config: dict) -> str:
        ## GATK best practices can be implemented here
        self.logger.info("GATK best practices workflow is not implemented yet.")

        # bam_files = list(self.output.parent.parent.glob("alignment/*.sorted.bam"))
        # for bam_file in bam_files:
        # bam_file_path = Path(str(bam_file))

        if not bam_file_path.is_file():
            self.logger.error("BAM file %s does not exist for GATK best practices workflow", str(bam_file_path))
            return None

        ## 1. MarkDuplicates: Create a deduplicated BAM file
        dedup_bam_file = str(self.output / (bam_file_path.stem + ".dedup.bam"))
        dedup_bam_file_metrics = str(self.output / (bam_file_path.stem + ".dedup.metrics"))
        self._run_gatk_mark_duplicates(bam_file_path, dedup_bam_file, dedup_bam_file_metrics)
        # cmd_dedup = (["gatk", "MarkDuplicates", "-I", str(bam_file_path)]
        #              + ["-O", str(dedup_bam_file)]
        #              + ["-M", str(dedup_bam_file_metrics)])
        # self.run_command(cmd_dedup)

        dedup_bai_file = dedup_bam_file + ".bai"
        cmd = ["samtools"] + ["index", "-b"] + ["-@", "2"] + [dedup_bam_file]
        self.run_command(cmd)

        ## 2. BaseRecalibrator: Create a recalibration table

        ## 2.1 Download known sites from 'https://console.cloud.google.com/storage/browser/_details/genomics-public-data/resources/broad/hg38/v0/1000G_omni2.5.hg38.vcf.gz' if not provided:
        # wget_cmd = (["wget", "-O", str(self.output / "known_sites.vcf.gz")]
        #             + [tool_config.get("knownSites", "https://console.cloud.google.com/storage/browser/_details/genomics-public-data/resources/broad/hg38/v0/hapmap_3.3.hg38.vcf.gz")])
        # self.run_command(wget_cmd)
        #
        # ## 2.2 Gunzip the known sites VCF
        # gunzip_cmd = (["gunzip", "-f", str(self.output / "known_sites.vcf.gz")])
        # self.run_command(gunzip_cmd)
        #
        # ## 2.2 Run BaseRecalibrator
        # recal_table = bam_file_path.stem + ".recal.table"
        # cmd_recal = (["gatk", "BaseRecalibrator", "-I", str(self.output / dedup_bam_file)]
        #              + ["-R", str(self._get_reference_index_dir(tool_config, input_config))]
        #              + ["--known-sites", str(self.output / "known_sites.vcf")]
        #              + ["-O", str(self.output / recal_table)])
        # self.run_command(cmd_recal)

        return dedup_bam_file

    def _run_gatk_mark_duplicates(self, input_bam: Path, output_bam: str, metrics_file: str):
        """Run GATK MarkDuplicates using Docker"""

        # Set up volume bindings
        bindings = []

        # Input BAM directory binding
        input_parent = input_bam.parent
        bindings.extend(["--mount", f"type=bind,source={input_parent},target=/input,readonly"])

        # Output directory binding
        bindings.extend(["--mount", f"type=bind,source={self.output},target=/output"])

        # Get relative paths
        input_bam_name = input_bam.name
        output_bam_name = Path(output_bam).name
        metrics_name = Path(metrics_file).name

        # Build Docker command
        docker_cmd = (["docker", "run", "--rm"]
                      + bindings
                      + [self.docker_image]
                      + ["gatk", "MarkDuplicates"]
                      + ["-I", f"/input/{input_bam_name}"]
                      + ["-O", f"/output/{output_bam_name}"]
                      + ["-M", f"/output/{metrics_name}"])

        self.logger.info("Running GATK MarkDuplicates with Docker command: %s", ' '.join(docker_cmd))
        self.run_command(docker_cmd)
