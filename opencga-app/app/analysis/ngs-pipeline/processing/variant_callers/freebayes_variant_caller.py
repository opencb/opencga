from __future__ import annotations

from pathlib import Path

from processing.variant_callers.variant_caller import VariantCaller


class FreebayesVariantCaller(VariantCaller):

    def __init__(self, output: Path, logger=None):
        super().__init__(output, logger)
        self.tool_name = "freebayes"


    """ Run Freebayes variant calling """
    # @override
    def call(self, input_config: dict, tool_config: dict) -> list[str]:
        reference_path = self._get_reference_index_dir(tool_config, input_config)

        bam_files = list(self.output.parent.parent.glob("alignment/*.sorted.bam"))
        vcf_files = []
        for bam_file in bam_files:
            bam_file_path = Path(str(bam_file))

            if bam_file_path.is_file():
                vcf_file = self._build_vcf_file_name(bam_file_path, ".freebayes.vcf")
                cmd = (["freebayes", "-f", str(reference_path)]
                       + [str(bam_file)]
                       + ["--vcf", str(self.output / vcf_file)])
                self.run_command(cmd)

                ## Post-process SAM to sorted BAM and index
                vcf_processed = self._vcf_post_processing(vcf_file)
                if vcf_processed:
                    vcf_files.append(str(self.output / vcf_processed))
            else:
                self.logger.error("BAM file %s does not exist for Freebayes variant calling", str(bam_file))

        return vcf_files

    def best_practices(self, input_config: dict, tool_config: dict) -> str:
        reference_path = self._get_reference_index_dir(tool_config, input_config)

        bam_file = list(self.output.parent.glob("alignment/*.sorted.bam"))
        if len(bam_file) != 2:
            self.logger.error("Best practices workflow requires exactly two BAM files (tumor and normal). Found %d", len(bam_file))
            return None

        tumor_bam = None
        normal_bam = None
        for bf in bam_file:
            if "tumor" in bf.name.lower():
                tumor_bam = bf
            elif "normal" in bf.name.lower():
                normal_bam = bf

        if not tumor_bam or not normal_bam:
            self.logger.error("Could not identify tumor and normal BAM files based on naming convention.")
            return None

        if tumor_bam.is_file() and normal_bam.is_file():
            vcf_file = "sample.best_practices.vcf"
            cmd = (["gatk", "Mutect2", "-R", str(reference_path)]
                   + ["-I", str(tumor_bam)]
                   + ["-I", str(normal_bam)]
                   + ["-O", str(self.output / vcf_file)])
            self.run_command(cmd)

            ## Post-process SAM to sorted BAM and index
            vcf_processed = self._vcf_post_processing(str(self.output / vcf_file))
            if vcf_processed:
                return str(self.output / vcf_processed)

            return vcf_file
        else:
            self.logger.error("BAM files do not exist for GATK best practices variant calling: %s, %s", str(tumor_bam), str(normal_bam))

        return None