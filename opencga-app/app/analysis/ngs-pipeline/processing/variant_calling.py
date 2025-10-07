import subprocess
from pathlib import Path

from processing.base_processor import BaseProcessor


class VariantCalling(BaseProcessor):

    def __init__(self, pipeline: dict, output: Path, logger=None):
        """
        Initialize QualityControl with config and output parameters.

        Parameters
        ----------
        config : dict
            Configuration dictionary
        output : dict
            Output dictionary
        """
        super().__init__(output, logger)
        self.pipeline = pipeline


    """
    A single quality-control step.

    Implement `run` with concrete checks. `execute` wraps `run` adding logging
    and common error handling.
    """
    def execute(self) -> dict:
        self.logger.info("Starting VariantCalling step: %s", self.__class__.__name__)
        variant_calling_config = next((s for s in self.pipeline.get("steps", []) if s.get("name") == "variant-calling"), {})
        self.logger.debug("Configuration for QualityControl: %s", variant_calling_config)

        ## Get the tool in the quality-control step of the pipeline dict
        input = self.pipeline.get("input")
        gatk_tool_config = variant_calling_config.get("tools")[0] # Assume first tool for simplicity
        result = None
        if isinstance(gatk_tool_config, dict):
            result = self.gatk(input, gatk_tool_config)
        elif not isinstance(gatk_tool_config, dict):
            raise ValueError("Invalid tool configuration format in quality-control step")
        return result


    """ Run GATK HaplotypeCaller """
    def gatk(self, input: dict, gatk_tool_config: dict) -> str:
        # if len(input.get("files", [])) == 0:
        #     self.logger.error("No input files provided for bwa")
        #     return {"error": "No input files provided for bwa"}

        bam_file = list(self.output.parent.glob("alignment/*.sorted.bam"))[0]
        if Path(bam_file).is_file():
            vcf_file = Path(bam_file).stem + ".gatk.vcf"
            cmd = ["gatk", "HaplotypeCaller", "-R", gatk_tool_config.get("reference")] + ["-I", str(bam_file)] + ["-O", str(self.output / vcf_file)]
            self.run_command(cmd)

            ## Run bcftools stats
            #cmd = ["bcftools", "stats"] + ["-s", "-"] + [str(self.output / vcf_file)] + [">", str(self.output / (Path(vcf_file).stem + ".bcftools.stats"))]
            #self.run_command(cmd)

            ## Check if MultiQC is installed and run it
            multiqc_installed = self.run_command(["which", "multiqc"], check=False)
            if multiqc_installed.returncode == 0:
                cmd = ["multiqc"] + ["-o", str(self.output)] + [str(self.output / ".")]
                self.run_command(cmd)

            return vcf_file
        return None

    def mutect2(self, input: dict, mutect2_tool_config: dict) -> str:
        if len(input.get("files", [])) == 0:
            self.logger.error("No input files provided for bwa")
            return {"error": "No input files provided for bwa"}

        bam_file = list(self.output.parent.glob("alignment/*.sorted.bam"))[0]
        if (Path(bam_file).is_file()):
            # files = input.get("files", [])

            vcf_file = Path(bam_file).stem + ".mutect2.vcf"
            cmd = ["gatk", "Mutect2", "-R", mutect2_tool_config.get("reference")] + ["-I", bam_file] + ["-O", str(self.output / vcf_file)]
            self.run_command(cmd)
            return vcf_file

    def freebayes(self, input: dict, freebayes_tool_config: dict) -> str:
        if len(input.get("files", [])) == 0:
            self.logger.error("No input files provided for bwa")
            return {"error": "No input files provided for bwa"}

        bam_file = list(self.output.parent.glob("alignment/*.sorted.bam"))[0]
        if (Path(bam_file).is_file()):
            # files = input.get("files", [])

            vcf_file = Path(bam_file).stem + ".freebayes.vcf"
            cmd = ["freebayes", "-f", freebayes_tool_config.get("reference")] + [bam_file] + [">", str(self.output / vcf_file)]
            self.run_command(cmd)
            return vcf_file

    def strelka2(self, input: dict, strelka2_tool_config: dict) -> str:
        if len(input.get("files", [])) == 0:
            self.logger.error("No input files provided for bwa")
            return {"error": "No input files provided for bwa"}

        bam_file = list(self.output.parent.glob("alignment/*.sorted.bam"))[0]
        if (Path(bam_file).is_file()):
            # files = input.get("files", [])

            vcf_file = Path(bam_file).stem + ".strelka2.vcf"
            cmd = ["configureStrelkaGermlineWorkflow.py", "--referenceFasta", strelka2_tool_config.get("reference"), "--bam", bam_file, "--runDir", str(self.output / "strelka2_run")]
            self.run_command(cmd)

            run_cmd = [str(self.output / "strelka2_run" / "runWorkflow.py"), "-m", "local", "-j", "4"]
            self.run_command(cmd)

            vcf_path = self.output / "strelka2_run" / "results" / "variants" / "variants.vcf.gz"
            if vcf_path.is_file():
                subprocess.run(["gunzip", str(vcf_path)], check=True)
                return str(vcf_path.with_suffix(''))
        return None

    def gatk_best_practices(self):
        pass
