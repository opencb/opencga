import subprocess
from pathlib import Path

from .base_processor import BaseProcessor
from .variant_callers import GatkVariantCaller
from .variant_callers import FreebayesVariantCaller
from .variant_callers import Mutect2VariantCaller


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
    A single variant-calling step.
    Implement `run` with concrete checks. `execute` wraps `run` adding logging
    and common error handling.
    """
    # @override
    def execute(self) -> list[str] | None:
        ## 1. Get variant-calling step configuration
        self.logger.info("Starting VariantCalling step: %s", self.__class__.__name__)
        variant_calling_config = self.pipeline.get("steps", {}).get("variantCalling", {})
        self.logger.debug("Configuration for VariantCalling: %s", variant_calling_config)

        ## 2. Check if step is defined and active
        if not variant_calling_config or not variant_calling_config.get("active", True):
            self.logger.warning("VariantCalling step is not defined or not active. Skipping.")
            return None

        ## 3. Loop over each tool in the variant-calling step
        variant_caller = None
        vcfs = []
        for tool_config in variant_calling_config.get("tools", []):
            self.logger.debug("Variant calling tool configuration: %s", tool_config)

            ## 3.1. Select variant caller based on tool ID
            tool_id = tool_config.get("id")

            ## 3.2. Create output directory for variant calling
            variant_calling_output_dir = self.output / tool_id
            variant_calling_output_dir.mkdir(parents=True, exist_ok=True)
            self.logger.debug("Variant calling output directory: %s", str(variant_calling_output_dir))

            match (tool_id.upper()):
                case "GATK":
                    self.logger.debug("Using GATK variant caller")
                    variant_caller = GatkVariantCaller(variant_calling_output_dir, self.logger)
                case "FREEBAYES":
                    self.logger.debug("Using FreeBayes variant caller")
                    variant_caller = FreebayesVariantCaller(variant_calling_output_dir, self.logger)
                case "MUTECT2":
                    self.logger.debug("Using Mutect2 variant caller")
                    variant_caller = Mutect2VariantCaller(variant_calling_output_dir, self.logger)
                case _:
                    self.logger.error("Unsupported aligner tool: %s", tool_id)
                    raise ValueError(f"Unsupported aligner tool: {tool_id}")


            if variant_caller is not None:
                ## Perform variant calling
                vcfs = variant_caller.call(self.pipeline.get("input"), tool_config)

                ## Options:
                ## 1. Clean up intermediate files if specified in config
                if variant_calling_config.get("options", {}).get("clean", True):
                    variant_caller.clean()

                ## 2. Check is 'qc' options is set to True in config
                if variant_calling_config.get("options", {}).get("qc", True):
                    variant_caller.qc(vcfs)
            else:
                self.logger.error("VariantCaller instance could not be created.")
                raise ValueError("VariantCaller instance could not be created.")

        return vcfs


    def _get_reference_index_dir(self, tool_config: dict, input_config: dict) -> Path:
        reference_index_dir = tool_config.get("reference") or str(input_config.get("indexDir") + "/" + "reference-genome-index")

        ## If reference index is not provided, raise error
        if not reference_index_dir:
            raise ValueError("Reference index not provided in tool or input configuration")

        ## Get the fasta file
        fasta_list = list(Path(reference_index_dir).glob("*.fasta")) + list(Path(reference_index_dir).glob("*.fa")) + list(Path(reference_index_dir).glob("*.fna"))
        reference_path = list(fasta_list)[0] if fasta_list else None
        if not reference_path:
            raise ValueError("No FASTA file found in the reference index directory")
        return reference_path

    """ Run GATK HaplotypeCaller """
    def gatk(self, input: dict, gatk_tool_config: dict) -> str:
        # if len(input.get("files", [])) == 0:
        #     self.logger.error("No input files provided for bwa")
        #     return {"error": "No input files provided for bwa"}

        reference_path = self._get_reference_index_dir(gatk_tool_config, input)

        bam_file = list(self.output.parent.glob("alignment/*.sorted.bam"))[0]
        if Path(bam_file).is_file():
            vcf_file = Path(bam_file).stem + ".gatk.vcf"
            cmd = ["gatk", "HaplotypeCaller", "-R", str(reference_path)] + ["-I", str(bam_file)] + ["-O", str(self.output / vcf_file)]
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
