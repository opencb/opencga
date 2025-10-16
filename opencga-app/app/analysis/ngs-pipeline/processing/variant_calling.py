import subprocess
from pathlib import Path

from .base_processor import BaseProcessor
from .variant_callers import GatkVariantCaller


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
    # @override
    def execute(self) -> list[str]:
        ## 1. Check if input samples are provided
        input_config = self.pipeline.get("input")
        if len(input_config.get("samples", [])) == 0:
            self.logger.error("No input samples provided for bwa")
            raise ValueError("No input files provided for bwa")

        ## 2. Get variant-calling step configuration
        self.logger.info("Starting VariantCalling step: %s", self.__class__.__name__)
        variant_calling_config = next((s for s in self.pipeline.get("steps", []) if s.get("id") == "variant-calling"), {})
        tool_config = variant_calling_config.get("tools")[0]  # Assume first tool for simplicity
        self.logger.debug("Configuration for QualityControl: %s", variant_calling_config)

        ## 3. Select variant caller based on tool ID
        variant_caller = None
        match (tool_config.get("id").upper()):
            case "GATK":
                self.logger.debug("Using GATK variant caller")
                variant_caller = GatkVariantCaller(self.output, self.logger)
            # case "frebayes":
            #     self.logger.debug("Using FreeBayes variant caller")
            #     variant_caller = BwaMem2Aligner(self.output, self.logger)
            # case "mutect2":
            #     self.logger.debug("Using Mutect2 variant caller")
            #     variant_caller = Minimap2Aligner(self.output, self.logger)
            case _:
                self.logger.error("Unsupported aligner tool: %s", tool_config.get("id"))
                raise ValueError(f"Unsupported aligner tool: {tool_config.get('id')}")


        if variant_caller is not None:
            ## Perform variant calling
            vcfs = variant_caller.call(input_config, tool_config)

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
