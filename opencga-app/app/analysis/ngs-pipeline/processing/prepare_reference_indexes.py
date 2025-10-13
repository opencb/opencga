from pathlib import Path

from typing_extensions import override

from processing.base_processor import BaseProcessor


class PrepareReferenceIndexes(BaseProcessor):

    def __init__(self, reference_genome: str, indexes: list, output: Path, logger=None):
        """
        Initialize PrepareReference with config and output parameters.
        Parameters
        ----------
        reference : str
            Path or URL to the reference genome file
        output : Path
            Output directory path
        logger : logging.Logger, optional
            Logger instance for logging messages
        """
        super().__init__(output, logger)
        self.reference_genome = reference_genome
        self.indexes = indexes


    """
    A single prepare-reference step.
    """
    @override
    def execute(self) -> dict[str, str] | int:
        if self.reference_genome is None:
            self.logger.error("No reference genome file provided for indexing")
            return {"error": "No reference genome file provided for indexing"}

        ## 1. Download or copy the reference genome to self.output
        reference_path = ""
        if self.reference_genome.startswith(("http://", "https://", "ftp://")):
            self.logger.info("Downloading reference genome from URL: %s", self.reference_genome)
            ref_filename = Path(self.reference_genome).name
            cmd = ["wget", "-O", str(self.output / ref_filename), self.reference_genome]
            self.run_command(cmd)
            reference_path = str(self.output / ref_filename)
        else:
            ## Check if the reference file exists
            ref_path = Path(self.reference_genome).resolve()
            if not ref_path.is_file():
                self.logger.error("ERROR: Reference genome file not found: %s", ref_path)
                return {"error": f"Reference genome file not found: {ref_path}"}

            ## Copy the file to self.output
            dest_path = self.output / ref_path.name
            if ref_path != dest_path:
                self.logger.info("Copying reference genome to output directory: %s", dest_path)
                cmd = ["cp", str(ref_path), str(dest_path)]
                self.run_command(cmd)
                reference_path = str(dest_path)

        ## 2. Create indexes as specified in self.indexes
        self.logger.debug("Creating indexes: %s", ", ".join(self.indexes))
        for idx in self.indexes:
            match idx:
                case "reference-genome":
                    self.reference_genome_index(reference_path)
                case "bwa":
                    self.bwa_index(reference_path)
                case "bwa-mem2":
                    if self.check_tool_availability("bwa-mem2"):
                        self.bwa_mem2_index(reference_path)
        return 0


    def reference_genome_index(self, reference_path: str) -> str:
        """
            Create FASTA index and sequence dictionary for the reference genome.

            This method copies the reference genome to a reference index directory,
            decompresses it if it's gzipped, and creates the FASTA index (.fai) and
            sequence dictionary (.dict) files required for various genomics tools.

            Parameters
            ----------
            reference_path : str
                Path to the reference genome file

            Returns
            -------
            str
                Path to the decompressed reference FASTA file
        """

        # Create reference index directory
        reference_index_dir = self.output / "reference-genome-index"
        reference_index_dir.mkdir(parents=True, exist_ok=True)

        # Copy reference genome to index directory
        cmd = ["cp", str(reference_path), str(reference_index_dir)]
        self.run_command(cmd)

        # Decompress the reference genome file (assumes .gz format)
        reference_gz_path = str(reference_index_dir / Path(reference_path).name)
        cmd = ["gunzip", reference_gz_path]
        self.run_command(cmd)

        # Create FASTA index for the decompressed reference
        reference_fa_path = str(reference_index_dir / Path(reference_gz_path).stem)
        cmd = ["samtools"] + ["faidx"] + [reference_fa_path]
        self.run_command(cmd)

        # Create sequence dictionary for the reference
        reference_dict_path = str(reference_index_dir / Path(reference_fa_path).stem)
        cmd = ["samtools"] + ["dict"] + ["-o", reference_dict_path + ".dict"] + [reference_fa_path]
        self.run_command(cmd)

        return reference_fa_path


    def bwa_index(self, reference_path: str) -> str:
        """ Create BWA index for the reference genome.
        This method copies the reference genome to a BWA index directory,
        decompresses it if it's gzipped, and creates the BWA index files
        required for sequence alignment.

        Parameters
        ----------
        reference_path : str
            Path to the reference genome file

        Returns
        -------
        str
            The original reference path
        """

        # Create BWA index directory
        bwa_index_dir = self.output / "bwa-index"
        bwa_index_dir.mkdir(parents=True, exist_ok=True)

        # Copy reference genome to BWA index directory
        cmd = ["cp", str(reference_path), str(bwa_index_dir)]
        self.run_command(cmd)

        # Decompress the reference genome file (assumes .gz format)
        bwa_reference_gz_path = str(bwa_index_dir / Path(reference_path).name)
        cmd = ["gunzip", bwa_reference_gz_path]
        self.run_command(cmd)

        # Create BWA index files for the decompressed reference
        bwa_reference_fa_path = str(bwa_index_dir / Path(bwa_reference_gz_path).stem)
        cmd = ["bwa"] + ["index"] + [bwa_reference_fa_path]
        self.run_command(cmd)
        return reference_path


    def bwa_mem2_index(self, reference_path: str) -> str:
        """Create BWA-MEM2 index for the reference genome.
            This method copies the reference genome to a BWA-MEM2 index directory,
            decompresses it if it's gzipped, and creates the BWA-MEM2 index files
            required for sequence alignment.
            Parameters
            ----------
            reference_path : str
                Path to the reference genome file
            Returns
            -------
            str
                The original reference path
        """

        # Create BWA index directory
        bwa_index_dir = self.output / "bwa-mem2-index"
        bwa_index_dir.mkdir(parents=True, exist_ok=True)

        # Copy reference genome to BWA index directory
        cmd = ["cp", str(reference_path), str(bwa_index_dir)]
        self.run_command(cmd)

        # Decompress the reference genome file (assumes .gz format)
        bwa_reference_gz_path = str(bwa_index_dir / Path(reference_path).name)
        cmd = ["gunzip", bwa_reference_gz_path]
        self.run_command(cmd)

        # Create BWA index files for the decompressed reference
        bwa_reference_fa_path = str(bwa_index_dir / Path(bwa_reference_gz_path).stem)
        cmd = ["bwa-mem2"] + ["index"] + [bwa_reference_fa_path]
        self.run_command(cmd)
        return reference_path

