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
                case "bowtie2":
                    if self.check_tool_availability("bowtie2-build"):
                        self.bowtie2_index(reference_path)
                case "hisat2":
                    if self.check_tool_availability("hisat2-build"):
                        self.hisat2_index(reference_path)
                case "minimap2":
                    if self.check_tool_availability("minimap2"):
                        self.minimap2_index(reference_path)
        return 0

    def _copy_and_decompress_reference(self, reference_path: str, dest_dir: Path) -> str:
        """Copy and decompress the reference genome file to the destination directory.

        Parameters
        ----------
        reference_path : str
            Path to the reference genome file
        dest_dir : Path
            Destination directory to copy and decompress the file

        Returns
        -------
        str
            Path to the decompressed reference FASTA file
        """
        ## Copy reference genome to destination directory
        cmd = ["cp", str(reference_path), str(dest_dir)]
        self.run_command(cmd)

        ## Decompress the reference genome file (assumes .gz format)
        reference_gz_path = str(dest_dir / Path(reference_path).name)
        cmd = ["gunzip", reference_gz_path]
        self.run_command(cmd)

        ## Get path to decompressed reference FASTA file
        reference_fa_path = str(dest_dir / Path(reference_gz_path).stem)

        # Return path to decompressed reference FASTA file
        return reference_fa_path


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

        # Copy and gunzip reference genome to index directory
        reference_fa_path = self._copy_and_decompress_reference(reference_path, reference_index_dir)

        # Create FASTA index for the decompressed reference
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

        # Copy and gunzip reference genome to index directory
        reference_fa_path = self._copy_and_decompress_reference(reference_path, bwa_index_dir)

        # Create BWA index files for the decompressed reference
        cmd = ["bwa"] + ["index"] + [reference_fa_path]
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

        # Copy and gunzip reference genome to index directory
        reference_fa_path = self._copy_and_decompress_reference(reference_path, bwa_index_dir)

        # Create BWA index files for the decompressed reference
        cmd = ["bwa-mem2"] + ["index"] + [reference_fa_path]
        self.run_command(cmd)
        return reference_path


    def minimap2_index(self, reference_path: str) -> str:
        """ Create Minimap2 index for the reference genome.

        This method copies the reference genome to a Minimap2 index directory,
        decompresses it if it's gzipped, and creates the Minimap2 index files
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
        # Create Minimap2 index directory
        minimap2_index_dir = self.output / "minimap2-index"
        minimap2_index_dir.mkdir(parents=True, exist_ok=True)

        # Copy and gunzip reference genome to index directory
        reference_fa_path = self._copy_and_decompress_reference(reference_path, minimap2_index_dir)

        # Create Minimap2 index files for the decompressed reference
        cmd = ["minimap2"] + ["-d", str(minimap2_index_dir / (Path(reference_fa_path).stem + ".mmi"))] + [reference_fa_path]
        self.run_command(cmd)
        return reference_path


    def bowtie2_index(self, reference_path: str) -> str:
        """ Create Bowtie2 index for the reference genome.
        This method copies the reference genome to a Bowtie2 index directory,
        decompresses it if it's gzipped, and creates the Bowtie2 index files
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
        # Create Bowtie2 index directory
        bowtie2_index_dir = self.output / "bowtie2-index"
        bowtie2_index_dir.mkdir(parents=True, exist_ok=True)

        # Copy and gunzip reference genome to index directory
        reference_fa_path = self._copy_and_decompress_reference(reference_path, bowtie2_index_dir)

        # Create Bowtie2 index files for the decompressed reference
        cmd = ["bowtie2-build"] + ["--threads", "2"] + [reference_fa_path] + [str(bowtie2_index_dir / Path(reference_fa_path).stem)]
        self.run_command(cmd)
        return reference_path


    def hisat2_index(self, reference_path: str) -> str:
        """ Create HISAT2 index for the reference genome.

        This method copies the reference genome to a HISAT2 index directory,
        decompresses it if it's gzipped, and creates the HISAT2 index files
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
        # Create HISAT2 index directory
        hisat2_index_dir = self.output / "hisat2-index"
        hisat2_index_dir.mkdir(parents=True, exist_ok=True)

        # Copy and gunzip reference genome to index directory
        reference_fa_path = self._copy_and_decompress_reference(reference_path, hisat2_index_dir)

        # Create HISAT2 index files for the decompressed reference
        cmd = ["hisat2-build"] + ["-p", "2"] + [reference_fa_path] + [str(hisat2_index_dir / Path(reference_fa_path).stem)]
        self.run_command(cmd)
        return reference_path

