from pathlib import Path

from processing.base_processor import BaseProcessor


class Affymetrix(BaseProcessor):
    def __init__(self, output: Path, logger=None):
        """
        Initialize Aligner with config and output parameters.
        Parameters
        ----------
        output : Path
            Output directory path
        logger : logging.Logger, optional
            Logger instance for logging messages
        """
        super().__init__(output, logger)

    @abstractmethod
    def process(self, input_config: dict, tool_config: dict) -> list[str]:
        pass