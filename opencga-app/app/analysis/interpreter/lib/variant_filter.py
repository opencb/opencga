from pathlib import Path

from pyopencga.opencga_client import OpencgaClient


class VariantFilter:

    def __init__(self, opencga_client, output: Path, logger=None):
        super().__init__()
        self.opencga_client = opencga_client
        self.output = output
        self.logger = logger


    # def parse(self):
    #     self.parser = Lark(self.grammar, start="expr")

    def query(self, query: dict) -> set:
        response = self.opencga_client.get_clinical_client().query_variant(query)
        variant_ids = set()
        for result in response.get('responses', []):
            for variant in result.get('results', []):
                variant_ids.add(variant.get('id'))
        self.logger.debug(f"Queried {len(variant_ids)} variants with query: {query}")
        return variant_ids


