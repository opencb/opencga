from pathlib import Path

import requests


class VariantFilter:

    def __init__(self, opencga_client, output: Path, logger=None):
        super().__init__()
        self.opencga_client = opencga_client
        self.output = output
        self.logger = logger


    # def parse(self):
    #     self.parser = Lark(self.grammar, start="expr")

    def query(self, query: dict) -> set:
        ## 1. Process query dictionary
        # 1. Convert lists to comma-separated strings for OpenCGA query
        for key, value in query.items():
            if isinstance(value, list):
                query[key] = ",".join(map(str, value))

        # 2. Remove empty values from the query
        query = {k: v for k, v in query.items() if v}

        # 3. Convert HPO terms to gene list if 'hpo' key is present
        if 'hpo' in query:
            hpo_terms = query.pop('hpo')
            gene_list = self._get_gene_list(hpo_terms)
            self.logger.debug(f"Genes associated with HPO terms: {gene_list}")
            if gene_list:
                query['gene'] = ",".join(gene_list)
            else:
                self.logger.info("No genes found for provided HPO terms; proceeding without gene filter.")

        # 4.

        # 5. Log the final query
        self.logger.debug(f"Final variant query parameters: {query}")

        ## 2. Query variants from OpenCGA Clinical
        response = self.opencga_client.get_clinical_client().query_variant(**query)
        variant_ids = set()
        for variant in response.get_results():
            variant_ids.add(variant.get('id'))

        self.logger.debug(f"Queried {len(variant_ids)} variants with query: {query}")
        return variant_ids


    def _get_gene_list(self, hpo_terms: str) -> list:
        """
        Retrieve genes associated with given HPO terms using CellBase.
        Args:
            hpo_terms (list): List of HPO term dictionaries
        Returns:
            list: List of gene IDs associated with the HPO terms
        """

        ## 1. Extract HPO terms from the case
        hpo_gene_ids = []
        if len(hpo_terms) >= 1:
            # hpo_terms_ids = ",".join(hpo_terms)
            self.logger.debug("Pipeline - Case HPO terms: %s", hpo_terms)

            # Call to this URL and fetch JSON result:  https://ws.zettagenomics.com/cellbase/webservices/rest/v5.8/hsapiens/feature/gene/search?include=id,name,biotype&biotype=protein_coding&limit=5000&disease=HP:0000407
            url = "https://ws.zettagenomics.com/cellbase/webservices/rest/v5.8/hsapiens/feature/gene/search"
            params = {
                "include": "id,name,biotype",
                "biotype": "protein_coding",
                "limit": 5000,
                "disease": hpo_terms
            }
            self.logger.debug("Pipeline - Requesting CellBase gene search: %s params=%s", url, params)
            try:
                resp = requests.get(url, params=params, timeout=30)
                resp.raise_for_status()
                data = resp.json()
                # Extract results (CellBase wraps them in responses -> results)
                genes = (data.get("responses") or [{}])[0].get("results", [])
                self.logger.info("Pipeline - CellBase gene count: %d", len(genes))
            except Exception as e:
                self.logger.error("Pipeline - CellBase request failed: %s", e)
                genes = []

            # Extract gene IDs
            hpo_gene_ids = list(dict.fromkeys(g.get('name') for g in genes if isinstance(g, dict) and 'name' in g))
            self.logger.debug("Pipeline - Unique genes associated with HPO terms (count): %i", len(hpo_gene_ids))
        else:
            self.logger.info("Pipeline - No HPO terms provided, skipping CellBase gene retrieval.")

        return hpo_gene_ids
