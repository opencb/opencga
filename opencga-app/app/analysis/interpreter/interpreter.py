import getpass
import json
import os
from pathlib import Path

from pyopencga.opencga_client import OpencgaClient  # import client module
from pyopencga.opencga_config import ClientConfiguration  # import configuration module

from lib import Grammar, ExecTransformer, VariantFilter


class Interpreter:

    def __init__(self, pipeline: dict, output: Path, logger=None):
        """
        Initialize Pipeline with config and output parameters.

        Parameters
        ----------
        pipeline : dict
            Configuration dictionary
        output : dict
            Output dictionary
        logger : logging.Logger, optional
        """
        self.pipeline = pipeline
        self.output = output
        self.logger = logger

    def _create_opencga_client(self, args):
        config_dict = {'rest': {
            'host': self.pipeline.get("opencga", {}).get("host", ""), }
        }
        self.logger.debug(f"OpenCGA configuration: {config_dict}")

        ## 1. Load configuration and create the OpenCGA client
        client_config = ClientConfiguration(config_dict)

        ## 2. Read OPENCGA_TOKEN from environment variable and set it in the client
        token = os.getenv("OPENCGA_TOKEN")
        if token:
            self.logger.debug("Setting OpenCGA token from environment variable OPENCGA_TOKEN")
            oc = OpencgaClient(client_config, token=token)
            self.logger.debug("OpenCGA Client token: %s", oc.token)
        else:
            self.logger.warning("Environment variable OPENCGA_TOKEN not set; proceeding without authentication token")
            ## 3. If no token is provided, call to login with user and password from argparse
            oc = OpencgaClient(client_config)
            user = self.pipeline.get("opencga", {}).get("user", "")
            password = args.password or getpass.getpass("OpenCGA password: ")
            organization = self.pipeline.get("opencga", {}).get("organization", "")
            if user and password:
                self.logger.debug(f"Logging in to OpenCGA as user: {user}")
                oc.login(user, password, organization)
                self.logger.debug("OpenCGA Client token: %s", oc.token)
                self.logger.debug("OpenCGA Client configuration: %s",
                                  json.dumps(vars(oc.configuration), default=str, indent=2))
            else:
                self.logger.error("OpenCGA user or password not provided; cannot login")
                return 1
        return oc


    def execute(self, case_id: str, study_id: str, args) -> int:
        ## 1. Create OpenCGA client and authenticate
        oc = self._create_opencga_client(args)

        ## 2. Get study
        if not study_id or study_id == "":
            self.logger.debug("Study parameter not provided, retrieving OPENCGA_STUDY from environment variable")
            study_id = os.getenv("OPENCGA_STUDY")
            if not study_id or study_id == "":
                self.logger.error("Study parameter not provided and OPENCGA_STUDY environment variable not set")
                return 1
            else:
                self.logger.info("Using study from OPENCGA_STUDY environment variable: %s", study_id)
        else:
            self.logger.debug("Using study parameter from CLI: %s", study_id)

        ## 3. Retrieve clinical case information
        response = oc.get_clinical_client().info(case_id, study=study_id)
        clinical_analysis = response.get_result(0)

        grammar = Grammar(self.logger)
        execution_logic = "(((query1 OR query2) AND (query3 OR query4)) AND (query5 OR query6)) NOT IN query7"
        print(f"1. Execution logic: {execution_logic}\n")

        tree = grammar.parse(execution_logic)
        print(f"2. Execution logic tree after parsing the query: {tree.pretty()}\n")

        query_sets = {
            "query1": {"v1", "v2"},
            "query2": {"v3"},
            "query3": {"v2", "v4"},
            "query4": {"v5"},
            "query5": {"v2", "v6"},
            "query6": {"v7"},
            "query7": {"v3"},
        }
        print(f"3. Example query results: {query_sets}\n")

        query_sets = set()
        variant_filter = VariantFilter(opencga_client=oc, output=self.output, logger=self.logger)
        queries = self.pipeline.get("queries", [])
        self.logger.debug(f"Queries to execute: {queries}")
        for query in queries:
            self.logger.info(f"Executing query: {query.get('name', 'Unnamed Query')}")
            # Here you would implement the logic to execute each query using the OpenCGA client
            # and process the results as needed.
            # This is a placeholder for demonstration purposes.
            query['study'] = args.study
            query['include'] = "id"
            # r = self.opencga_client.get_clinical_client().query_variant(query)
            result = variant_filter.query(query)
            query_sets[query.get('id', 'Unnamed Query')] = result
            print(f"Query result: {result}")
            self.logger.debug(f"Query details: {query}")

        exec_transformer = ExecTransformer(query_sets, self.logger)
        result_set = exec_transformer.execute(tree)
        print(f"4. Results after executing query: {result_set}\n")

        return 0

