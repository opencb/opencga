from lark import Lark, Tree


class Grammar:
    def __init__(self, logger=None):
        super().__init__()
        self.parser = None
        self.grammar = r"""
            ?expr: expr "OR" expr       -> or_op
                 | expr "AND" expr      -> and_op
                 | expr "NOT IN" expr   -> not_in_op
                 | "(" expr ")"
                 | CNAME                -> var
        
            %import common.CNAME
            %import common.WS
            %ignore WS
        """
        self.logger = logger


    def parse(self, execution_logic: str) -> Tree:
        # Example -> execution_logic = "((query1 OR query2) AND (query3 OR query4)) AND (query5 OR query6)"
        # Create the parser if it doesn't exist
        if self.parser is None:
            self.parser = Lark(self.grammar, start="expr")

        # Parse the execution logic
        tree = self.parser.parse(execution_logic)
        self.logger.debug(f"Parsed execution logic tree: {tree.pretty()}")
        return tree
