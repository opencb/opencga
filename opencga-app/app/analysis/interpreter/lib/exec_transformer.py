from lark import Transformer


class ExecTransformer(Transformer):
    def __init__(self, query_sets, logger=None):
        super().__init__()
        self.query_sets = query_sets
        self.logger = logger

    def var(self, name):
        key = str(name[0])
        value = self.query_sets[key]
        print(f"\tRetrieving set for '{key}':", value)
        return value

    def and_op(self, items):
        print(f"\t\tAND operation between: {items}\n")
        left, right = items
        return left & right    # intersection

    def or_op(self, items):
        print(f"\t\tOR operation between: {items}\n")
        left, right = items
        return left | right    # union

    def not_in_op(self, items):
        print(f"\t\tNOT IN operation between: {items}\n")
        left, right = items
        return left - right    # difference

    def execute(self, tree):
        result = self.transform(tree)
        self.logger.debug(f"Final result set: {result}")
        return result
