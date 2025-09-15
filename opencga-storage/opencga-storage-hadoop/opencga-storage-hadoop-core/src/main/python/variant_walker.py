import sys
import importlib
import importlib.machinery
import importlib.util
import os
from abc import ABC, abstractmethod

class VariantWalker(ABC):
    @abstractmethod
    def setup(self, *arg):
        """
        This function is responsible for setting up any necessary configurations
        before processing the entries.
        *args: Configuration arguments.
        """
        pass

    @abstractmethod
    def header(self, header):
        """
        This function will process the header as a list of strings.
        header (list): A list of strings representing the header.
        """
        pass

    @abstractmethod
    def map(self, line):
        """
        This function processes each entry.

        Args:
            line (str): A line read from stdin.
        """
        pass

    @abstractmethod
    def cleanup(self):
        """
        This function is responsible for any cleanup tasks after all entries have been processed.
        """
        pass

    def count(self, key, increment):
        """
        Increment a counter with a given value.

        Args:
            key (str): Counter name
            increment (int): Counter increment
        """
        if not all(char.isalnum() or char in ['_', '-'] for char in key):
            raise ValueError("Invalid key. Key can only contain alphanumeric characters, underscores, and hyphens.")

        print(f"reporter:counter:{key},{increment}", file=sys.stderr)

    def write(self, value):
        """
        Write a value to stdout.

        Args:
            value (str): The value to write.
        """
        print(value)

    # def jsonHeaderToVcfHeader(self, jsonHeader):
    #     """
    #     Convert a JSON header to a VCF header.
    #
    #     Args:
    #         jsonHeader (dict): The JSON header to convert.
    #     """
    #     # TODO: Implement this method
    #     return ""


    def getTmpdir(self):
        """
        Get the output directory.

        Returns:
            str: The output directory.
        """
        return os.environ.get("TMPDIR", "/tmp")




def main(module_name, class_name, *args):
    """
    This is the main function that sets up the environment, reads lines from stdin,
    processes them using the map function, and performs cleanup tasks.

    Args:
        module_name (str): The name of the module where the VariantWalker subclass is defined.
        class_name (str): The name of the VariantWalker subclass to use.
        *args: Additional arguments to pass to the setup method of the VariantWalker subclass.
    """
    ## If the modulename is a fileName, use the source file loader to load the module
    if module_name.endswith(".py"):
        ## If the modulename is a relative path, we need to make it an absolute path prepending the current working dir
        if not module_name.startswith("/"):
            module_name = f"{os.getcwd()}/{module_name}"

        loader = importlib.machinery.SourceFileLoader( 'walker_module', module_name )
        spec = importlib.util.spec_from_loader( 'walker_module', loader )
        module = importlib.util.module_from_spec( spec )
        loader.exec_module( module )
    else:
        module = importlib.import_module(module_name)

    WalkerClass = getattr(module, class_name)
    walker = WalkerClass()

    try:
        walker.setup(*args)
    except Exception as e:
        print(f"An error occurred during setup: {e}", file=sys.stderr)
        raise

    input_lines_count = 0
    header_lines_count = 0
    header_size_bytes = 0
    variant_count = 0
    variant_size_bytes = 0

    header_read = False
    header = []
    for line in sys.stdin:
        input_lines_count = input_lines_count + 1
        # Now 'line' does not have trailing '\n' or '\r'
        line = line.rstrip()

        ## The line will be a header line if it starts with '#' or if it's the first line
        if not header_read:
            if line.startswith("#") or input_lines_count == 1:
                header.append(line)
                header_lines_count = header_lines_count + 1
                header_size_bytes = header_size_bytes + len(line)
                ## Keep reading header lines until we find a non-header line
                continue
            else:
                ## Process the header
                header_read = True
                try:
                    walker.header(header)
                except Exception as e:
                    print(f"An error occurred while processing the header: {e}", file=sys.stderr)
                    raise
                header = None

        variant_count = variant_count + 1
        variant_size_bytes = variant_size_bytes + len(line)
        try:
            walker.map(line)
        except Exception as e:
            print(f"An error occurred while processing a line: {e}", file=sys.stderr)
            raise

    walker.count("input_lines_count", input_lines_count)
    walker.count("header_lines_count", header_lines_count)
    walker.count("header_size_bytes", header_size_bytes)
    walker.count("variant_count", variant_count)
    walker.count("variant_size_bytes", variant_size_bytes)
    try:
        walker.cleanup()
    except Exception as e:
        print(f"An error occurred during cleanup: {e}", file=sys.stderr)
        raise
    return 0

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python variant_walker.py <module_name> <class_name> [args...]", file=sys.stderr)
        sys.exit(1)
    sys.exit(main(sys.argv[1], sys.argv[2], *sys.argv[3:]))
