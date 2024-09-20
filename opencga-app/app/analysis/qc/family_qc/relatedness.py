import os
import logging
import gzip
import json

from utils import create_output_dir, execute_bash_command

LOGGER = logging.getLogger('variant_qc_logger')

