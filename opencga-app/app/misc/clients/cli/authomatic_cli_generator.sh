#!/bin/bash
# -*- ENCODING: UTF-8 -*-
echo "Building Option files for the command line"
python3 options_cli_generator.py
echo "Building Executor files for the command line"
python3 executors_cli_generator.py
echo "Building Parser file for the command line"
python3 parser_cli_generator.py
echo "Execution finish!!!"

exit

