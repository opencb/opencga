#!/bin/bash
SCRIPT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

Rscript $SCRIPT_DIR/rna_diffexpr.r "$@"