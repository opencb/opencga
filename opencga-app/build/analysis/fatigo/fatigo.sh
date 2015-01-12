#!/bin/bash
SCRIPT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

#echo "$*"
"$SCRIPT_DIR"/babelomics/babelomics.sh --tool fatigo "$*"


while [[ $# > 1 ]]
do
key="$1"

case $key in
    --outdir)
    OUTDIR="$2"
    shift
    ;;
    *)
            # unknown option
    ;;
esac
shift
done

echo "$OUTDIR"

for i in $( ls $OUTDIR | grep "go_molecular_function\|go_biological_process\|go_cellular_component" | grep -v "0.005\|0.05\|0.1\|0.01" | grep ".txt" ); do
    Rscript $SCRIPT_DIR/fatigo-graph/get_GOsubgraph.r $OUTDIR/$i 0.005
    Rscript $SCRIPT_DIR/fatigo-graph/get_GOsubgraph.r $OUTDIR/$i 0.05
    Rscript $SCRIPT_DIR/fatigo-graph/get_GOsubgraph.r $OUTDIR/$i 0.01
    Rscript $SCRIPT_DIR/fatigo-graph/get_GOsubgraph.r $OUTDIR/$i 0.1
done
