#!/bin/bash
SCRIPT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
$BABELOMICS_HOME/babelomics.sh --tool association "$*"
while [[ $# > 1 ]]
do
key="$1"
case $key in
   --outdir)
   outdir="$2"
   shift
   ;;
   # unknown option
   *)
   ;;
esac
shift
done
export MANHATTAN_PLOT_BASE_DIR="$SCRIPT_DIR"/Manhattan_plot
echo "$SCRIPT_DIR"/Manhattan_plot/scripts/create_manhattan_plot.pl --output_format=png --title_plot='Manhattan_plot' --output_dir=$outdir --assoc_file_path=$outdir/plink.assoc --manhattan_plot_base_dir "$SCRIPT_DIR"/Manhattan_plot



perl "$SCRIPT_DIR"/Manhattan_plot/scripts/create_manhattan_plot.pl --output_format=png --title_plot='Manhattan_plot' --output_dir=$outdir --assoc_file_path=$outdir/plink.assoc --manhattan_plot_base_dir "$SCRIPT_DIR"/Manhattan_plot
