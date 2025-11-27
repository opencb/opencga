#!/usr/bin/env python3
import argparse
import json
import logging
import sys
from datetime import datetime
from pathlib import Path

from lib.interpreter import Interpreter

# Define global constants
VALID_STEPS = ["quality-control", "alignment", "variant-calling"]
SENTINEL = "DONE"

# Define global logger
outdir = None
logger = None


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Pipeline runner", formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    subparsers = parser.add_subparsers(dest="command", required=True)

    ## --- index command ---
    prepare_parser = subparsers.add_parser("prepare", help="Index the reference genome")
    prepare_parser.add_argument("-r", "--reference-genome", required=True, help="Path or URL to the reference genome in FASTA format")
    prepare_parser.add_argument("-i", "--indexes", default="bwa,bwa-mem2,minimap2,bowtie2,hisat2,affy", help="Comma-separated list of indexes to prepare (reference-genome,bwa,bwa-mem2,minimap2). Reference-genome is always executed.")
    prepare_parser.add_argument("-c", "--clean", action="store_true", help="Clean existing directory before running")
    prepare_parser.add_argument("-l", "--log-level", default="info", choices=["debug", "info", "warning", "error"], help="Set console logging level")
    prepare_parser.add_argument("-o", "--outdir", required=True, help="Base output directory, index subfolders will be created")

    ## --- genomics command ---
    run_parser = subparsers.add_parser("interpreter", help="Align reads to reference genome and call variants")
    run_parser.add_argument("-p", "--pipeline", required=True, help="Pipeline JSON file to execute")
    # run_parser.add_argument("-s", "--samples", help="Samples to be processed. Accepted format: sample_id::file1,file2::somatic(0/1)::role(F/M/C/U)")
    # run_parser.add_argument("--samples-file", help="File containing samples to be processed, one per line. Accepted format: sample_id::file1,file2::somatic(0/1)::role(F/M/C/U)")
    # run_parser.add_argument("-i", "--index-dir", help="Directory containing reference and aligner indexes")
    # run_parser.add_argument("--steps", default="quality-control,alignment,variant-calling", help="Pipeline step to execute")
    # run_parser.add_argument("--overwrite", action="store_true", help="Force re-run even if step previously completed")
    run_parser.add_argument("--organization", help="OpenCGA user")
    run_parser.add_argument("-s", "--study", help="Pipeline step to execute")
    run_parser.add_argument("-u", "--user", help="OpenCGA user")
    run_parser.add_argument("--password", help="OpenCGA user")
    run_parser.add_argument("-l", "--log-level", default="INFO", choices=["debug", "info", "warning", "error"], help="Set console logging level")
    run_parser.add_argument("-o", "--outdir", required=True, help="Base output directory, step subfolders will be created")

    return parser.parse_args(argv)

def prepare_step_dir(base_outdir: Path, step: str):
    step_dir = base_outdir / step
    step_dir.mkdir(parents=True, exist_ok=True)
    return step_dir

def mark_completed(step_dir: Path):
    (step_dir / SENTINEL).write_text(f"Completed: {datetime.now().isoformat()}Z\n")

def is_completed(step_dir: Path):
    return (step_dir / SENTINEL).is_file()

def create_output_dir(args):
    global outdir

    ## Get absolute path of outdir and create if not exists
    outdir = Path(args.outdir).resolve()
    outdir.mkdir(parents=True, exist_ok=True)
    return outdir

def configure_logger(args):
    global outdir, logger

    ## Determine log level from args
    outdir = Path(args.outdir).resolve()
    log_level = getattr(logging, args.log_level.upper(), logging.INFO)

    ## Create logger
    logger = logging.getLogger("ngs_pipeline")
    logger.setLevel(logging.DEBUG)  # master level, controls what gets through to handlers

    ## File handler
    file_handler = logging.FileHandler(str(outdir / "app.log"))
    file_handler.setLevel(logging.DEBUG)  # log everything (DEBUG+) to file
    file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_formatter)

    ## Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)  # log_level and above to console
    console_formatter = logging.Formatter('%(levelname)s - %(message)s')
    console_handler.setFormatter(console_formatter)

    ## Add handlers to logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

def get_pipeline(pipeline: str):
    ## 1. Load pipeline configuration
    pipeline_path = Path(pipeline).resolve()
    if not pipeline_path.is_file():
        logger.error(f"ERROR: 'pipeline' configuration file not found: {pipeline_path}")
        return 1
    with pipeline_path.open("r", encoding="utf-8") as fh:
        pipeline = json.load(fh)
    if not isinstance(pipeline, dict):
        logger.error(f"ERROR: 'pipeline' configuration is not a JSON object: {pipeline_path}")
        return 1
    return pipeline


def interpreter(args):
    ## 1. Load pipeline configuration
    # pipeline_path = Path(args.pipeline).resolve()
    # if not pipeline_path.is_file():
    #     logger.error(f"ERROR: 'pipeline' configuration file not found: {pipeline_path}")
    #     return 1
    # with pipeline_path.open("r", encoding="utf-8") as fh:
    #     pipeline = json.load(fh)
    # if not isinstance(pipeline, dict):
    #     logger.error(f"ERROR: 'pipeline' configuration is not a JSON object: {pipeline_path}")
    #     return 1
    ## 1. Load pipeline configuration
    pipeline = get_pipeline(args.pipeline)
    if not pipeline or not isinstance(pipeline, dict):
        return 1

    ## 2. Create Interpreter instance
    interpreter = Interpreter(pipeline, outdir, logger)
    interpreter.execute(args)



def main(argv=None):
    args = parse_args(argv)

    ## 1. Get absolute path of outdir and create if not exists
    create_output_dir(args)

    ## 2. Configure global logger
    configure_logger(args)

    # 2. Run the appropriate command
    logger.debug(f"Executing command '{args.command}' in output directory '{outdir}'")
    match args.command:
        case "interpreter":
            return interpreter(args)
        case _:
            logger.error(f"Unknown command: {args.command}")
            return 1


if __name__ == "__main__":
    sys.exit(main())
