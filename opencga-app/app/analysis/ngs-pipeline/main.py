#!/usr/bin/env python3
import argparse
import json
import logging
import shutil
import sys
from datetime import datetime
from pathlib import Path

from processing import QualityControl, Alignment, VariantCalling, PrepareReference

VALID_STEPS = ["quality-control", "alignment", "variant-calling"]
SENTINEL = "DONE"

# Get a logger for the current module
logger = logging.getLogger(__name__)

def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Pipeline runner", formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    subparsers = parser.add_subparsers(dest="command", required=True)

    # --- index command ---
    prepare_parser = subparsers.add_parser("prepare", help="Index the reference genome")
    prepare_parser.add_argument("-r", "--reference", required=True, help="Pipeline step to execute")
    prepare_parser.add_argument("-i", "--index", default="reference-genome,bwa,bwa-mem2", help="Pipeline step to execute")
    prepare_parser.add_argument("-l", "--log-level", default="info", choices=["debug", "info", "warning", "error"], help="Set logging level")
    prepare_parser.add_argument("-o", "--outdir", required=True, help="Base output directory (step subfolder will be created)")

    # --- align command ---
    run_parser = subparsers.add_parser("run", help="Align reads to reference genome")
    run_parser.add_argument("-p", "--pipeline", help="Pipeline step to execute")
    run_parser.add_argument("-i", "--input", help="Input data file or directory")
    run_parser.add_argument("--index", help="Input data file or directory")
    run_parser.add_argument("--steps", default="quality-control,alignment,variant-calling", help="Pipeline step to execute")
    run_parser.add_argument("--resume", action="store_true", help="Resume previous failed run in the step directory")
    run_parser.add_argument("--overwrite", action="store_true", help="Force re-run even if step previously completed")
    run_parser.add_argument("--clean", action="store_true", help="Clean existing step directory before running")
    run_parser.add_argument("-l", "--log-level", default="info", choices=["debug", "info", "warning", "error"], help="Set logging level")
    run_parser.add_argument("-o", "--outdir", required=True, help="Base output directory (step subfolder will be created)")
    return parser.parse_args(argv)

def prepare_step_dir(base_outdir: Path, step: str):
    step_dir = base_outdir / step
    step_dir.mkdir(parents=True, exist_ok=True)
    return step_dir

def mark_completed(step_dir: Path):
    (step_dir / SENTINEL).write_text(f"Completed: {datetime.now().isoformat()}Z\n")

def is_completed(step_dir: Path):
    return (step_dir / SENTINEL).is_file()


def prepare(args):
    ## 1. Get absolute path of outdir
    outdir = Path(args.outdir).resolve()
    outdir.mkdir(parents=True, exist_ok=True)
    logger.debug(f"Output directory resolved to: {outdir}")

    ## 2. Validate the index types
    indexes = args.index.split(",")
    for idx in indexes:
        if idx not in ["reference-genome", "bwa", "bwa-mem2"]:
            logger.error(f"ERROR: Unsupported index type specified: {idx}")
            return 1

    ## 3. Calculate indexes and prepare reference
    prepare_reference = PrepareReference(reference=args.reference, indexes=indexes, output=outdir, logger=logger)
    prepare_reference.execute()
    return 0

def run(args):
    ## 1. Get absolute path of outdir
    outdir = Path(args.outdir).resolve()
    outdir.mkdir(parents=True, exist_ok=True)
    logger.debug(f"Output directory resolved to: {outdir}")

    ## 2. Handle --clean option
    if args.clean:
        logger.info(f"Cleaning existing output directory: {outdir}")
        ## removing all contents of outdir
        for item in outdir.iterdir():
            if item.is_dir():
                shutil.rmtree(item)
            elif item.is_file():
                item.unlink()
            logger.info("Output directory cleaned.")
        return 0

    ## 3. Load pipeline configuration
    pipeline_path = Path(args.pipeline).resolve()
    if not pipeline_path.is_file():
        logger.error(f"ERROR: 'pipeline' configuration file not found: {pipeline_path}")
        return 1
    with pipeline_path.open("r", encoding="utf-8") as fh:
        pipeline = json.load(fh)
    if not isinstance(pipeline, dict):
        logger.error(f"ERROR: 'pipeline' configuration is not a JSON object: {pipeline_path}")
        return 1

    ## 4. Set input in pipeline configuration if provided
    if args.input:
        files = args.input.split(",")
        # if not input_path.exists():
        #     logger.error(f"ERROR: 'input' path not found: {input_path}")
        #     return 1
        type = "fastq" if files[0].endswith((".fastq", ".fastq.gz", ".fq", ".fq.gz")) else "bam" if files[0].endswith(
            ".bam") else "unknown"
        pipeline.get("input", {}).update({"files": files, "type": type})
        # logger.info(f"Input set in pipeline configuration: {pipeline['input']}")
        # logger.info(f"Input set in pipeline configuration: {pipeline}")
    ## 4.1 Check input files are valid
    ## TODO: implement more thorough checks based on type

    ## 5. Set reference in pipeline configuration if provided
    if args.index:
        pipeline.get("input", {}).update({"index": args.index})

    ## 6. Prepare first step execution
    steps = args.steps.split(",")
    ## Check that all steps are in VALID_STEPS
    for step in steps:
        if step not in VALID_STEPS:
            logger.error(f"ERROR: Invalid step specified: {step}. Valid steps are: {', '.join(VALID_STEPS)}")
            return 1

    ## 7. Execute steps in order
    for step in steps:
        ## 1. Prepare step directory and implementation
        step_dir = prepare_step_dir(outdir, step)
        impl = None
        logger.debug(f"Starting step='{step}' resume={args.resume} overwrite={args.overwrite}")
        match step:
            case "quality-control":
                impl = QualityControl(pipeline=pipeline, output=step_dir, logger=logger)
            case "alignment":
                impl = Alignment(pipeline=pipeline, output=step_dir, logger=logger)
            case "variant-calling":
                impl = VariantCalling(pipeline=pipeline, output=step_dir, logger=logger)
            case _:
                logger.debug("Done with all steps.")
                return 1

        ## 2. Execute step if implementation exists
        if impl:
            if not is_completed(step_dir):
                logger.debug(f"Executing step: '{step}'; No existing completion detected")
                impl.execute()
                mark_completed(step_dir)
            else:
                if args.overwrite:
                    logger.debug(
                        "Executing step: '{step}'; Existing completion detected, but overwriting as requested.")
                    impl.execute()
                    mark_completed(step_dir)
                else:
                    logger.warning(f"Step '{step}' already completed. Use --overwrite to force.")


def main(argv=None):
    args = parse_args(argv)

    ## Configure basic logging (optional, if not already configured)
    log_level = getattr(logging, args.log_level.upper(), logging.INFO)
    logging.basicConfig(level=log_level, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    if args.command == "prepare":
        prepare(args)
        return 0

    if args.command == "run":
        run(args)
        return 0

    return None

if __name__ == "__main__":
    sys.exit(main())
