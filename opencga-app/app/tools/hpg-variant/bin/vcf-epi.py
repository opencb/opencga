#!/usr/bin/python
import sys, os, argparse, commands

def execute(cmd):
    status, output = commands.getstatusoutput(cmd)
    if	status != 0:
	print("EXIT ERROR")
	print(str(status) + " -> " + output + " - " + cmd)
	sys.exit(1)


def main():

    appPath = os.path.abspath(os.path.join(os.environ["_"], os.pardir))

    parser = argparse.ArgumentParser(prog="variant")

    parser.add_argument("--outdir",      help = "Directory where the output files will be stored", required=True)
    parser.add_argument("--config",      help = "")

    # vcf2epi
    parser.add_argument("--vcf-file",    help = "VCF file used as input", required=True)
    parser.add_argument("--ped-file",    help = "PED file used as input")
    parser.add_argument("--species",     help = "Species whose genome is taken as reference")
    parser.add_argument("--alleles",     help = "Filter: by number of alleles")
    parser.add_argument("--coverage",    help = "Filter: by minimum coverage")
    parser.add_argument("--quality",     help = "Filter: by minimum quality")
    parser.add_argument("--maf",         help = "Filter: by MAF (minimum allele frequency, decimal like 0.01)")
    parser.add_argument("--missing",     help = "Filter: by maximum missing values (decimal like 0.1)")
    parser.add_argument("--gene",        help = "Filter: by a comma-separated list of genes")
    parser.add_argument("--region",      help = "Filter: by a list of regions (chr1:start1-end1,chr2:start2-end2...)")
    parser.add_argument("--region-file", help = "Filter: by a list of regions (read from a GFF file)")
    parser.add_argument("--region-type", help = "Filter: by type of region (used along with the 'region-file' argument)")
    parser.add_argument("--snp",         help = "Filter: by being a SNP or not (include/exclude)")
    parser.add_argument("--indel",       help = "Filter: by being an indel or not, counting characters in REF and ALT (include/exclude)")
    parser.add_argument("--inh-dom",     help = "Filter: by percentage of samples following dominant inheritance pattern (decimal like 0.1)")
    parser.add_argument("--inh-rec",     help = "Filter: by percentage of samples following recessive inheritance pattern (decimal like 0.1)")

    # epi
    parser.add_argument("--order",       help = "Number of SNPs to be combined at the same time")
    parser.add_argument("--num-folds",   help = "Number of folds in a k-fold cross-validation")
    parser.add_argument("--num-cv-runs", help = "Number of times the k-fold cross-validation process is run")
    parser.add_argument("--rank-size",   help = "Number of best models saved")
    parser.add_argument("--eval-subset", help = "Whether to used training (default) or testing partitions when evaluating the best models")
    parser.add_argument("--eval-mode",   help = "Whether to rank risky combinations by their CV-C or CV-A (values can be 'count' or 'accu')")
    parser.add_argument("--stride",      help = "Number of SNPs per block partition of the dataset")



    args = parser.parse_args()

    args_vcf2epi = ""
    args_epi = ""

    if args.vcf_file:
        args_vcf2epi += " --vcf-file " + args.vcf_file

    if args.ped_file:
        args_vcf2epi += " --ped-file " + args.ped_file

    #if args.out:
        #args_vcf2epi += " --out " + args.out

    if args.species:
        args_vcf2epi += " --species " + args.species


    if args.alleles:
        args_vcf2epi += " --alleles " + args.alleles


    if args.coverage:
        args_vcf2epi += " --coverage " + args.coverage


    if args.quality:
        args_vcf2epi += " --quality " + args.quality


    if args.maf:
        args_vcf2epi += " --maf " + args.maf


    if args.missing:
        args_vcf2epi += " --missing " + args.missing


    if args.gene:
        args_vcf2epi += " --gene " + args.gene


    if args.region:
        args_vcf2epi += " --region " + args.region


    if args.region_file:
        args_vcf2epi += " --region_file " + args.region_file


    if args.region_type:
        args_vcf2epi += " --region_type " + args.region_type


    if args.snp:
        args_vcf2epi += " --snp " + args.snp


    if args.indel:
        args_vcf2epi += " --indel " + args.indel


    if args.inh_dom:
        args_vcf2epi += " --inh_dom " + args.inh_dom


    if args.inh_rec:
        args_vcf2epi += " --inh_rec " + args.inh_rec


    if args.order:
        args_epi += " --order " + args.order


    if args.num_folds:
        args_epi += " --num_fold " + args.num_folds


    if args.num_cv_runs:
        args_epi += " --num_cv_runs " + args.num_cv_runs


    if args.rank_size:
        args_epi += " --rank_size " + args.rank_size


    if args.eval_subset:
        args_epi += " --eval_subset " + args.eval_subset


    if args.eval_mode:
        args_epi += " --eval_mode " + args.eval_mode


    if args.stride:
        args_epi += " --stride " + args.stride

    if args.config:
        args_epi += " --config " + args.config
        args_vcf2epi += " --config " + args.config


    args_vcf2epi += " --outdir " + args.outdir
    args_epi += " --outdir " + args.outdir

    command_vcfepi = appPath + "/hpg-var-vcf vcf2epi " + args_vcf2epi
    command_epi    = appPath + "/hpg-var-gwas epi --dataset "  + args.outdir + "/epistasis_dataset.bin " + args_epi

    print(command_vcfepi)
    execute(command_vcfepi)


    print(command_epi)
    execute(command_epi)


main()
