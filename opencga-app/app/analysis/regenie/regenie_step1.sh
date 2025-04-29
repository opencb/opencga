#!/bin/bash

# Check if the required parameters are provided
if [ -z "$1" ] || [ -z "$2" ] || [ -z "$3" ] || [ -z "$4" ]; then
  echo "Usage: $0 <input dir> <VCF basename> <phenotype filename>"
  echo "  <input dir>:    The input directory."
  echo "  <VCF basename>: The VCF base name."
  echo "  <phenotype filename>: The phenotype filename."
  echo "  <output dir>:    The output directory."
  exit 1
fi

# Assign the input parameters to variables
input_dir="$1"
basename="$2"
pheno_filename="$3"
output_dir="$4"

# Add a trailing slash to dir if it's not already there.
if [[ "$dir" != */ ]]; then
    dir="$dir/"
fi

echo "Running with input dir: $input_dir, basename: $basename, phenotype filename: $pheno_filename and output dir: $output_dir"

# 1. bcftools annotate
echo "1. Annotating VCF using bcftools..."
bcftools annotate --set-id '%CHROM:%POS:%REF:%FIRST_ALT' "${input_dir}/${basename}.vcf.gz" -Oz > "${input_dir}/${basename}.annotated.vcf.gz"
if [ $? -ne 0 ]; then
  echo "Error: bcftools annotate failed!"
  exit 1
fi

# 2. plink --make-bed
echo "2. Running plink --make-bed..."
plink1.9 --vcf "${input_dir}/${basename}.annotated.vcf.gz" --make-bed --out "${input_dir}/${basename}.annotated" --memory 30600
if [ $? -ne 0 ]; then
  echo "Error: plink --make-bed failed!"
  exit 1
fi

# 3. plink --indep-pairwise
echo "3. Running plink --indep-pairwise..."
#plink1.9 --bfile "${input_dir}/${basename}.annotated" --indep-pairwise 50 5 0.2 --maf 0.05 --out "${input_dir}/variants" --memory 30600
plink1.9 --bfile "${input_dir}/${basename}.annotated" --indep-pairwise 50 5 0.2 --out "${input_dir}/variants" --memory 30600
if [ $? -ne 0 ]; then
  echo "Error: plink --indep-pairwise failed!"
  exit 1
fi

# 4. plink --extract
echo "4. Running plink --extract..."
plink1.9 --bfile "${input_dir}/${basename}.annotated" --extract "${input_dir}/variants.prune.in" --make-bed --out "${input_dir}/${basename}.annotated.pruned.in" --memory 30600
if [ $? -ne 0 ]; then
  echo "Error: plink --extract failed!"
  exit 1
fi

# 5. regenie --step 1
echo "5. Running regenie --step 1..."
regenie --step 1 --bed "${input_dir}/${basename}.annotated.pruned.in" --bsize 1000 --out "${output_dir}/step1" --phenoFile "${input_dir}/${pheno_filename}" --bt
if [ $? -ne 0 ]; then
  echo "Error: regenie --step 1 failed!"
  exit 1
fi

echo "All steps completed successfully!"
