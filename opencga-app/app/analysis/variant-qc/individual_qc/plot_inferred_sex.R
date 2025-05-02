library(ggplot2)

# Read command line arguments
args <- commandArgs(trailingOnly = TRUE)

if (length(args) < 4) {
  stop("Usage: Rscript plot_inferred_sex.R <title> <sex check file> <reference file> <output_file>")
}

# Parse input parameters
title <- args[1]   # title
reference_file <- args[2]   # Reference file
sexcheck_file <- args[3]       # PLINK sex check file
output_file <- args[4]           # Output image file path

# Check if the reference file exists before reading
if (!file.exists(reference_file)) {
  stop(paste("Error: Reference file", reference_file, "does not exist."))
}

ref_table <- read.table(reference_file, header=T, sep="\t", stringsAsFactors=F)

# Check if the sexcheck file exists before reading
if (!file.exists(sexcheck_file)) {
  stop(paste("Error: Sex check file", sexcheck_file, "does not exist."))
}

sexcheck_table <- read.table(sexcheck_file, header=T, sep="", stringsAsFactors=F)
sexcheck_table$REF <- title

# Check and adjust columns before rbind()
missing_cols <- setdiff(colnames(ref_table), colnames(sexcheck_table))
sexcheck_table[missing_cols] <- NA

missing_cols <- setdiff(colnames(sexcheck_table), colnames(ref_table))
ref_table[missing_cols] <- NA

# Ensure the same column order
sexcheck_table <- sexcheck_table[, colnames(ref_table)]

print(colnames(ref_table))
print(colnames(sexcheck_table))

ref_table.sexcheck <- rbind(ref_table, sexcheck_table)

p1 <- ggplot(ref_table.sexcheck, aes(as.character(PEDSEX), F, fill=as.character(SNPSEX)))+geom_jitter(aes(fill=as.character(SNPSEX)), shape=21,
 color="black", size=2)+theme_bw()+scale_fill_manual(values=c("blue","red"), name="Predicted Sex",
  labels=c("male","female"))+scale_x_discrete(name="Supplied Sex",
   labels=c("1"="male", "2"="female"))+geom_point(sexcheck_table, mapping=aes(as.character(PEDSEX),F), shape=23, color="black", size=4)

ggsave(output_file, p1)