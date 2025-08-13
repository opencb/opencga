# deseq2_script.R

# --- 0. Load Libraries ---
# This script requires DESeq2, jsonlite, and ggplot2.
# Make sure they are installed in your R environment.
suppressPackageStartupMessages(library("DESeq2"))
suppressPackageStartupMessages(library("jsonlite"))
suppressPackageStartupMessages(library("ggplot2"))

# --- 1. Load Configuration from JSON ---
args <- commandArgs(trailingOnly = TRUE)
if (length(args) != 1) {
  stop("Usage: Rscript deseq2_script.R <path_to_config.json>", call. = FALSE)
}
config <- fromJSON(args[1])
cat("Configuration loaded.\n")

# --- 2. Load Input Data ---
cat("Loading data...\n")
counts_data <- read.csv(config$input$countsFile, header = TRUE, row.names = 1)
metadata <- read.csv(config$input$metadataFile, header = TRUE, row.names = 1)

# Ensure sample order matches between counts and metadata
if (!all(colnames(counts_data) %in% rownames(metadata))) {
    stop("Mismatch: Not all sample names in the counts file columns are present in the metadata file rows.")
}
metadata <- metadata[colnames(counts_data), , drop=FALSE]
cat("   - Data loaded successfully.\n")

# --- 3. Determine Design Formula ---
# If designFormula is null, create a simple one from the contrast factor.
# This adds robustness for the most common use case.
design_formula_str <- config$analysis$designFormula
if (is.null(design_formula_str)) {
  if (!is.null(config$analysis$contrast$factorName)) {
    design_formula_str <- paste0("~ ", config$analysis$contrast$factorName)
    cat(paste("   - Design formula was null, automatically generated:", design_formula_str, "\n"))
  } else {
    stop("Error: 'designFormula' is null and 'contrast' factor is not specified. Cannot proceed.", call. = FALSE)
  }
}

# --- 4. Pre-filter Low Count Genes ---
# Filter only if prefilterMinCount is greater than 0.
if (config$analysis$prefilterMinCount > 0) {
    cat(paste0("Pre-filtering genes with total count < ", config$analysis$prefilterMinCount, "...\n"))
    keep <- rowSums(counts_data) >= config$analysis$prefilterMinCount
    counts_data <- counts_data[keep, ]
    cat(paste("   - Kept", sum(keep), "out of", length(keep), "genes.\n"))
}

# --- 5. Run Core DESeq2 Analysis ---
cat("Creating DESeqDataSet...\n")
dds <- DESeqDataSetFromMatrix(countData = counts_data,
                              colData = metadata,
                              design = as.formula(design_formula_str))

cat("Running DESeq2 analysis...\n")
if (config$analysis$testMethod == "LRT") {
  if (is.null(config$analysis$reducedFormula)) {
        stop("Error: 'reducedFormula' must be provided in config for LRT test.", call. = FALSE)
  }
  cat("   - Using Likelihood Ratio Test (LRT).\n")
  tryCatch({
      dds <- DESeq(dds, test = "LRT", reduced = as.formula(config$analysis$reducedFormula))
    }, error = function(e) {
      if (grepl("all gene-wise dispersion estimates are within", e$message)) {
        cat("DESeq2 warning: Dispersion estimates are too close. Using gene-wise estimates directly.\n")
        dds <<- estimateSizeFactors(dds)
        dds <<- estimateDispersionsGeneEst(dds)
        dispersions(dds) <- mcols(dds)$dispGeneEst
        dds <<- nbinomLRT(dds, reduced = as.formula(config$analysis$reducedFormula))
      } else {
        stop(e)
      }
    })

  # Get results
  cat("   - Getting results for the test LRT (interaction term).\n")
  res <- results(dds)

} else {
  cat("   - Using Wald Test.\n")
  tryCatch({
    dds <- DESeq(dds)
  }, error = function(e) {
    if (grepl("all gene-wise dispersion estimates are within", e$message)) {
      cat("⚠️ DESeq2 warning: Dispersion estimates are too close. Using gene-wise estimates directly.\n")
      dds <<- estimateSizeFactors(dds)
      dds <<- estimateDispersionsGeneEst(dds)
      dispersions(dds) <- mcols(dds)$dispGeneEst
      dds <<- nbinomWaldTest(dds)
    } else {
      stop(e)
    }
  })

  # Get results
  cat("   - Getting results for the test Wald.\n")
  if (is.null(config$analysis$contrast$factorName)) {
    stop("Error: 'contrast' must be provided in config for Wald test.", call. = FALSE)
  }
  contrast_vector <- c(
    config$analysis$contrast$factorName,
    config$analysis$contrast$numeratorLevel,
    config$analysis$contrast$denominatorLevel
  )
  cat(paste0("   - Using contrast: ", paste(contrast_vector, collapse=" vs "), "\n"))
  res <- results(dds, contrast = contrast_vector)
}

# --- 6. Extract and Save Results ---
cat("Saving results...\n")

res_ordered <- res[order(res$pvalue), ]
output_results_file <- paste0(config$output$basename, ".csv")
write.csv(as.data.frame(res_ordered), file = output_results_file)
cat(paste0("   - Main results saved to: ", output_results_file, "\n"))

# --- 7. Generate Additional Outputs based on Boolean Flags ---
# Generate and save PCA plot if requested
if (isTRUE(config$output$pcaPlot)) {
    cat("Generating PCA plot...\n")
    # Use varianceStabilizingTransformation for small datasets
    vsd <- varianceStabilizingTransformation(dds, blind = TRUE)
    # Use the factor from the contrast for coloring the plot
    pca_plot <- plotPCA(vsd, intgroup = config$analysis$contrast$factorName) +
                geom_point(size=4) + theme_bw()

    pca_plot_file <- paste0(config$output$basename, "_pca_plot.png")

    # Use png() and dev.off() to save the plot to a file
    # This is more robust than ggsave on some systems
    png(filename = pca_plot_file, width = 7, height = 6, units = "in", res = 300)
    print(pca_plot)
    dev.off()

    cat(paste0("   - PCA plot saved to: ", pca_plot_file, "\n"))
}

# Generate and save MA plot if requested
if (isTRUE(config$output$maPlot)) {
    cat("Generating MA-plot...\n")
    ma_plot_file <- paste0(config$output$basename, "_ma_plot.png")
    png(ma_plot_file, width = 480, height = 480)
    plotMA(res, main="MA Plot")
    dev.off()
    cat(paste0("   - MA-plot saved to: ", ma_plot_file, "\n"))
}

# Save variance-stabilized transformed counts if requested
if (isTRUE(config$output$transformedCounts)) {
    cat("Saving transformed counts...\n")
    # Avoid re-calculating if already done for PCA plot
    if (!exists("vsd")) {
        vsd <- varianceStabilizingTransformation(dds, blind = TRUE)
    }
    transformed_counts_file <- paste0(config$output$basename, "_transformed_counts.csv")
    write.csv(assay(vsd), file = transformed_counts_file)
    cat(paste0("   - Transformed counts saved to: ", transformed_counts_file, "\n"))
}

cat("Analysis finished successfully!\n")
# --- return value 0 as exit value
quit(status = 0)
