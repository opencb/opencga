library(ggplot2)
library(dplyr)
library(jsonlite)

# Read command line arguments
args <- commandArgs(trailingOnly = TRUE)

if (length(args) < 2) {
  stop("Usage: Rscript plot_mendelian_errors.R <input JSON file> <output image file>")
}

# Parse input parameters
json_file <- args[1]   # JSON file
output_file <- args[2] # Output image file path

# Read the JSON file
data <- fromJSON(json_file, simplifyVector = FALSE)

# Extract chromosome data and create a data frame
chrom_data <- data$chromAggregation %>%
  lapply(function(x) {
    chrom <- x$chromosome[[1]]
    num_errors <- x$numErrors[[1]]
    ratio <- x$ratio[[1]]

    # Extract error codes from errorCodeAggregation
    error_codes_df <- stack(x$errorCodeAggregation)
    names(error_codes_df) <- c("count", "error_code")

    error_codes_df$chromosome <- chrom
    error_codes_df$num_errors <- num_errors
    error_codes_df$ratio <- ratio
    return(error_codes_df)
  }) %>%
  bind_rows()

# Extract unique error codes
unique_error_codes <- unique(chrom_data$error_code)

# Handle NA and numeric codes separately
numeric_codes <- as.numeric(unique_error_codes[!is.na(as.numeric(unique_error_codes))])
sorted_numeric_codes <- sort(numeric_codes)

# Explicitly add "NA" and "7" to the levels
all_possible_codes <- c(sorted_numeric_codes, "NA", 7) # Add 7 as a number, and 'NA' as string.
final_levels <- as.character(sort(all_possible_codes)) # Convert to string, and sort.

# Convert error_code to factor with correct levels
chrom_data$error_code <- factor(chrom_data$error_code, levels = final_levels)

# Create the plot
plot <- ggplot(chrom_data, aes(x = chromosome, y = count, fill = error_code)) +
  geom_bar(stat = "identity", position = "stack") +
  labs(title = "Number of errors by chromosome and error code",
       x = "Chromosome",
       y = "Number of errors",
       fill = "Error code") +
  theme_minimal() +
  theme(axis.text.x = element_text(angle = 45, hjust = 1)) # Rotate x-axis labels

# Save the plot as a PNG file
ggsave(output_file, plot, width = 10, height = 6) # Adjust width and height as needed

print(output_file) # Confirmation message