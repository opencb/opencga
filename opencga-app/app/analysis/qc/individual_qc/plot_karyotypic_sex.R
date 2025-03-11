# Load necessary libraries
library(ggplot2)
library(jsonlite)

# Read command line arguments
args <- commandArgs(trailingOnly = TRUE)

if (length(args) < 4) {
  stop("Usage: Rscript plot_karyotypic_sex.R <x-ratio> <y-ratio> <thresholds_file> <output_file>")
}

# Parse input parameters
x_ratio <- as.numeric(args[1])   # X coordinate of the point
y_ratio <- as.numeric(args[2])   # Y coordinate of the point
thresholds_file <- args[3]       # Path to JSON file
output_file <- args[4]           # Output image file path

# Load the thresholds
thresholds <- fromJSON(thresholds_file)

# Convert JSON structure to a data frame
categories <- c("xx", "xy", "xo_clearcut", "xyy", "xxy", "xxx", "xxxy", "xyyy")
data_list <- lapply(categories, function(cat) {
  data.frame(
    category = cat,
    xmin = thresholds[[paste0(cat, ".xmin")]],
    xmax = thresholds[[paste0(cat, ".xmax")]],
    ymin = thresholds[[paste0(cat, ".ymin")]],
    ymax = thresholds[[paste0(cat, ".ymax")]]
  )
})
thresholds_df <- do.call(rbind, data_list)

# Add label positions (center of each rectangle)
thresholds_df$label_x <- (thresholds_df$xmin + thresholds_df$xmax) / 2
thresholds_df$label_y <- (thresholds_df$ymin + thresholds_df$ymax) / 2

# Create the plot
plot <- ggplot() +
  geom_rect(data = thresholds_df,
            aes(xmin = xmin, xmax = xmax, ymin = ymin, ymax = ymax, fill = category),
            alpha = 0.3, color = "black") +
  # Add category labels inside the regions
  geom_text(data = thresholds_df, aes(x = label_x, y = label_y, label = category),
            color = "black", fontface = "bold", size = 4, hjust = 0.5, vjust = 0.5) +
  # Add the target point
  geom_point(aes(x = x_ratio, y = y_ratio), color = "red", size = 4) +
  labs(title = "Karyotypic Sex Thresholds",
       x = "X Ratio",
       y = "Y Ratio") +
  theme_minimal() +
  theme(legend.position = "right")

# Save the plot to a file (PNG format)
ggsave(output_file, plot, width = 8, height = 6, dpi = 300)

print(paste("Plot saved as:", output_file))