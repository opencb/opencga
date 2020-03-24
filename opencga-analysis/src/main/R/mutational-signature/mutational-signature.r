#install.packages("nnls")
#install.packages("ggplot2")
#install.packages("jsonlite")

library(nnls)
library(ggplot2)
suppressMessages(library(jsonlite))

# Getting command arguments
args <- commandArgs(trailingOnly = TRUE)

context <- args[1]
signatures <- args[2]
outdir <- args[3]

context
signatures
outdir


# Getting absolute counts for each SNV trinucleotide context
#dataTable <- read.table(args[1], sep = "\t", header = TRUE)
dataTable <- read.table(context, sep = "\t", header = TRUE)
dataTable <- as.vector(dataTable$Count)

# Normalising frequencies to values between 0 and 1 like the signature table is
dataTable <- dataTable / sum(dataTable)

# Getting signature probabilities reference table
#signatureTable <- read.table(args[2], sep = "\t", header = TRUE)
signatureTable <- read.table(signatures, sep = "\t", header = TRUE)
signatureTable <- as.matrix(signatureTable[1:96,4:33])

# Applying non-negative least squares (NNLS)
coefficients <- nnls(signatureTable, dataTable)

#################################
# Calculating confidence values #
#################################

# Getting the original RSS value
RSSOriginal <- coefficients$deviance

# Optimising the RSS score by iteratively removing signatures with coefficients
# less than 0.06 and refitting the model.
# Optimisation is limitted to 10 iterations to avoid large computaional time
# and because model improvement will not be significant after 2-3 iterations
coefficientsCopy <- coefficients  # Copy of the nnls object
signatureTableCopy <- signatureTable  # Copy of COSMIC signature table
counter <- 0  # Iteration counter
toRemove <- which(coefficientsCopy$x < 0.06 & coefficientsCopy$x > 0 )

# Optimisation
while(length(toRemove) > 0 && counter < 10) {
  toRemove <- which(coefficientsCopy$x < 0.06 & coefficientsCopy$x > 0 )
  # Removing
  for (i in 1:length(toRemove)) {
    signatureTableCopy[, toRemove[i]] <- 0
  }
  # Recalculating coefficients
  coefficientsCopy <- nnls(signatureTableCopy, dataTable)
  counter <- counter + 1
  toRemove <- which(coefficientsCopy$x < 0.06 & coefficientsCopy$x > 0 )
}

# Explained Sum of Squares
#ESS <- sum((coefficientsCopy$fitted - apply(dataTable, 1, mean)) ^ 2)

# Residual Sum of Squares
RSS <- sum(coefficientsCopy$residuals^2)

# Total Sum of Squares
#TSS <- ESS + RSS

# Value to be reported: RSS
RSS <- round(RSS, digits = 4)

#####################
# Writing JSON file #
#####################

# Normalising coefficients to values between 0 and 100
coefficients <- coefficients$x / sum(coefficients$x) * 100

# Creating signature names
tags <- paste("Signature", 1:30, sep = " ")

# Writing coefficients and RSS to JSON file
dfCoeff <- as.data.frame(t(data.frame(coefficients)))
colnames(dfCoeff) <- sapply(sub(' ', '', tags), tolower)
rownames(dfCoeff) <- NULL
df <- NULL
df$coefficients <- dfCoeff
df$rss <- RSS
j <- gsub("\\[|\\]", "", toJSON(df))
write(j, paste0(outdir, "/signature_coefficients.json"))

############
# Plotting #
############

# Ordering coefficients by value
orderedIndex <- order(coefficients, decreasing = TRUE)
orderedCoefficients <- coefficients[orderedIndex]
orderedTags <- tags[orderedIndex]

# Filtering out coefficients below 5
coeffs2 <- orderedCoefficients[orderedCoefficients > 5]
tags2 <- orderedTags[orderedCoefficients > 5]

# Setting up boxes y-limits
cumulativeSum <- cumsum(c(0, coeffs2, 100 - sum(coeffs2)))
starts <- cumulativeSum[1:length(cumulativeSum) - 1]
ends <- cumulativeSum[2:length(cumulativeSum)]

# Setting up boxes colours
colours <- c("darkblue", "darkgreen", "gold2", "darkorange", "darkred", "purple4", "blue1", "chartreuse3", "darkgoldenrod1", "chocolate1", "firebrick1", "darkorchid3")

# Setting up ticks and ticks labels
tickPos <- 0:5 * 20
tickNames <- paste0(tickPos, "%")

# Setting up legend positions
numLegends <- 1:(length(coeffs2) + 1)
legendPos <- ((numLegends - (mean(numLegends))) * 7) + 50

# PLOTTING
png(paste0(outdir, "/signature_summary.png"), width = 1028, height = 800, type='cairo')
ggplot() +
  # Boxes
  geom_rect(mapping=aes(xmin = 0, xmax = 1, ymin = starts, ymax = ends - 0.3),
            fill = c(colours[1:length(starts) - 1], "white"),
            color = "black", alpha = 0.7) +
  # Plot width
  xlim(-1, 2) +
  # Y-axis line
  geom_segment(mapping = aes(x = -0.1, xend = -0.1, y = 0, yend = 100)) +
  # Y-axis ticks
  geom_segment(mapping = aes(x = -0.1, xend = -0.15,
                             y = tickPos, yend = tickPos)) +
  # Y-axis labels
  geom_text(mapping = aes(x = -0.3, y = tickPos), label = tickNames) +
  # Y-axis title
  geom_text(mapping = aes(x = -0.5, y = 50), label = "Signature contribution",
            angle = 90) +
  # Legend boxes
  geom_rect(mapping = aes(xmin = 1.2, xmax = 1.25,
                          ymin = (legendPos - 1), ymax = (legendPos + 1)),
            color = "black",
            fill = c(colours[1:length(legendPos) - 1], "white"),
            alpha = 0.7) +
  # Legend text
  geom_text(mapping = aes(x = 1.28, y = legendPos), label = c(tags2, "Other"),
            hjust = 0) +
  # Removing every other ggplot element from the plot
  theme(axis.text.x = element_blank(),
        axis.ticks.y = element_blank(),
        axis.ticks.x = element_blank(),
        axis.title.x = element_blank(),
        axis.title.y = element_blank(),
        axis.text.y = element_blank(),
        panel.background = element_blank(),
        panel.grid.major = element_blank(),
        panel.grid.minor = element_blank(),
        plot.background = element_blank())

garbage <- dev.off()