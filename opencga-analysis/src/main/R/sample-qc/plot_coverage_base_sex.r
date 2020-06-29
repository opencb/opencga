library(ggplot2)
library(rjson)
library(dplyr)
library("RColorBrewer")

# New 100k thresholds
thresholds <- fromJSON(file = "201906_thresholds_grch38_v4.json")
dat <- read.delim("201908019_coverage_based_sex_thresholds.txt",
                  na.strings="None", header=FALSE, stringsAsFactors=FALSE)
colnames(dat) <- c("study", "sample", "deliveryId", "infKaryotype", "avg.X.auto", "avg.Y.auto")


# Selected samples (file)
# selected_samples <- scan("~/gel/cajondesastre/coverage_based_sex/robust_cbsc_samples.txt", what = "character")
# Selected samples (manual entry)
selected_samples <- c("LP3001659-DNA_E03")

# identify test samples
mycolours = c("")
dat$selected <- "notSelected"
dat[dat$sample %in% selected_samples, "selected"] <- "selected"

# Plot
dat$cbskaryotype <- ""
fil_xx <- which(dat$med.X.auto >= thresholds$xx.xmin & dat$med.X.auto <= thresholds$xx.xmax & dat$med.Y.auto >= thresholds$xx.ymin & dat$med.Y.auto <= thresholds$xx.ymax)
fil_xy <- which(dat$med.X.auto >= thresholds$xy.xmin & dat$med.X.auto <= thresholds$xy.xmax & dat$med.Y.auto >= thresholds$xy.ymin & dat$med.Y.auto <= thresholds$xy.ymax)
fil_xo <- which(dat$med.X.auto >= thresholds$xo_clearcut.xmin & dat$med.X.auto <= thresholds$xo_clearcut.xmax & dat$med.Y.auto >= thresholds$xo_clearcut.ymin & dat$med.Y.auto <= thresholds$xo_clearcut.ymax)
fil_xxy <- which(dat$med.X.auto >= thresholds$xxy.xmin & dat$med.X.auto <= thresholds$xxy.xmax & dat$med.Y.auto >= thresholds$xxy.ymin & dat$med.Y.auto <= thresholds$xxy.ymax)
fil_xxx <- which(dat$med.X.auto >= thresholds$xxx.xmin & dat$med.X.auto <= thresholds$xxx.xmax & dat$med.Y.auto >= thresholds$xxx.ymin & dat$med.Y.auto <= thresholds$xxx.ymax)
fil_xyy <- which(dat$med.X.auto >= thresholds$xyy.xmin & dat$med.X.auto <= thresholds$xyy.xmax & dat$med.Y.auto >= thresholds$xyy.ymin & dat$med.Y.auto <= thresholds$xyy.ymax)
fil_xxxy <- which(dat$med.X.auto >= thresholds$xxxy.xmin & dat$med.X.auto <= thresholds$xxxy.xmax & dat$med.Y.auto >= thresholds$xxxy.ymin & dat$med.Y.auto <= thresholds$xxxy.ymax)
fil_xyyy <- which(dat$med.X.auto >= thresholds$xyyy.xmin & dat$med.X.auto <= thresholds$xyyy.xmax & dat$med.Y.auto >= thresholds$xyyy.ymin & dat$med.Y.auto <= thresholds$xyyy.ymax)

dat[fil_xx, "cbskaryotype"] <- "XX"
dat[fil_xy, "cbskaryotype"] <- "XY"
dat[fil_xo, "cbskaryotype"] <- "XO"
dat[fil_xxx, "cbskaryotype"] <- "XXX"
dat[fil_xxy, "cbskaryotype"] <- "XXY"
dat[fil_xyy, "cbskaryotype"] <- "XYY"
dat[fil_xxxy, "cbskaryotype"] <- "XXXY"
dat[fil_xyyy, "cbskaryotype"] <- "XYYY"

p <- ggplot(data=dat, mapping = aes(x = avg.X.auto, y=avg.Y.auto, colour=selected)) +
    geom_point(alpha=0.7) +
    xlab("Median X / median autosomal coverage") +
    ylab("Median Y / median autosomal coverage") +
    scale_color_manual(values = c("#999999", "#FF0000")) +
    ggtitle(label = "Dragen CBS using median coverage per chromosome") +
    theme_bw(16)

p <- p +
    geom_rect(mapping = aes(xmin = thresholds$xy.xmin, xmax = thresholds$xy.xmax, ymin = thresholds$xy.ymin, ymax = thresholds$xy.ymax), alpha = 0, size = 0.2, color="#107aa7") +
    geom_rect(mapping = aes(xmin = thresholds$xx.xmin, xmax = thresholds$xx.xmax, ymin = thresholds$xx.ymin, ymax = thresholds$xx.ymax), alpha = 0, size = 0.2, color="#107aa7") +
    geom_rect(mapping = aes(xmin = thresholds$xo_clearcut.xmin, xmax = thresholds$xo_clearcut.xmax, ymin = thresholds$xo_clearcut.ymin, ymax = thresholds$xo_clearcut.ymax), alpha = 0, size = 0.2, color="#107aa7") +
    geom_rect(mapping = aes(xmin = thresholds$xyy.xmin, xmax = thresholds$xyy.xmax, ymin = thresholds$xyy.ymin, ymax = thresholds$xyy.ymax), alpha = 0, size = 0.2, color="#107aa7") +
    geom_rect(mapping = aes(xmin = thresholds$xxy.xmin, xmax = thresholds$xxy.xmax, ymin = thresholds$xxy.ymin, ymax = thresholds$xxy.ymax), alpha = 0, size = 0.2, color="#107aa7") +
    geom_rect(mapping = aes(xmin = thresholds$xxx.xmin, xmax = thresholds$xxx.xmax, ymin = thresholds$xxx.ymin, ymax = thresholds$xxx.ymax), alpha = 0, size = 0.2, color="#107aa7") +
    geom_rect(mapping = aes(xmin = thresholds$xxxy.xmin, xmax = thresholds$xxxy.xmax, ymin = thresholds$xxxy.ymin, ymax = thresholds$xxxy.ymax), alpha = 0, size = 0.2, color="#107aa7") +
    geom_rect(mapping = aes(xmin = thresholds$xyyy.xmin, xmax = thresholds$xyyy.xmax, ymin = thresholds$xyyy.ymin, ymax = thresholds$xyyy.ymax), alpha = 0, size = 0.2, color="#107aa7")
p