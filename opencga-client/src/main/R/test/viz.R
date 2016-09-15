library(ggplot2)
library(viridis)
file_formats <- table(files$format)
str(file_formats)
barplot(file_formats)
ggplot(ft, aes(x=format, fill=format))+geom_bar()+scale_color_viridis(discrete = T)
library(lubridate)
ft <- as.data.table(studyFiles)
ft[, creationDate:=ymd_hms(creationDate)]
ft[,modificationDate:=ymd_hms(modificationDate)]
names(ft)
vt <- as.data.table(filvars)
smvt <- vt[1:100,]
pop1 <- smvt$annotation.populationFrequencies[[1]]
# create a new coulmn with names of populatioons
smvt[, `:=`(pops=lapply(annotation.populationFrequencies, function(x)paste(x$study, collapse = "|")))]
smvt[, `:=`(popn=lapply(annotation.populationFrequencies, function(x)paste(x$population, collapse = "|")))]
smvt[, `:=`(popu=lapply(annotation.populationFrequencies, function(x)paste(x$study, x$population,sep="_", collapse = "|")))]
smvt[, `:=`(alt_freq=lapply(annotation.populationFrequencies, function(x)paste(x$altAlleleFreq, collapse = "|")))]
library(tidyr)
test <- separate(smvt, col = popu, c(paste0("pop",1:22)), sep = "\\|", extra = "drop", fill = 'warn')
