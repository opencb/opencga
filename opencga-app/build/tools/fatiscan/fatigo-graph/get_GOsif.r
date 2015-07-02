
### LIBRARIES
library(GO.db)
library(reshape2)


### Parents
GOsif <- rbind(melt(as.list(GOBPPARENTS)),
                   melt(as.list(GOCCPARENTS)),
                   melt(as.list(GOMFPARENTS)))
names(GOsif) <- c("Parent", "Child")
write.table(GOsif, file="GO.sif", quote=F, sep="\t", col.names=F, row.names=F)


GOs <- lapply(unique(unlist(GOsif$Child)), function(go)  GOTERM[[ go ]]) 
GOnames <- lapply(GOs, Term  ) 
names(GOnames) <- unique(unlist(GOsif$Child))
GO_annot <- melt(GOnames)
write.table(GO_annot, file="~/projects/cellmaps/plugins/fatigo/GO/GO.annot", col.names=F, row.names=F, quote=F, sep="\t")