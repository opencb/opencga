## Luz Garcia Alonso
## 07/2014
##
## This script takes the "all results" output file from Fatigo for GO terms
## and converts the GO based table (each row is a GO) into a node based table (each row is a node)
## according the node attributes table format for CellMaps
##
## Columns in the outfile represent:
##         1. Node id (genes, proteins, etc)
##         2. GO list of overrepresented GOs (separated by ,)
##         3. GO list of underrepresented GOs (separated by ,)
##         4. GO list of overrepresented GOs names (separated by ,)
##         5. GO list of underrepresented GOs names (separated by ,)
##         6. GO list of overrepresented GOs colors (separated by ,)
##         7. GO list of underrepresented GOs colors (separated by ,)
## NOTE: GO colors are not pre-established but associated according ggcolors function
##       The color palette depends ont the number of significant gos



#############################################################################
cat("# Loading libs & functions\n")
library(GO.db)

ggcolors <- function(n=6, h=c(0, 360) +15){
  if ((diff(h)%%360) < 1) h[2] <- h[2] - 360/n
  hcl(h = (seq(h[1], h[2], length = n)), c = 100, l = 65)
}

spl <- function(x, split, ...){
  unlist(strsplit(x, split=split, ...))
}

gos_in_genes <- function(gene, over_1=T, type="id"){
  if (over_1){
    side <- significant_fatiGO_results[ significant_fatiGO_results$odds_ratio_log > 0 , ]
  } else{
    side <- significant_fatiGO_results[ significant_fatiGO_results$odds_ratio_log < 0 , ]
  }
  genesXgos <- paste(side$list1_positive_ids, side$list2_positive_ids, sep=",")
  pos <- unlist(lapply( genesXgos , function(x) any( spl(x, ",") == gene ) ))
  gos <- side$X.term [ pos ]
  if( type == "name" ){
    gos <- unlist(lapply(gos, function(go)  go_names_hash[[ go ]]   ))
  }
  if( type == "color" ){
    gos <- unlist(lapply(gos, function(go)  go_color_hash[[ go ]]   ))
  }
  gos <- paste(gos, collapse=",")
}
#############################################################################







#############################################################################
### MAIN
cat("# Reading file\n")
args <- commandArgs(trailingOnly = TRUE)
infile <- args[1]
threshold <- args[2]
fatiGO_results <- read.delim(infile, stringsAsFactors=F )
fatiGO_results$GO_name <-  unlist(lapply(strsplit(fatiGO_results$X.term, "\\("), function(x) x[1] ))
fatiGO_results$X.term <-  unlist(lapply(strsplit(fatiGO_results$X.term, "\\("), function(x) gsub(")", "", x[2])  ))



cat("# Getting GOs which adj pvalue is below the threshold\n")
significant_fatiGO_results <- fatiGO_results[ fatiGO_results$adj_pvalue <= threshold , ]
significant_fatiGO_results <- significant_fatiGO_results[ order(significant_fatiGO_results$odds_ratio_log) , ]



cat("# Retrieving name and color data for each GO\n")
go_color_hash <- as.list(ggcolors(nrow(significant_fatiGO_results)))
names(go_color_hash) <- significant_fatiGO_results$X.term
go_names_hash <- lapply(significant_fatiGO_results$X.term, function(go)  fatiGO_results$GO_name[ fatiGO_results$X.term == go ] )
names(go_names_hash) <- significant_fatiGO_results$X.term



cat("# Building node attribute file \n")
gene_attr <- data.frame(ids=unique(c(spl(significant_fatiGO_results$list1_positive_ids, ","), 
                                     spl(significant_fatiGO_results$list2_positive_ids, ","))),
                        stringsAsFactors=F)
gene_attr$overrepresented_go_list1 <- unlist(lapply(gene_attr$ids, gos_in_genes, T))
gene_attr$underrepresented_go_list1 <- unlist(lapply(gene_attr$ids, gos_in_genes, F))
gene_attr$overrepresented_goname_list1 <- unlist(lapply(gene_attr$ids, gos_in_genes, T, "name"))
gene_attr$underrepresented_goname_list1 <- unlist(lapply(gene_attr$ids, gos_in_genes, F, "name"))
gene_attr$overrepresented_colgo_list1 <- unlist(lapply(gene_attr$ids, gos_in_genes, T, "color"))
gene_attr$underrepresented_colgo_list1 <- unlist(lapply(gene_attr$ids, gos_in_genes, T, "color"))

cat("# Writing node attribute file \n")
outfile <- gsub(".txt", paste("_", threshold, "_nodes.attr", sep=""), infile)
colnames(gene_attr)[1] <- paste("#", colnames(gene_attr)[1], sep="")
write.table(gene_attr, outfile, quote=F, sep="\t", col.names=T, row.names=F)


