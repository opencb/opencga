#######################################################################################
#######################################################################################
## Luz Garcia Alonso
## 08/2014
##
## Build a GO subgraph from FatiGO results
## Input
## 1) results file
## 2) threshold
## Output
## 1) gosubgraph.sif (dad, child relationships)
## 2) gosubgraph.attr (GO id, GO name, GO color according the pvalue)
#######################################################################################
#######################################################################################

args <- commandArgs(trailingOnly = F)
scriptPath <- dirname(sub("--file=","",args[grep("--file",args)]))


cat("# Loading libs & functions\n")
library(GO.db)
library(reshape2)
library(RColorBrewer)

pvals  <- c(1, 0.1, 0.05, 0.01, 0.005, 0.001, 0.0005)
colorize <- function(go){
  col <- "white"
  x <- as.numeric(fatiGO_results[ fatiGO_results$X.term == go , c(12, 14) ])
  if ( ! any(is.na(x)) ) {
    side <- x[1]
    pval <- x[2]
    if( side > 0 ){
      col <- brewer.pal("Reds", n=9)[ tail( which(pvals >= pval), 1)  ]
    } else{
      col <- brewer.pal("Blues", n=9)[ tail( which(pvals >= pval), 1)  ]
    }
  } 
  return(col)
}



cat("# Reading imput params\n")
args <- commandArgs(trailingOnly = TRUE)
infile <- args[1]
threshold <- args[2]

cat("# Reading file\n")
fatiGO_results <- read.delim(infile, stringsAsFactors=F )
fatiGO_results$GO_name <-  unlist(lapply(strsplit(fatiGO_results$X.term, "\\("), function(x) x[1] ))
fatiGO_results$X.term <-  unlist(lapply(strsplit(fatiGO_results$X.term, "\\("), function(x) gsub(")", "", x[2])  ))


cat("# Loading whole GO graph\n")
GO_sif <- read.delim(paste(scriptPath, "/GO.sif", sep=""), stringsAsFactors=F, header=F)


cat("# Getting GOs which adj pvalue is below the threshold\n")
significant_go <- fatiGO_results$X.term[ fatiGO_results$adj_pvalue <= threshold ]


cat("# Finding which GO domine\n")
if( any(significant_go %in% names(as.list(GOCCANCESTOR))) ){
  goancestor <- as.list(GOCCANCESTOR)
} else if( any(significant_go %in% names(as.list(GOMFANCESTOR))) ){
  goancestor <- as.list(GOMFANCESTOR)
}else{
  goancestor <- as.list(GOBPANCESTOR)
} 
cat("# Extracting GO for the subgraph\n")
allgos <- unique(c(unlist(goancestor[significant_go]), significant_go))



GOsubgraph_sif <- c()
GOsubgraph_attr <- as.data.frame(setNames(replicate(5,numeric(0), simplify = F), c("GO","name","color","adj_pval","odds_ratio_log")))

if( length(allgos) != 0 ){ 
    cat("# Extracting GOsubgraph\n")
    GOsubgraph_sif <- GO_sif[ intersect(which(GO_sif$V1 %in% allgos), which(GO_sif$V2 %in% allgos) ) , ]
    if ( nrow(GOsubgraph_sif) != 0 ) {
      cat("# Building attributes\n")
      GOsubgraph_attr <- data.frame(GO=unique(unlist(GOsubgraph_sif)), stringsAsFactors=F)
      GOsubgraph_attr$name <- unlist(lapply(GOsubgraph_attr$GO, function(go)  Term(GOTERM[[go]])  ))
      GOsubgraph_attr <- GOsubgraph_attr[ GOsubgraph_attr$GO != "all" ,  ]
      GOsubgraph_attr$color <- unlist(lapply(GOsubgraph_attr$GO, colorize ))
      GOsubgraph_attr$adj_pval <- fatiGO_results$adj_pvalue[ match(GOsubgraph_attr$GO, fatiGO_results$X.term) ]
      GOsubgraph_attr$adj_pval[ is.na(GOsubgraph_attr$adj_pval) ]  <- 1
      GOsubgraph_attr$odds_ratio_log <- fatiGO_results$odds_ratio_log[ match(GOsubgraph_attr$GO, fatiGO_results$X.term) ]
      GOsubgraph_attr$odds_ratio_log[ is.na(GOsubgraph_attr$odds_ratio_log) ]  <- 0
    }else{
      cat("WARNING: The significant terms are all obsolete and no ancestrir is found\n")
    }
}


cat("# Writing outfiles\n")
outfile <- gsub(".txt", paste("_", threshold, "_GOsubgraph.sif", sep=""), infile)
write.table(GOsubgraph_sif, outfile, quote=F, sep="\t", col.names=F, row.names=F)
outfile <- gsub(".txt", paste("_", threshold, "_GOsubgraph.attr", sep=""), infile)
colnames(GOsubgraph_attr)[1] <- paste("#", colnames(GOsubgraph_attr)[1], sep="")
write.table(GOsubgraph_attr, outfile, quote=F, sep="\t", col.names=T, row.names=F)




