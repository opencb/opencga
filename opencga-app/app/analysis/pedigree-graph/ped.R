suppressMessages(library(optparse))
suppressMessages(library(jsonlite))
suppressMessages(library(kinship2))
set.seed(42)

# Load file data
load_file_data <- function(ped_fpath) {
  # Load all pedigree
  family_ped <- read.table(ped_fpath, header=TRUE,
                           colClasses = c(id="character",
                                          dadid="character",
                                          momid="character",
                                          relation="character"))
  
  # Extract affected info
  affected <- as.data.frame(family_ped[, grepl("affected", names(family_ped))])
  if (ncol(affected) == 1) {
    affected <- family_ped$affected
  } else {
    colnames(affected) <- unlist(lapply(colnames(affected), function (x) sub('affected.', '', x)))
    affected <- as.matrix(affected)
  }

  # Extract relation info
  columns <- c("id1", "id2", "code")
  rel_df <- data.frame(matrix(nrow = 0, ncol = length(columns)))  # Create empty relationship data frame
  colnames(rel_df) = columns
  relation <- family_ped[!is.na(family_ped$relation), c("id", "relation")]  # Remove NA and subset
  if(nrow(relation) > 0) {
    group_and_rel <- strsplit(as.character(relation$relation),',')  # Split group and relationship code
    relation <- data.frame(relation$id, do.call(rbind, group_and_rel))  # Combine id, group and relationship code
    colnames(relation) <- c("id", "group", "code")
    agg <- aggregate(id~group, data = relation, paste0, collapse=",")  # Concatenate ids by group
    for (row in 1:nrow(agg)) {  # Get pair combinations of all ids in the same group
      ids <- strsplit(as.character(agg$id),',')
      ids_comb_matrix <- t(do.call(cbind, lapply(ids, function(x) combn(x,2))))
    }
    codes <- do.call(rbind, lapply(ids_comb_matrix[,1], function(x) relation[relation$id==x, ]$code))  # Get code for each pair of ids
    rel_matrix = cbind(ids_comb_matrix, codes)  # Merge pair of ids and their corresponding code
    colnames(rel_matrix) <- c("id1", "id2", "code")
    rel_df <- as.data.frame(rel_matrix)  # Convert matrix to data frame
    rel_df$code <- as.numeric(rel_df$code)  # Relationship code has to be numeric
  }

  ped_info <- list("family_ped" = family_ped, "affected" = affected, "relation" = rel_df)
  return(ped_info)
}

# Main function
plot_pedigree <- function(ped_fpath, out_dir, plot_format, coords_format, no_legend, legend_pos) {
  # Gather pedigree info
  ped_info <- load_file_data(ped_fpath)
  family_ped <- ped_info$family_ped
  affected <- ped_info$affected
  relation <- ped_info$relation
  
  # Create pedigree
  ped_all <- pedigree(id=family_ped$id,
                      dadid=family_ped$dadid,
                      momid=family_ped$momid,
                      sex=family_ped$sex,
                      affected=affected,
                      status = family_ped$status,
                      relation = relation)
  
  # Print pedigree plot
  if (legend_pos %in% c("topright", "bottomright")) {
    margins <- c(5.1, 4.1, 4.1, 10.1)  # bottom, left, top, right
  } else {
    margins <- c(5.1, 12.1, 4.1, 2.1)
  }
  plot_formats <- unlist(strsplit(plot_format, ','))
  if ("png" %in% plot_formats) {
    png(file=paste(out_dir, "pedigree.png", sep='/'))
    if (no_legend | ncol(as.data.frame(affected)) == 1) {
      plot_df <- plot(ped_all)
    } else{
      plot_df <- plot(ped_all,
                      mar=margins,
                      density = seq(-1, 90, 90/ncol(affected)),
                      angle = rev(seq(-1, 90, 90/ncol(affected))))
      pedigree.legend(ped_all, location=legend_pos, radius=plot_df$boxh)
    }
    garbage <-dev.off()
  }
  if ("svg" %in% plot_formats) {
    svg(file=paste(out_dir, "pedigree.svg", sep='/'))
    if (no_legend | ncol(as.data.frame(affected)) == 1) {
      plot_df <- plot(ped_all)
    } else{
      plot_df <- plot(ped_all,
                      mar=margins,
                      density = seq(-1, 90, 90/ncol(affected)),
                      angle = rev(seq(-1, 90, 90/ncol(affected))))
      pedigree.legend(ped_all, location=legend_pos, radius=plot_df$boxh)
    }
    garbage <-dev.off()
  }
  
  # Adding coordinates
  ped_coords <- cbind(family_ped, round(plot_df$x, 2), round(plot_df$y, 2))
  colnames(ped_coords) <- c(colnames(family_ped), c("x", "y"))
  
  # Adding spouses
  spouses <- as.vector(t(plot_df$plist$spouse))  # Get spouses
  ind_order <- as.vector(t(plot_df$plist$nid))  # Get ind order in plot
  spouses <- spouses[ind_order!=0]  # Remove empty positions
  ind_order <- ind_order[ind_order!=0]  # Remove empty positions
  ped_coords$spouse <- NA  # Add new spouse column to family ped
  for (i in 1:length(spouses)) {
    if (spouses[i] == 1) {  # If ind is linked to next ind
      ped_coords[ind_order[i],]$spouse <- c(ped_coords[ind_order[i+1],]$id)
    }
    if (i!=1 && spouses[i] == 1 && spouses[i-1] == 1) {  # If ind is linked to previous and next ind
      ped_coords[ind_order[i],]$spouse <- paste(c(ped_coords[ind_order[i-1],]$id, ped_coords[ind_order[i],]$spouse), collapse=',')
    }
    if (i!=1 && spouses[i] == 0 && spouses[i-1] == 1) {  # If ind is linked to previous ind 
      ped_coords[ind_order[i],]$spouse <- c(ped_coords[ind_order[i-1],]$id)
    }
  }
  
  # Output file with coordinates
  coords_formats <- unlist(strsplit(coords_format, ','))
  if ("tsv" %in% coords_formats) {
    write.table(ped_coords, file=paste(out_dir, "ped_coords.tsv", sep='/'), sep = '\t', quote=FALSE, row.names=FALSE)
  }
  if ("json" %in% coords_formats) {
    write_json(ped_coords, path=paste(out_dir, "ped_coords.json", sep='/'), na='null')
  }
}


# Command line interface
option_list <- list(
  make_option(c("--plot_format"), type="character", default="svg",
              help="Plot format, options: [\"svg\", \"png\"]. Default: \"svg\""),
  make_option(c("--coords_format"), type="character", default="json",
              help="Coords file format, options: [\"json\", \"tsv\"]. Default: \"json\""),
  make_option(c("--no_legend"), type="logical", default=FALSE, action = "store_true",
              help="Removes plot legend"),
  make_option(c("--legend_pos"), type="character", default="topright",
              help="Legend position, options: [\"bottomright\", \"bottomleft\", \"topleft\", \"topright\"]. Default: \"topright\"")
)
parser <- OptionParser(usage = "%prog ped_fpath out_dir [options]", option_list=option_list)
arguments <- parse_args(parser, positional_arguments = 2)
opt <- arguments$options
args <- arguments$args

# Run main function
plot_pedigree(args[1],
              args[2],
              plot_format=opt$plot_format,
              coords_format=opt$coords_format,
              no_legend=opt$no_legend,
              legend_pos=opt$legend_pos)

