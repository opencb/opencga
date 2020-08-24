#!/usr/bin/env Rscript

library(RCircos);
library(scales)
library(optparse)


my.RCircos.Tile.Plot <- function (tile.data, track.num, side, tile.colors=NA)
{
  RCircos.Pos <- RCircos.Get.Plot.Positions()
  RCircos.Par <- RCircos.Get.Plot.Parameters()
  tile.data <- RCircos.Get.Plot.Data.nosort(tile.data, "plot")
  the.layer <- 1
  the.chr <- tile.data[1, 1]
  start <- tile.data[1, 2]
  end <- tile.data[1, 3]
  tile.layers <- rep(1, nrow(tile.data))
  if (nrow(tile.data)>1) {
    for (a.row in 2:nrow(tile.data)) {
      if (tile.data[a.row, 2] >= end) {
        the.layer <- 1
        start <- tile.data[a.row, 2]
        end <- tile.data[a.row, 3]
      }
      else if (tile.data[a.row, 1] != the.chr) {
        the.layer <- 1
        the.chr <- tile.data[a.row, 1]
        start <- tile.data[a.row, 2]
        end <- tile.data[a.row, 3]
      }
      else {
        the.layer <- the.layer + 1
        if (tile.data[a.row, 3] > end) {
          end <- tile.data[a.row, 3]
        }
      }
      tile.layers[a.row] <- 1
    }
  }
  locations <- RCircos.Track.Positions.my(side, track.num)
  out.pos <- locations[1]
  in.pos <- locations[2]
  layer.height <- RCircos.Par$track.height/RCircos.Par$max.layers
  num.layers <- max(tile.layers)
  if (num.layers > RCircos.Par$max.layers) {
    if (side == "in") {
      in.pos <- out.pos - layer.height * num.layers
    }
    else {
      out.pos <- in.pos + layer.height * num.layers
    }
    cat(paste("Tiles plot will use more than one track.",
              "Please select correct area for next track.\n"))
  }
  if (num.layers < RCircos.Par$max.layers) {
    layer.height <- RCircos.Par$track.height/num.layers
  }
  if (length(tile.colors)==1) {
    tile.colors <- RCircos.Get.Plot.Colors(tile.data, RCircos.Par$tile.color)}
  RCircos.Track.Outline.my(out.pos, in.pos, num.layers)
  the.loc <- ncol(tile.data)
  for (a.row in 1:nrow(tile.data)) {
    tile.len <- tile.data[a.row, 3] - tile.data[a.row, 2]
    tile.range <- round(tile.len/RCircos.Par$base.per.unit/2,
                        digits = 0)
    start <- tile.data[a.row, the.loc] - tile.range
    end <- tile.data[a.row, the.loc] + tile.range
    layer.bot <- in.pos
    layer.top <- out.pos

    #Catch positions that fall outside a band (eg when using exome ideogram)
    if (is.na(start) || (is.na(end))) {
      next;
    }
    polygon.x <- c(RCircos.Pos[start:end, 1] * layer.top,
                   RCircos.Pos[end:start, 1] * layer.bot)
    polygon.y <- c(RCircos.Pos[start:end, 2] * layer.top,
                   RCircos.Pos[end:start, 2] * layer.bot)
    polygon(polygon.x, polygon.y, col = tile.colors[a.row], lwd=RCircos.Par$line.width, border=tile.colors[a.row])
  }
}


RCircos.Chromosome.Ideogram.Plot.my <- function (chrTextColor = 'grey', gridLineColor = 'grey', textSize = 0.6)
{
  RCircos.Cyto <- RCircos.Get.Plot.Ideogram()
  RCircos.Pos <- RCircos.Get.Plot.Positions()
  RCircos.Par <- RCircos.Get.Plot.Parameters()
  right.side <- nrow(RCircos.Pos)/2
  if (is.null(RCircos.Par$chr.ideog.pos)){
    RCircos.Par$chr.ideog.pos <- 1.95 #3.1 #1.95
  }
  outer.location <- RCircos.Par$chr.ideog.pos + RCircos.Par$chrom.width
  inner.location <- RCircos.Par$chr.ideog.pos
  chroms <- unique(RCircos.Cyto$Chromosome)
  for (a.chr in 1:length(chroms)) {
    the.chr <- RCircos.Cyto[RCircos.Cyto$Chromosome == chroms[a.chr],
                            ]
    ##new RCircos version
    start <- the.chr$StartPoint[1]
    end <- the.chr$EndPoint[nrow(the.chr)]
    mid <- round((end - start + 1)/2, digits = 0) + start
    #        chr.color <- 'grey'
    chr.color <- chrTextColor
    pos.x <- c(RCircos.Pos[start:end, 1] * outer.location,
               RCircos.Pos[end:start, 1] * inner.location)
    pos.y <- c(RCircos.Pos[start:end, 2] * outer.location,
               RCircos.Pos[end:start, 2] * inner.location)
    #        polygon(pos.x, pos.y, border='grey', lwd=0.5)
    polygon(pos.x, pos.y, border=gridLineColor, lwd=0.5)
    chr.name <- sub(pattern = "chr", replacement = "", chroms[a.chr])
    text(RCircos.Pos[mid, 1] * RCircos.Par$chr.name.pos,
         RCircos.Pos[mid, 2] * RCircos.Par$chr.name.pos, label = chr.name,
         #            srt = RCircos.Pos$degree[mid], col='grey', cex=0.6)
         srt = RCircos.Pos$degree[mid], col=gridLineColor, cex=textSize)
    lines(RCircos.Pos[start:end, ] * RCircos.Par$highlight.pos,
          col = chr.color, lwd = 0.5)
  }
  for (a.band in 1:nrow(RCircos.Cyto)) {
    a.color <- RCircos.Cyto$BandColor[a.band]
    if (a.color == "white") {
      next
    }
    ##new RCircos version
    start <- RCircos.Cyto$StartPoint[a.band]
    end <- RCircos.Cyto$EndPoint[a.band]
    pos.x <- c(RCircos.Pos[start:end, 1] * outer.location,
               RCircos.Pos[end:start, 1] * inner.location)
    pos.y <- c(RCircos.Pos[start:end, 2] * outer.location,
               RCircos.Pos[end:start, 2] * inner.location)
    polygon(pos.x, pos.y, col = alpha(a.color,0.25), border = NA)
  }
}


RCircos.Get.Plot.Data.nosort <- function (genomic.data, plot.type, validate=TRUE) 
{

  data.points <- rep(0, nrow(genomic.data))
  for (a.row in 1:nrow(genomic.data)) {
    chromosome <- as.character(genomic.data[a.row, 1])
    location <- round((genomic.data[a.row, 2] + genomic.data[a.row, 
                                                             3])/2, digits = 0)
    data.points[a.row] <- RCircos.Data.Point(chromosome, location)
  }
  genomic.data["Location"] <- data.points
  return(genomic.data)
}


RCircos.Heatmap.Plot.my <- function (heatmap.data, data.col, track.num, side, plotTrack=TRUE, heatmap.ranges=NA, heatmap.color=NA)
{
  RCircos.Cyto <- RCircos.Get.Plot.Ideogram()
  RCircos.Pos <- RCircos.Get.Plot.Positions()
  RCircos.Par <- RCircos.Get.Plot.Parameters()
  
  min.with <- 1000000
  heatmap.data$width <- heatmap.data$chromEnd - heatmap.data$chromStart
  heatmap.data <- heatmap.data[order(-heatmap.data$width),]  # make sure the narrowest plots are drawn as last
  narrow.cn <-  heatmap.data$width < min.with
  flank <- (min.with - heatmap.data$width[narrow.cn])/2
  heatmap.data$chromEnd[narrow.cn] <- heatmap.data$chromEnd[narrow.cn ] + flank
  heatmap.data$chromStart[narrow.cn ] <- heatmap.data$chromStart[narrow.cn ] - flank
  heatmap.data$chromStart[heatmap.data$chromStart<0] <- 0
  
  heatmap.data <- RCircos.Get.Plot.Data.nosort(heatmap.data, "plot")
  heatmap.data1 <- RCircos.Get.Plot.Data.nosort(data.frame(Chromosome=heatmap.data$Chromosome, chromStart=heatmap.data$chromStart, chromEnd=heatmap.data$chromStart), "plot")
  heatmap.data2 <- RCircos.Get.Plot.Data.nosort(data.frame(Chromosome=heatmap.data$Chromosome, chromStart=heatmap.data$chromEnd, chromEnd=heatmap.data$chromEnd), "plot")
  
  
  if ((length(heatmap.ranges)==1) && (is.na(heatmap.ranges))) {
    ColorLevel <- RCircos.Par$heatmap.ranges
  } else {
    ColorLevel <- heatmap.ranges
  }
  
  if ((length(heatmap.color)==1) && (is.na(heatmap.color))) {
    ColorRamp <- RCircos.Get.Heatmap.ColorScales(RCircos.Par$heatmap.color)
  } 
  
  columns <- 5:(ncol(heatmap.data) - 1)
  min.value <- min(as.matrix(heatmap.data[, columns]))
  max.value <- max(as.matrix(heatmap.data[, columns]))
  
  heatmap.locations1 <- as.numeric(heatmap.data1[, ncol(heatmap.data2)])
  heatmap.locations2 <- as.numeric(heatmap.data2[, ncol(heatmap.data2)])
  
  start <- heatmap.locations1 # -  RCircos.Par$heatmap.width/2
  end <- heatmap.locations2 # + RCircos.Par$heatmap.width/2
  data.chroms <- as.character(heatmap.data[, 1])
  chromosomes <- unique(data.chroms)
  cyto.chroms <- as.character(RCircos.Cyto$Chromosome)
  
  for (a.chr in 1:length(chromosomes)) {
    cyto.rows <- which(cyto.chroms == chromosomes[a.chr])
    locations <- as.numeric(RCircos.Cyto$EndPoint[cyto.rows]) # chromosome locations
    chr.start <- min(locations) - RCircos.Cyto$StartPoint[cyto.rows[1]] # chromosome start
    chr.end <- max(locations) # chromosome end
    data.rows <- which(data.chroms == chromosomes[a.chr]) # points on this chromosome
    start[data.rows[start[data.rows] < chr.start]] <- chr.start # chromosome starts for each point
    end[data.rows[end[data.rows] > chr.end]] <- chr.end # chromosome end for each point
  }
  
  locations <- RCircos.Track.Positions.my(side, track.num)  # positions
  out.pos <- locations[1]
  in.pos <- locations[2]
  chroms <- unique(RCircos.Cyto$Chromosome)
  for (a.chr in 1:length(chroms)) {
    the.chr <- RCircos.Cyto[RCircos.Cyto$Chromosome == chroms[a.chr],
                            ]
    the.start <- the.chr$StartPoint[1]
    the.end <- the.chr$EndPoint[nrow(the.chr)]
    polygon.x <- c(RCircos.Pos[the.start:the.end, 1] * out.pos,
                   RCircos.Pos[the.end:the.start, 1] * in.pos)
    polygon.y <- c(RCircos.Pos[the.start:the.end, 2] * out.pos,
                   RCircos.Pos[the.end:the.start, 2] * in.pos)
    polygon(polygon.x, polygon.y, col = "white",  border = RCircos.Par$grid.line.color, lwd=0.3)
  }
  
  
  heatmap.value <- as.numeric(heatmap.data[, data.col])
  for (a.point in 1:length(heatmap.value)) {
    
    the.level <- which(ColorLevel <= heatmap.value[a.point])
    cell.color <- heatmap.color[max(the.level)] # establish the color
    
    the.start <- start[a.point]
    the.end <- end[a.point]
    #if (is.na(the.start) |  is.na(the.end)) {
    #    browser()
    #}
    
    #Catch positions that fall outside a band (eg when using exome ideogram)
    if (is.na(the.start) || (is.na(the.end))) {
      next;
    }
    polygon.x <- c(RCircos.Pos[the.start:the.end, 1] * out.pos, RCircos.Pos[the.end:the.start, 1] * in.pos)
    polygon.y <- c(RCircos.Pos[the.start:the.end, 2] * out.pos, RCircos.Pos[the.end:the.start, 2] * in.pos)
    polygon(polygon.x, polygon.y, col = cell.color, border = NA)
  }
  
}


RCircos.Link.Plot.my <- function (link.data, track.num, by.chromosome = FALSE, link.colors=NA)
{
  
  if (length(link.colors)==1) {
    link.colors <- rep('BurlyWood', nrow(link.data))
  }
  
  
  RCircos.Pos <- RCircos.Get.Plot.Positions()
  RCircos.Par <- RCircos.Get.Plot.Parameters()
  locations <- RCircos.Track.Positions.my('in', track.num)
  start <- locations[['out.loc']]
  base.positions <- RCircos.Pos * start
  data.points <- matrix(rep(0, nrow(link.data) * 2), ncol = 2)
  for (a.link in 1:nrow(link.data)) {
    data.points[a.link, 1] <- RCircos.Data.Point(link.data[a.link,
                                                           1], link.data[a.link, 2])
    data.points[a.link, 2] <- RCircos.Data.Point(link.data[a.link,
                                                           4], link.data[a.link, 5])
    if (data.points[a.link, 1] == 0 || data.points[a.link,
                                                   2] == 0) {
      print("Error in chromosome locations ...")
      break
    }
  }
  for (a.link in 1:nrow(data.points)) {
    point.one <- data.points[a.link, 1]
    point.two <- data.points[a.link, 2]
    if (point.one > point.two) {
      point.one <- data.points[a.link, 2]
      point.two <- data.points[a.link, 1]
    }
    P0 <- as.numeric(base.positions[point.one, ])
    P2 <- as.numeric(base.positions[point.two, ])
    links <- RCircos.Link.Line(P0, P2)
    lines(links$pos.x, links$pos.y, type = "l", col = link.colors[a.link], lwd=RCircos.Par$link.line.width)
  }
}


RCircos.Scatter.Plot.color <- function (scatter.data, data.col, track.num, side, scatter.colors, draw.bg =TRUE, no.sort=FALSE) 
{

  RCircos.Pos <- RCircos.Get.Plot.Positions()
  pch <- RCircos.Get.Plot.Parameters()$point.type
  cex <- RCircos.Get.Plot.Parameters()$point.size
  scatter.data <- RCircos.Get.Plot.Data.nosort(scatter.data, "plot")
  
  locations <- RCircos.Track.Positions.my(side, track.num, track.heights = 4)
  out.pos <- locations[1]
  in.pos <- locations[2]
  point.bottom <- in.pos
  data.ceiling <- max(scatter.data[, data.col])

  sub.height <- out.pos - point.bottom
  
  RCircos.Track.Outline.my(out.pos, in.pos)
  
  scatter.data[scatter.data[data.col]>data.ceiling, data.col] <- data.ceiling
  scatter.data[scatter.data[data.col]<(-data.ceiling), data.col] <- -data.ceiling
  scatter.data$height <- point.bottom + scatter.data[, data.col]/data.ceiling * sub.height
  scatter.data$x_coord <- RCircos.Pos[scatter.data$Location, 1] * scatter.data$height
  scatter.data$y_coord <- RCircos.Pos[scatter.data$Location, 2] * scatter.data$height

  points(scatter.data$x_coord,
         scatter.data$y_coord,
         col = scatter.colors,
         pch = pch, 
         cex = cex)
}


RCircos.Track.Outline.my <- function (out.pos, in.pos, num.layers = 1) 
{
  RCircos.Cyto <- RCircos.Get.Plot.Ideogram()
  RCircos.Pos <- RCircos.Get.Plot.Positions()
  RCircos.Par <- RCircos.Get.Plot.Parameters()
  subtrack.height <- (out.pos - in.pos)/num.layers
  chroms <- unique(RCircos.Cyto$Chromosome)
  for (a.chr in 1:length(chroms)) {
    the.chr <- RCircos.Cyto[RCircos.Cyto$Chromosome == chroms[a.chr], ]
    start <- the.chr$StartPoint[1]
    end <- the.chr$EndPoint[nrow(the.chr)]
    polygon.x <- c(RCircos.Pos[start:end, 1] * out.pos, RCircos.Pos[end:start, 
                                                                    1] * in.pos)
    polygon.y <- c(RCircos.Pos[start:end, 2] * out.pos, RCircos.Pos[end:start, 
                                                                    2] * in.pos)
    polygon(polygon.x, polygon.y, col = NULL, lwd=0.3, border=RCircos.Par$grid.line.color)
    
    for (a.line in 1:(num.layers - 1)) {
      height <- out.pos - a.line * subtrack.height
      lines(RCircos.Pos[start:end, 1] * height, RCircos.Pos[start:end, 2] * height, col = RCircos.Par$grid.line.color, lwd=0.3)
    }
  }
}


RCircos.Track.Positions.my <- function (side, track.num, track.heights = 1) 
{
  RCircos.Par <- RCircos.Get.Plot.Parameters()
  one.track <- RCircos.Par$track.height + RCircos.Par$track.padding
  side <- tolower(side)
  if (side == "in") {
    out.pos <- RCircos.Par$track.in.start - (track.num - 
                                               1) * one.track
    in.pos <- out.pos - RCircos.Par$track.height - 
      one.track * ( track.heights - 1)
  } else if (side == "out") {
    in.pos <- RCircos.Par$track.out.start + (track.num - 
                                               1) * one.track
    out.pos <- in.pos + RCircos.Par$track.height
  } else {
    stop("Incorrect track location. It must be \"in\" or \"out\".")
  }
  return(c(out.loc = out.pos, in.loc = in.pos))
}


set.plot.circosParams <- function(){
  
  # circos parameters
  circosParams.my <- list()
  
  #use these two circosParams to adjust circle size
  circosParams.my$plot.radius <- 2.15
  circosParams.my$genomeplot.margin <- 0.25
  
  circosParams.my$track.background <- 'white'
  circosParams.my$highlight.width <- 0.2
  circosParams.my$point.size <- 0.3
  circosParams.my$point.type <- 16
  circosParams.my$radius.len <- 3
  circosParams.my$chr.ideog.pos <- 3.2
  circosParams.my$highlight.pos <- 2.09 #3.35
  circosParams.my$chr.name.pos <- 2.14 #3.45
  circosParams.my$track.in.start <- 3.05
  circosParams.my$track.out.start <- 3.2
  
  circosParams.my$tracks.inside <- 10
  circosParams.my$tracks.outside <- 1
  
  circosParams.my$line.width <- 1
  circosParams.my$link.line.width <- 0.5
  
  circosParams.my$text.size <-  0.6
  
  circosParams.my$text.color <- 'black'
  
  circosParams.my$track.padding <- c(0.07,  0.0, 0.07, 0.0,0.07, 0)
  
  circosParams.my$grid.line.color <- 'lightgrey'
  circosParams.my$chr.text.color <- 'grey'
  
  circosParams.my$track.heights <- c(0.85, 0.07, 0.07, 0.1, 0.1,  0.1)
  circosParams.my$track.height <- 0.1
  circosParams.my$sub.tracks <- 1
  circosParams.my$heatmap.cols <- c(alpha('lightcoral', 1),
                              alpha('lightcoral', 0.5),
                              alpha('lightgrey',0.10),
                              alpha('olivedrab2', 0.3),
                              alpha('olivedrab2', 0.5),
                              alpha('olivedrab2',.7),
                              alpha('olivedrab2', 0.75),
                              alpha('olivedrab3', 0.9),
                              alpha('olivedrab4', 0.9))
  circosParams.my$heatmap.ranges <- c(0,1,3,4,8,16, 32,64,1000)
  
  #Set copynumber (and indel) colour scheme
  circosParams.my$heatmap.color.gain <- c( alpha('lightgrey',0.10), alpha('olivedrab2', 0.3),  alpha('olivedrab2', 0.5), alpha('olivedrab2',.7), alpha('olivedrab2', 0.75), alpha('olivedrab3', 0.9), alpha('olivedrab4', 0.9))
  circosParams.my$heatmap.ranges.gain <- c(0,2,4,8,16, 32,64,1000)
  
  circosParams.my$heatmap.ranges.loh <- c(0,1,1000)
  circosParams.my$heatmap.color.loh <- c(alpha('lightcoral', 1), alpha('lightgrey',0.10))
  
  circosParams.my$heatmap.key.gain.col <- alpha('olivedrab2', 0.3)
  circosParams.my$heatmap.key.loh.col <- alpha('lightcoral', 1)
  circosParams.my$heatmap.key.gain.title <- 'gain'
  circosParams.my$heatmap.key.loh.title <- 'LOH'
  
  #tumour majorCN
  circosParams.my$heatmap.data.col.gain <- 8
  #tumour minorCN
  circosParams.my$heatmap.data.col.loh <- 7
  
  #Indel colours
  circosParams.my$indel.mhomology <- 'firebrick4'
  circosParams.my$indel.repeatmediated <- 'firebrick1'
  circosParams.my$indel.other <- 'firebrick3'
  circosParams.my$indel.insertion <- 'darkgreen'
  circosParams.my$indel.complex <- 'grey'

  return(circosParams.my)
  
}


genomePlot <- function(snvs.file, indels.file, cnvs.file, rearrs.file, 
                       sampleID, genome.v="hg19", ..., plot_title = NULL, 
                       no_copynumber = FALSE, no_rearrangements = FALSE, no_indels = FALSE, out_format = "png", out_path = ".") {
  
  genome.ideogram = switch(genome.v,
                           "hg19" = "UCSC.HG19.Human.CytoBandIdeogram",
                           "hg38" = "UCSC.HG38.Human.CytoBandIdeogram")
  data(list=genome.ideogram, package = "RCircos");
  species.cyto <- get(genome.ideogram);
  
  circosParams.my <- set.plot.circosParams()
  
  # rearrangement links colors
  inv.col <- alpha('dodgerblue2', 1)
  del.col <- alpha('coral2', 1)
  dupl.col <-  alpha('darkgreen', 1)
  transloc.colour <- alpha('gray35', 1)
  
  #Set up height, width and resolution parameters
  cPanelWidth = 0
  graph.height = 4100
  graph.wd_ht_ratio = 1  #width/height ratio
  graph.width = graph.height * graph.wd_ht_ratio
  graph.wd_res_ratio = (4100/550)
  graph.res = graph.width/graph.wd_res_ratio
  
  graph.height.inches = graph.height/graph.res
  graph.width.inches = graph.width/graph.res

  # substitutions
  subs <- read.table(file = snvs.file, sep = '\t', header = TRUE)
  # subs <- read.table(file = '/home/dapregi/tmp/snvs.tsv', sep = '\t', header = TRUE)
  subs$color[(subs$ref=='C' & subs$alt=='A') | (subs$ref=='G' & subs$alt=='T')] <- 'royalblue'
  subs$color[(subs$ref=='C' & subs$alt=='G') | (subs$ref=='G' & subs$alt=='C')] <- 'black'
  subs$color[(subs$ref=='C' & subs$alt=='T') | (subs$ref=='G' & subs$alt=='A')] <- 'red'
  subs$color[(subs$ref=='T' & subs$alt=='A') | (subs$ref=='A' & subs$alt=='T')] <- 'grey'
  subs$color[(subs$ref=='T' & subs$alt=='C') | (subs$ref=='A' & subs$alt=='G')] <- 'green2'
  subs$color[(subs$ref=='T' & subs$alt=='G') | (subs$ref=='A' & subs$alt=='C')] <- 'hotpink'
  
  # indels
  indels <- read.table(file = indels.file, sep = '\t', header = TRUE)
  # indels <- read.table(file = '/home/dapregi/tmp/indels.tsv', sep = '\t', header = TRUE)
  dels.formatted <- data.frame()
  ins.formatted <- data.frame()
  if (!no_indels && !is.null(indels)) {
    ins <- indels[which(indels$type=='I'),]
    dels <- indels[which(indels$type=='D' | indels$type=='DI'),]
    dels$color[dels$classification=='Microhomology-mediated'] <- circosParams.my$indel.mhomology
    dels$color[dels$classification=='Repeat-mediated'] <- circosParams.my$indel.repeatmediated
    dels$color[dels$classification=='None'] <- circosParams.my$indel.other
  } 
  
  # copy number
  cv.data <- data.frame()
  #Skip if no copynumber was requested
  if (!no_copynumber) {
    cv.data <- read.table(file = cnvs.file, sep = '\t', header = TRUE)
    # cv.data <- read.table(file = '/home/dapregi/tmp/cnvs.tsv', sep = '\t', header = TRUE)
    if(is.null(cv.data) || nrow(cv.data)==0){
      no_copynumber <- TRUE
    }
  }
  
  # rearrangements
  rearrs <- data.frame()
  if (!no_rearrangements) {
    rearrs <- read.table(file = rearrs.file, sep = '\t', header = TRUE)
    if(is.null(rearrs) || nrow(rearrs)==0){
      no_rearrangements <- TRUE
    }
  }
  
  ################################################################################
  
  fn = file.path(out_path, paste(sampleID, ".genomePlot.", out_format, sep=''), fsep = .Platform$file.sep)
  
  if (out_format == 'png') {
    png(file=fn, height=graph.height, width=(graph.width*(1/(1-cPanelWidth))), res=graph.res)
  } else if (out_format == 'svg') {
    svg(fn, height=graph.height.inches, width=graph.width.inches)
  } else {
    stop("Invalid file type. Only png and svg are supported");
  }
  
  RCircos.Set.Core.Components(cyto.info=species.cyto, chr.exclude=NULL,  tracks.inside=circosParams.my$tracks.inside,
                              tracks.outside=circosParams.my$tracks.outside);

  # set plot colours and parameters
  circosParams <- RCircos.Get.Plot.Parameters();
  circosParams$point.type <- circosParams.my$point.type
  circosParams$point.size <- circosParams.my$point.size
  RCircos.Reset.Plot.Parameters(circosParams)
  
  par(mar=c(0.001, 0.001, 0.001, 0.001))
  par(mai=c(circosParams.my$genomeplot.margin, circosParams.my$genomeplot.margin, circosParams.my$genomeplot.margin, circosParams.my$genomeplot.margin))
  plot.new()
  plot.window(c(-circosParams.my$plot.radius,circosParams.my$plot.radius), c(-circosParams.my$plot.radius, circosParams.my$plot.radius))
  RCircos.Chromosome.Ideogram.Plot.my(circosParams.my$chr.text.color, circosParams.my$grid.line.color, circosParams.my$text.size);
  
  title(main = sampleID)
  
  if (!is.null(plot_title)) {
    title(paste(plot_title, sep=''), line=-1);
  }
  
  # substitutions
  # start.time <- Sys.time()
  # summary(subs)
  # subs$distance <- 10^subs$logDistPrev
  # subs <- subs[subs$distance<400000,]
  # print(nrow(subs))
  if (exists("subs")) {
    RCircos.Scatter.Plot.color(scatter.data=subs, data.col=6, track.num=1, side="in", scatter.colors = subs$color);
  }
  # end.time <- Sys.time()
  # time.taken <- end.time - start.time
  # print(time.taken)

  # Insertions
  circosParams <- RCircos.Get.Plot.Parameters();
  circosParams$line.color <- 'white'
  circosParams$highlight.width <- 0.2
  circosParams$max.layers <- 5
  circosParams$tile.color <- 'darkgreen'
  RCircos.Reset.Plot.Parameters(circosParams)
  if (exists("ins") && nrow(ins)>0) {
    my.RCircos.Tile.Plot(tile.data=ins, track.num=5, side="in");
  }
  
  # Deletions
  circosParams <- RCircos.Get.Plot.Parameters();
  circosParams$tile.color <- 'firebrick4'
  RCircos.Reset.Plot.Parameters(circosParams)
  if (exists("dels") && nrow(dels)>0) {
    my.RCircos.Tile.Plot(tile.data=dels, track.num=6, side="in", tile.colors=dels$color);
  }
  
  
  # Copy number
  if (exists('cv.data') && (nrow(cv.data)>0)) {
    heatmap.ranges.major <-circosParams.my$heatmap.ranges.gain
    heatmap.color.major <-circosParams.my$heatmap.color.gain
    RCircos.Heatmap.Plot.my(heatmap.data=cv.data, data.col=5, track.num=7, side="in", heatmap.ranges=heatmap.ranges.major , heatmap.color=heatmap.color.major ); # major copy number

    heatmap.ranges.minor <-circosParams.my$heatmap.ranges.loh
    heatmap.color.minor <-circosParams.my$heatmap.color.loh
    RCircos.Heatmap.Plot.my(heatmap.data=cv.data, data.col=6, track.num=8, side="in", heatmap.ranges=heatmap.ranges.minor , heatmap.color=heatmap.color.minor ); # minor copy number

  }
  
  # Rearrangement
  # Chromosome chromStart  chromEnd Chromosome.1 chromStart.1 chromEnd.1 type
  link.colors <- vector()
  if (exists("rearrs")) {
    link.data <- rearrs
    link.colors[link.data$type=='INV'] <- inv.col
    link.colors[link.data$type=='DEL'] <- del.col
    link.colors[link.data$type=='DUP'] <- dupl.col
    link.colors[link.data$type=='BND'] <- transloc.colour
    if (nrow( rearrs)>0) {
      RCircos.Link.Plot.my(link.data = rearrs, track.num=9, by.chromosome=TRUE, link.colors);
    }
  }
  
  invisible(dev.off())

}

####################################################################################

# Getting command arguments
option_list <- list(
  make_option(c("--genome_version"), type="character", default="hg19",
              help="Genome version", metavar="character"),
  make_option(c("--plot_title"), type="character", default="",
              help="Plot title", metavar="character"),
  make_option(c("--no_copynumber"), action="store_true", default=FALSE,
              help="No CNV"),
  make_option(c("--no_rearrangements"), action="store_true", default=FALSE,
              help="No rearrangements"),
  make_option(c("--no_indels"), action="store_true", default=FALSE,
              help="No indels"),
  make_option(c("--out_format"), type="character", default="png",
              help="Output format", metavar="character"),
  make_option(c("--out_path"), type="character", default=".",
              help="Output file path", metavar="character")
  )
parser <- OptionParser(usage = "%prog [options] snvs_file indels_file cnvs_file rearrs_file sampleId", option_list=option_list)
arguments <- parse_args(parser, positional_arguments = 5)
opt <- arguments$options
args <- arguments$args


genomePlot(args[1], args[2], args[3], args[4], args[5], genome.v=opt$genome_version, plot_title = opt$plot_title,
           no_copynumber = opt$no_copynumber, no_rearrangements = opt$no_rearrangements, no_indels = opt$no_indels,
           out_format = opt$out_format, out_path = opt$out_path)

# start.time <- Sys.time()
# genomePlot('/home/dapregi/tmp/snvs.chr.tsv', '/home/dapregi/tmp/indels.tsv', '/home/dapregi/tmp/cnvs.tsv', '/home/dapregi/tmp/rearrs.tsv',
#            'sampleID', genome.v="hg19", plot_title = '',
#            no_copynumber = FALSE, no_rearrangements = FALSE, no_indels = FALSE, out_format = "png", out_path = "/home/dapregi/tmp/")
# end.time <- Sys.time()
# time.taken <- end.time - start.time
# print(time.taken)