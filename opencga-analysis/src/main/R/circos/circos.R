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
      # tile.layers[a.row] <- the.layer
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
    # layer.bot <- in.pos + layer.height * (tile.layers[a.row] - 1)
    # layer.top <- layer.bot + layer.height * 0.8
    # layer.top <- layer.bot + layer.height
    
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
    ##old RCircos version
    #start <- the.chr$Location[1] - the.chr$Unit[1] + 1
    #end <- the.chr$Location[nrow(the.chr)]
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
    ##old RCircos version
    #start <- RCircos.Cyto$Location[a.band] - RCircos.Cyto$Unit[a.band] + 1
    #end <- RCircos.Cyto$Location[a.band]
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


RCircos.Gene.Connector.Plot.my <- function (genomic.data, track.num, side, in.pos = 1.32) 
{
  RCircos.Pos <- RCircos.Get.Plot.Positions()
  RCircos.Par <- RCircos.Get.Plot.Parameters()
  gene.data <- RCircos.Get.Plot.Data(genomic.data, "plot")
  label.data <- RCircos.Get.Gene.Label.Locations(gene.data)
  connect.data <- data.frame(label.data$Location, label.data$Label.Position)
  locations <- RCircos.Track.Positions(side, track.num)
  
  out.pos <- locations[1] # 
  
  
  line.colors <- RCircos.Get.Plot.Colors(label.data, RCircos.Par$text.color)
  
  
  genomic.col <- ncol(connect.data) - 1
  label.col <- ncol(connect.data)
  chroms <- unique(connect.data[, 1])
  for (a.chr in 1:length(chroms)) {
    chr.row <- which(connect.data[, 1] == chroms[a.chr])
    total <- length(chr.row)
    for (a.point in 1:total) {
      top.loc <- out.pos
      bot.loc <- RCircos.Par$track.in.start  - sum(RCircos.Par$track.heights[1:length(RCircos.Par$track.heights)]) - sum(RCircos.Par$track.padding[1:length(RCircos.Par$track.padding)] ) - 0.02
      
      
      p1 <- connect.data[chr.row[a.point], genomic.col]
      p2 <- connect.data[chr.row[a.point], genomic.col] # p2 <- connect.data[chr.row[a.point], label.col]
      
      # lines(c(RCircos.Pos[p1, 1] * out.pos, RCircos.Pos[p1,1] * top.loc),
      #       c(RCircos.Pos[p1, 2] * out.pos, RCircos.Pos[p1, 2] * top.loc), col = 'red')
      
      # lines(c(RCircos.Pos[p2, 1] * bot.loc, RCircos.Pos[p2, 1] * in.pos),
      #       c(RCircos.Pos[p2, 2] * bot.loc, RCircos.Pos[p2, 2] * in.pos), col = 'green')
      
      
      lines(c(RCircos.Pos[p1, 1] * top.loc, RCircos.Pos[p2, 1] * bot.loc), # xs
            c(RCircos.Pos[p1, 2] * top.loc, RCircos.Pos[p2, 2] * bot.loc), col =  alpha('black', 0.1), lwd=0.5) # ys
    }
  }
}


RCircos.Gene.Name.Plot.my <- function (gene.data, name.col, track.num, side, colors) 
{
  RCircos.Pos <- RCircos.Get.Plot.Positions()
  RCircos.Par <- RCircos.Get.Plot.Parameters()
  gene.data <- RCircos.Get.Plot.Data.nosort(gene.data, "plot")
  gene.data <- RCircos.Get.Gene.Label.Locations(gene.data)
  side <- tolower(side)
  locations <- RCircos.Track.Positions(side, track.num)
  if (side == "in") {
    label.pos <- locations[1]
  }
  else {
    label.pos <- locations[2]
  }
  right.side <- nrow(RCircos.Pos)/2
  text.colors <- RCircos.Get.Plot.Colors(gene.data, RCircos.Par$text.color)
  for (a.text in 1:nrow(gene.data)) {
    gene.name <- as.character(gene.data[a.text, name.col])
    the.point <- as.numeric(gene.data[a.text, ncol(gene.data)])
    rotation <- RCircos.Pos$degree[the.point]
    if (side == "in") {
      if (the.point <= right.side) {
        text.side <- 2
      }
      else {
        text.side <- 4
      }
    }
    else {
      if (the.point <= right.side) {
        text.side <- 4
      }
      else {
        text.side <- 2
      }
    }
    
    text(RCircos.Pos[the.point, 1] * label.pos, RCircos.Pos[the.point, 
                                                            2] * label.pos, label = gene.name, pos = text.side, 
         cex = RCircos.Par$text.size, srt = rotation, offset = 0, 
         col = as.character(gene.data$color[a.text]))
  }
}


RCircos.Get.Plot.Data.nosort <- function (genomic.data, plot.type, validate=TRUE) 
{
  
  if (validate) {
    genomic.data <- RCircos.Validate.Genomic.Data.my(genomic.data, plot.type) }
  data.points <- rep(0, nrow(genomic.data))
  for (a.row in 1:nrow(genomic.data)) {
    if ((a.row %% 1000)==0) {
      cat(paste(a.row,sep=''))
    }
    chromosome <- as.character(genomic.data[a.row, 1])
    location <- round((genomic.data[a.row, 2] + genomic.data[a.row, 
                                                             3])/2, digits = 0)
    data.points[a.row] <- RCircos.Data.Point(chromosome, location)
  }
  genomic.data["Location"] <- data.points
  # genomic.data <- genomic.data[order(genomic.data$Location), ]
  return(genomic.data)
}


RCircos.Get.Plot.Data.segment <- function (genomic.data, plot.type) 
{
  genomic.data <- RCircos.Validate.Genomic.Data.my(genomic.data, 'plot')
  data.points.start <- rep(0, nrow(genomic.data))
  data.points.end <- rep(0, nrow(genomic.data))
  
  for (a.row in 1:nrow(genomic.data)) {
    chromosome <- as.character(genomic.data[a.row, 1])
    data.points.start[a.row] <- RCircos.Data.Point(chromosome, genomic.data[a.row, 2])
    data.points.end[a.row] <- RCircos.Data.Point(chromosome,  genomic.data[a.row, 3])
  }
  genomic.data["Location.start"] <- data.points.start
  genomic.data["Location.end"] <- data.points.end
  # genomic.data <- genomic.data[order(genomic.data$Location), ]
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
    #locations <- as.numeric(RCircos.Cyto$Location[cyto.rows]) # chromosome locations
    #chr.start <- min(locations) - RCircos.Cyto$Unit[cyto.rows[1]] # chromosome start
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
    #the.start <- the.chr$Location[1] - the.chr$Unit[1] + 1
    #the.end <- the.chr$Location[nrow(the.chr)]
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


RCircos.Line.Plot.cn <- function (line.data, data.col, track.num, side, lineCol, lineType) 
{
  RCircos.Pos <- RCircos.Get.Plot.Positions()
  RCircos.Par <- RCircos.Get.Plot.Parameters()
  
  
  line.data1 <- RCircos.Get.Plot.Data.nosort(data.frame(chromosome=line.data$chromosome, start=line.data$start, end=line.data$start), "plot") # add a last column with integer locations
  line.data2 <- RCircos.Get.Plot.Data.nosort(data.frame(chromosome=line.data$chromosome, start=line.data$end, end=line.data$end), "plot")
  locations <- RCircos.Track.Positions.my(side, track.num)
  
  out.pos <- locations[1] # posiitons of the track
  in.pos <- locations[2] # position of the track
  
  if (min(as.numeric(line.data[, data.col])) >= 0) {
    point.bottom <- in.pos
    data.ceiling <- max(line.data[, data.col]) # data between 0 and 5
  }
  else {
    point.bottom <- in.pos + (RCircos.Par$track.height/2)
    data.ceiling <- 3 # data between -5 and 5
  }
  sub.height <- out.pos - point.bottom
  # line.colors <- RCircos.Get.Plot.Colors(line.data, RCircos.Par$line.color)
  
  RCircos.Track.Outline.my(out.pos, in.pos, RCircos.Par$sub.tracks)
  
  for (a.point in 1:(nrow(line.data))) {
    point.one <- line.data1[a.point, ncol(line.data1)] # integer location of point  1
    point.two <- line.data2[a.point, ncol(line.data2)] # integer location of point 2
    
    # cut the values if needed
    # if (line.data[a.point, 1] != line.data[a.point + 1, 1]) {
    #     next
    # }
    if (line.data[a.point, data.col] > data.ceiling) {
      value.one <- data.ceiling
    }
    else if (line.data[a.point, data.col] < (-1 * data.ceiling)) {
      value.one <- data.ceiling * -1
    }
    else {
      value.one <- line.data[a.point, data.col]
    }
    
    if (line.data[a.point , data.col] > data.ceiling) {
      value.two <- data.ceiling
    }
    else if (line.data[a.point , data.col] < (-1 * data.ceiling)) {
      value.two <- data.ceiling * -1
    }
    else {
      value.two <- line.data[a.point, data.col]
    }
    
    height.one <- point.bottom + value.one/data.ceiling * sub.height # scale the y values
    height.two <- point.bottom + value.two/data.ceiling * sub.height # scale the y values
    
    # height <- out.pos - a.line * subtrack.height
    # lines(RCircos.Pos[start:end, 1] * height, RCircos.Pos[start:end, 2] * height, col = RCircos.Par$grid.line.color, lwd=0.3)
    
    lines(c(RCircos.Pos[point.one:point.two, 1] * height.one), # xs
          c(RCircos.Pos[point.one:point.two, 2] * height.one), # ys, RCircos.Pos[point.one, *] is always 1 anyway 
          col = lineCol[a.point], lty=lineType, lwd=0.7)
    
  }
}


RCircos.Line.Plot.my <- function (line.data, data.col, track.num, side, lineCol, lineType) 
{
  RCircos.Pos <- RCircos.Get.Plot.Positions()
  RCircos.Par <- RCircos.Get.Plot.Parameters()
  
  line.data1 <- RCircos.Get.Plot.Data.nosort(data.frame(chromosome=line.data$chromosome, start=line.data$start, end=line.data$start), "plot") # add a last column with integer locations
  line.data2 <- RCircos.Get.Plot.Data.nosort(data.frame(chromosome=line.data$chromosome, start=line.data$end, end=line.data$end), "plot")
  # get locations of the tracks
  locations <- RCircos.Track.Positions.my(side, track.num)
  
  out.pos <- locations[1] # posiitons of the track
  in.pos <- locations[2] # position of the track
  
  
  if (min(as.numeric(line.data[, data.col])) >= 0) {
    point.bottom <- in.pos
    
    data.ceiling <- max(line.data[, data.col]) # data between 0 and 5
  }
  else {
    point.bottom <- in.pos + (RCircos.Par$track.height/2)
    data.ceiling <- 3 # data between -5 and 5
  }
  sub.height <- out.pos - point.bottom
  # line.colors <- RCircos.Get.Plot.Colors(line.data, RCircos.Par$line.color)
  
  RCircos.Track.Outline.my(out.pos, in.pos, RCircos.Par$sub.tracks)
  
  for (a.point in 1:(nrow(line.data))) {
    point.one <- line.data1[a.point, ncol(line.data1)] # integer location of point  1
    point.two <- line.data2[a.point, ncol(line.data2)] # integer location of point 2
    
    
    # cut the values if needed
    # if (line.data[a.point, 1] != line.data[a.point + 1, 1]) {
    #     next
    # }
    if (line.data[a.point, data.col] > data.ceiling) {
      value.one <- data.ceiling
    }
    else if (line.data[a.point, data.col] < (-1 * data.ceiling)) {
      value.one <- data.ceiling * -1
    }
    else {
      value.one <- line.data[a.point, data.col]
    }
    
    if (line.data[a.point , data.col] > data.ceiling) {
      value.two <- data.ceiling
    }
    else if (line.data[a.point , data.col] < (-1 * data.ceiling)) {
      value.two <- data.ceiling * -1
    }
    else {
      value.two <- line.data[a.point, data.col]
    }
    
    height.one <- point.bottom + value.one/data.ceiling * sub.height # scale the y values
    height.two <- point.bottom + value.two/data.ceiling * sub.height # scale the y values
    
    # height <- out.pos - a.line * subtrack.height
    # lines(RCircos.Pos[start:end, 1] * height, RCircos.Pos[start:end, 2] * height, col = RCircos.Par$grid.line.color, lwd=0.3)
    
    lines(c(RCircos.Pos[point.one:point.two, 1] * height.one), # xs
          c(RCircos.Pos[point.one:point.two, 2] * height.one), # ys, RCircos.Pos[point.one, *] is always 1 anyway 
          col = lineCol[a.point], lty=lineType, lwd=1.5)
    
  }
}


RCircos.Link.Plot.my <- function (link.data, track.num, by.chromosome = FALSE, link.colors=NA)
{
  
  if (length(link.colors)==1) {
    link.colors <- rep('BurlyWood', nrow(link.data))
  }
  
  
  RCircos.Pos <- RCircos.Get.Plot.Positions()
  RCircos.Par <- RCircos.Get.Plot.Parameters()
  link.data <- RCircos.Validate.Genomic.Data.my(link.data, plot.type = "link")
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
  # link.colors <- RCircos.Get.Link.Colors(link.data, by.chromosome)
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
    # lines(links$pos.x, links$pos.y, type = "l", col = link.colors[a.link])
    #        lines(links$pos.x, links$pos.y, type = "l", col = link.colors[a.link], lwd=0.5)
    lines(links$pos.x, links$pos.y, type = "l", col = link.colors[a.link], lwd=RCircos.Par$link.line.width)
  }
}


RCircos.Scatter.Plot.cn <- function (scatter.data, track.num, side, by.fold = 0,  theColor, plotTrack=TRUE) 
{
  
  no.points <- length(scatter.data)
  
  
  RCircos.Pos <- RCircos.Get.Plot.Positions()
  RCircos.Par <- RCircos.Get.Plot.Parameters()
  
  # scatter.data <- RCircos.Get.Plot.Data.nosort(scatter.data, "plot")
  
  
  locations <- RCircos.Track.Positions.my(side, track.num)
  out.pos <- locations[1]
  in.pos <- locations[2]
  if (min(as.numeric(scatter.data)) >= 0) {
    point.bottom <- in.pos
    data.ceiling <-1
  }
  else {
    point.bottom <- in.pos + (RCircos.Par$track.height/2)
    data.ceiling <- 5
  }
  sub.height <- out.pos - point.bottom
  
  if (plotTrack) {
    RCircos.Track.Outline.my(out.pos, in.pos, RCircos.Par$sub.tracks)
  }
  for (a.point in 1:length(scatter.data)) {
    the.point <- a.point
    if (scatter.data[a.point] > data.ceiling) {
      the.value <- data.ceiling
    }
    else if (scatter.data[a.point] < (-1 * data.ceiling)) {
      the.value <- data.ceiling * -1
    }
    else {
      the.value <- scatter.data[a.point]
    }
    
    height <- point.bottom + the.value/data.ceiling * sub.height
    points(RCircos.Pos[the.point, 1] * height, RCircos.Pos[the.point, 
                                                           2] * height, col = theColor, pch = RCircos.Par$point.type, 
           cex = RCircos.Par$point.size)
  }
}


RCircos.Scatter.Plot.color <- function (scatter.data, data.col, track.num, side, by.fold = 0,  scatter.colors, draw.bg =TRUE, draw.scale=FALSE, no.sort=FALSE, data.ceiling=NA) 
{
  
  # scatter.data.original <- scatter.data
  
  # scatter.data.original.small <- scatter.data.original[c(1:10, 601:610),]
  # no.points.small <- nrow(scatter.data.original.small )
  # new.order.small <- sample(1:no.points.small, no.points.small)
  # scatter.data.original.small[new.order.small, ]
  # scatter.data.original.small$rate[scatter.data.original.small$col=='grey']
  
  # scatter.data <- scatter.data.original 
  
  no.points <- nrow(scatter.data)
  if (no.sort) {
    new.order <- 1:no.points
  } else {
    new.order <- sample(1:no.points, no.points)
  }
  
  # apply new ordering
  scatter.data <- scatter.data[new.order,]
  scatter.colors <- scatter.colors[new.order]
  
  RCircos.Pos <- RCircos.Get.Plot.Positions()
  RCircos.Par <- RCircos.Get.Plot.Parameters()
  scatter.data <- RCircos.Get.Plot.Data.nosort(scatter.data, "plot")
  
  
  locations <- RCircos.Track.Positions.my(side, track.num, track.heights = 4)
  out.pos <- locations[1]
  in.pos <- locations[2]
  if (min(as.numeric(scatter.data[, data.col])) >= 0) {
    point.bottom <- in.pos
    if (is.na(data.ceiling)) {
      data.ceiling <- max(scatter.data[, data.col])
    }
  } else {
    point.bottom <- in.pos + (RCircos.Par$track.height/2)
    if (is.na(data.ceiling)) {
      data.ceiling <- 5
    }
  }
  sub.height <- out.pos - point.bottom
  
  if (draw.bg) {
    RCircos.Track.Outline.my(out.pos, in.pos) #, RCircos.Par$sub.tracks)
  }
  if (draw.scale) {
    text(RCircos.Pos[1, 1] * locations[1], RCircos.Pos[1, 2] * locations[1], round(data.ceiling), cex=0.5)
    text(RCircos.Pos[1, 1] * locations[2], RCircos.Pos[1, 2] * locations[2], '0', cex=0.5)
  }
  
  
  for (a.point in 1:nrow(scatter.data)) {
    the.point <- scatter.data[a.point, ncol(scatter.data)]
    color <- scatter.colors[a.point]
    if (scatter.data[a.point, data.col] > data.ceiling) {
      the.value <- data.ceiling
    }
    else if (scatter.data[a.point, data.col] < (-1 * data.ceiling)) {
      the.value <- data.ceiling * -1
    }
    else {
      the.value <- scatter.data[a.point, data.col]
    }
    
    if (by.fold > 0) {
      if (the.value >= by.fold) {
        color <- "red"
      }
      else if (the.value <= -by.fold) {
        color <- "blue"
      }
      else {
        color <- "black"
      }
    }
    height <- point.bottom + the.value/data.ceiling * sub.height
    points(RCircos.Pos[the.point, 1] * height,
           RCircos.Pos[the.point, 2] * height,
           col = color,
           pch = RCircos.Par$point.type, 
           cex = RCircos.Par$point.size)
  }
}


RCircos.Scatter.Plot.ra <- function (scatter.data, mids, track.num, side, by.fold = 0,  theColor, maxvalue=NA, plot.bg=TRUE, p.pch=NA, p.cex=NA, is.lines=TRUE) 
{
  
  no.points <- length(scatter.data)
  new.order <- sample(1:no.points, no.points)
  scatter.data <- scatter.data[new.order]
  mids <- mids[new.order]
  
  if (length(theColor)==1) {
    theColor <- rep(theColor, no.points)
  }
  
  theColor <- theColor[ new.order]
  
  
  RCircos.Pos <- RCircos.Get.Plot.Positions()
  RCircos.Par <- RCircos.Get.Plot.Parameters()
  
  # scatter.data <- RCircos.Get.Plot.Data.nosort(scatter.data, "plot")
  
  
  locations <- RCircos.Track.Positions.my(side, track.num)
  out.pos <- locations[1]
  in.pos <- locations[2]
  if (min(as.numeric(scatter.data)) >= 0) {
    point.bottom <- in.pos
    if (!is.na(maxvalue)) {
      data.ceiling <- maxvalue
    } else {
      data.ceiling <- max(scatter.data)
    }
  }
  else {
    point.bottom <- in.pos + (RCircos.Par$track.height/2)
    data.ceiling <- 5
  }
  sub.height <- out.pos - point.bottom
  
  if (plot.bg) {
    RCircos.Track.Outline.my(out.pos, in.pos, RCircos.Par$sub.tracks)
    
  }
  if (is.na(p.pch)) {
    p.pch <- RCircos.Par$point.type
  }
  if (is.na(p.cex)) {
    p.cex <-  RCircos.Par$point.size
  }
  
  for (a.point in 1:length(scatter.data)) {
    the.point <- mids[a.point]
    if (scatter.data[a.point] > data.ceiling) {
      the.value <- data.ceiling
    }
    else if (scatter.data[a.point] < (-1 * data.ceiling)) {
      the.value <- data.ceiling * -1
    }
    else {
      the.value <- scatter.data[a.point]
    }
    
    heightUp <- point.bottom + the.value/data.ceiling * sub.height
    heightDown <- point.bottom
    
    
    if (is.lines) {
      lines(RCircos.Pos[the.point, 1] * c(heightDown,heightUp),
            RCircos.Pos[the.point, 2] * c(heightDown,heightUp), col = theColor[a.point], lwd=0.8) }
    else {
      points(RCircos.Pos[the.point, 1] * heightUp,
             RCircos.Pos[the.point, 2] * heightUp, col = theColor[a.point], pch = p.pch, cex = p.cex)
    }
  }
  
  if (plot.bg) {
    the.point <- 1
    height <- point.bottom
    
    text(RCircos.Pos[the.point, 1] * height,
         RCircos.Pos[the.point, 2] * height,
         '0', cex=1, po=2)
    
    height <- point.bottom + sub.height
    text(RCircos.Pos[the.point, 1] * height,
         RCircos.Pos[the.point, 2] * height,
         format(maxvalue,  digits=2), cex=1, pos=2)       
  }
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
    #start <- the.chr$Location[1] - the.chr$Unit[1] + 1
    #end <- the.chr$Location[nrow(the.chr)]
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
    #out.pos <- RCircos.Par$track.in.start 
    #=if (track.num>1) {
    #    out.pos <- RCircos.Par$track.in.start - sum( RCircos.Par$track.heights[1:(track.num-1)]) -          
    #         sum(RCircos.Par$track.padding[1:(track.num - 1)])
    #}
    #in.pos <- out.pos - RCircos.Par $track.heights[track.num]       
  } else if (side == "out") {
    in.pos <- RCircos.Par$track.out.start + (track.num - 
                                               1) * one.track
    out.pos <- in.pos + RCircos.Par$track.height
  } else {
    stop("Incorrect track location. It must be \"in\" or \"out\".")
  }
  return(c(out.loc = out.pos, in.loc = in.pos))
}


RCircos.Validate.Genomic.Data.my <- function (genomic.data, plot.type = c("plot", "link")) 
{
  RCircos.Cyto <- RCircos.Get.Plot.Ideogram()
  plot.type <- tolower(plot.type)
  if (plot.type == "plot") {
    chrom.col <- 1
  }
  else if (plot.type == "link") {
    chrom.col <- c(1, 4)
  }
  else {
    stop("Plot type must be \"plot\" or \"line\"")
  }
  for (a.col in 1:length(chrom.col)) {
    the.col <- chrom.col[a.col]
    genomic.data[, the.col] <- as.character(genomic.data[, 
                                                         the.col])
    for (a.row in 1:nrow(genomic.data)) {
      if (length(grep("chr", genomic.data[a.row, the.col])) == 
          0) {
        genomic.data[a.row, the.col] <- paste("chr", 
                                              genomic.data[a.row, the.col], sep = "")
      }
    }
    cyto.chroms <- unique(as.character(RCircos.Cyto$Chromosome))
    data.chroms <- unique(as.character(genomic.data[, the.col]))
    if (sum(data.chroms %in% cyto.chroms) < length(data.chroms)) {
      cat(paste("Some chromosomes are in genomic data only", 
                "and have been removed.\n\n"))
      all.chroms <- as.character(genomic.data[, the.col])
      genomic.data <- genomic.data[all.chroms %in% cyto.chroms, ]
    }
    data.chroms <- unique(as.character(genomic.data[, the.col]))
    if (min(genomic.data[, the.col + 1]) < 0) {
      stop("Error! chromStart position less than 0.")
    }
    if (min(genomic.data[, the.col + 2]) < 0) {
      stop("Error! chromEnd position less than 0.")
    }
    for (a.chr in 1:length(data.chroms)) {
      the.chr <- data.chroms[a.chr]
      in.data <- genomic.data[genomic.data[, the.col] == the.chr, ]
      cyto.data <- RCircos.Cyto[grep(the.chr, RCircos.Cyto$Chromosome), 
                                ]
      
      bad.rows <- in.data[, the.col + 1] > max(cyto.data[, 3])
      in.data[bad.rows, the.col + 1] <- max(cyto.data[, 3])
      bad.rows <- in.data[, the.col + 2] > max(cyto.data[, 3])
      in.data[bad.rows, the.col + 2] <- max(cyto.data[, 3])
      
      genomic.data[genomic.data[, the.col] == the.chr, ] <- in.data 
      
      if (max(in.data[, the.col + 1]) > max(cyto.data[, 3]) | max(in.data[, the.col + 2]) > max(cyto.data[, 3])) {
        cat(paste(the.chr, max(in.data[, 2]), max(in.data[, 3]), "\n"))
        stop("Error! Location is outside of chromosome length.")
      }
    }
    for (a.row in 1:nrow(genomic.data)) {
      if (genomic.data[a.row, the.col + 1] > genomic.data[a.row, 
                                                          the.col + 2]) {
        cat("chromStart greater than chromEnd.\n")
        stop(paste("Row:", a.row, genomic.data[a.row, 
                                               2], genomic.data[a.row, 3]))
      }
    }
  }
  return(genomic.data)
}


set.plot.params <- function(colour.scheme = "ascat"){
  
  # circos parameters
  params.my <- list()
  
  #use these two params to adjust circle size
  params.my$plot.radius <- 2.15
  params.my$genomeplot.margin <- 0.25
  
  params.my$track.background <- 'white'
  params.my$highlight.width <- 0.2
  params.my$point.size <- 0.3
  params.my$point.type <- 16
  params.my$radius.len <- 3
  params.my$chr.ideog.pos <- 3.2
  params.my$highlight.pos <- 2.09 #3.35
  params.my$chr.name.pos <- 2.14 #3.45
  params.my$track.in.start <- 3.05
  params.my$track.out.start <- 3.2
  
  params.my$tracks.inside <- 10
  params.my$tracks.outside <- 1
  
  params.my$line.width <- 1
  params.my$link.line.width <- 0.5
  
  params.my$text.size <-  0.6
  
  params.my$text.color <- 'black'
  
  params.my$track.padding <- c(0.07,  0.0, 0.07, 0.0,0.07, 0)
  
  params.my$grid.line.color <- 'lightgrey'
  params.my$chr.text.color <- 'grey'
  
  params.my$track.heights <- c(0.85, 0.07, 0.07, 0.1, 0.1,  0.1)
  params.my$track.height <- 0.1
  params.my$sub.tracks <- 1
  params.my$heatmap.cols <- c(alpha('lightcoral', 1),
                              alpha('lightcoral', 0.5),
                              alpha('lightgrey',0.10),
                              alpha('olivedrab2', 0.3),
                              alpha('olivedrab2', 0.5),
                              alpha('olivedrab2',.7),
                              alpha('olivedrab2', 0.75),
                              alpha('olivedrab3', 0.9),
                              alpha('olivedrab4', 0.9))
  params.my$heatmap.ranges <- c(0,1,3,4,8,16, 32,64,1000)
  
  #Set copynumber (and indel) colour scheme
  if (colour.scheme == 'picnic') {
    
    #tumour totalCN column
    params.my$heatmap.data.col.gain <- 6
    params.my$heatmap.data.col.loh <- 6
    
    params.my$heatmap.color.gain <- c(alpha('white',1), alpha('lightgrey',0.10), alpha('firebrick1',0.7), alpha('firebrick3',1.0))
    params.my$heatmap.ranges.gain <- c(0, 2, 4, 8, 1000)
    
    params.my$heatmap.ranges.loh <- c(0,1,2,1000)
    params.my$heatmap.color.loh <- c(alpha('darkblue', 1), alpha('grey',0.50), alpha('white',1))
    
    params.my$heatmap.key.gain.col <- alpha('firebrick1', 0.9)
    params.my$heatmap.key.loh.col <- alpha('darkblue', 1)
    params.my$heatmap.key.gain.title <- 'gain'
    params.my$heatmap.key.loh.title <- 'deletion'
    
    #Indel colours (to deferentiate from the copynumber colours)
    params.my$indel.mhomology <- 'mediumpurple4'
    params.my$indel.repeatmediated <- 'mediumpurple1'
    params.my$indel.other <- 'mediumorchid3'
    params.my$indel.insertion <- 'darkgreen'
    params.my$indel.complex <- 'grey'
    
  } else {
    #ascat
    params.my$heatmap.color.gain <- c( alpha('lightgrey',0.10), alpha('olivedrab2', 0.3),  alpha('olivedrab2', 0.5), alpha('olivedrab2',.7), alpha('olivedrab2', 0.75), alpha('olivedrab3', 0.9), alpha('olivedrab4', 0.9))
    params.my$heatmap.ranges.gain <- c(0,2,4,8,16, 32,64,1000)
    
    params.my$heatmap.ranges.loh <- c(0,1,1000)
    params.my$heatmap.color.loh <- c(alpha('lightcoral', 1), alpha('lightgrey',0.10))
    
    params.my$heatmap.key.gain.col <- alpha('olivedrab2', 0.3)
    params.my$heatmap.key.loh.col <- alpha('lightcoral', 1)
    params.my$heatmap.key.gain.title <- 'gain'
    params.my$heatmap.key.loh.title <- 'LOH'
    
    #tumour majorCN
    params.my$heatmap.data.col.gain <- 8
    #tumour minorCN
    params.my$heatmap.data.col.loh <- 7
    
    #Indel colours
    params.my$indel.mhomology <- 'firebrick4'
    params.my$indel.repeatmediated <- 'firebrick1'
    params.my$indel.other <- 'firebrick3'
    params.my$indel.insertion <- 'darkgreen'
    params.my$indel.complex <- 'grey'
  }
  
  return(params.my)
  
}


genomePlot <- function(snvs.file, indels.file, cnvs.file, rearrs.file, 
                       sampleID, genome.v="hg19", ..., plot_title = NULL, 
                       no_copynumber = FALSE, no_rearrangements = FALSE, no_indels = FALSE, out_format = "png", out_path = ".") {
  
  genome.bsgenome = switch(genome.v,
                           "hg19" = BSgenome.Hsapiens.1000genomes.hs37d5::BSgenome.Hsapiens.1000genomes.hs37d5,
                           "hg38" = BSgenome.Hsapiens.UCSC.hg38::BSgenome.Hsapiens.UCSC.hg38)
  
  genome.ideogram = switch(genome.v,
                           "hg19" = "UCSC.HG19.Human.CytoBandIdeogram",
                           "hg38" = "UCSC.HG38.Human.CytoBandIdeogram")
  
  data(list=genome.ideogram, package = "RCircos");
  species.cyto <- get(genome.ideogram);
  
  params.my <- set.plot.params()
  
  # rearrangement links colors
  inv.col <- alpha('dodgerblue2', 1)
  del.col <- alpha('coral2', 1) # originally .5
  dupl.col <-  alpha('darkgreen', 1)
  transloc.colour <- alpha('gray35', 1) # originally 0.5
  
  #Set up height, width and resolution parameters
  cPanelWidth = 0 #0.17
  graph.height = 4100
  graph.wd_ht_ratio = (5400/4100)  #width/height ratio
  graph.width = graph.height * graph.wd_ht_ratio
  graph.wd_res_ratio = (5400/550)
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
  indels <- NULL
  dels.formatted <- data.frame()
  ins.formatted <- data.frame()
  
  indels <- read.table(file = indels.file, sep = '\t', header = TRUE)
  # indels <- read.table(file = '/home/dapregi/tmp/indels.tsv', sep = '\t', header = TRUE)

  if (!no_indels && !is.null(indels)) {
    ins <- indels[which(indels$type=='I'),]
    dels <- indels[which(indels$type=='D' | indels$type=='DI'),]
    dels$color[dels$classification=='Microhomology-mediated'] <- params.my$indel.mhomology
    dels$color[dels$classification=='Repeat-mediated'] <- params.my$indel.repeatmediated
    dels$color[dels$classification=='None'] <- params.my$indel.other
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
  
  RCircos.Set.Core.Components(cyto.info=species.cyto, chr.exclude=NULL,  tracks.inside=params.my$tracks.inside,
                              tracks.outside=params.my$tracks.outside);

  RCircos.Set.Core.Components(cyto.info=species.cyto, chr.exclude=NULL,  tracks.inside=params.my$tracks.inside, tracks.outside=params.my$tracks.outside);
  
  # set plot colours and parameters
  params <- RCircos.Get.Plot.Parameters();
  params$point.type <- params.my$point.type
  params$point.size <- params.my$point.size
  RCircos.Reset.Plot.Parameters(params)
  
  par(mar=c(0.001, 0.001, 0.001, 0.001))
  par(fig=c(cPanelWidth,0.75*(1-cPanelWidth)+cPanelWidth,0,1),cex=1.2)
  par(mai=c(params.my$genomeplot.margin, params.my$genomeplot.margin, params.my$genomeplot.margin, params.my$genomeplot.margin))
  plot.new()
  plot.window(c(-params.my$plot.radius,params.my$plot.radius), c(-params.my$plot.radius, params.my$plot.radius))
  RCircos.Chromosome.Ideogram.Plot.my(params.my$chr.text.color, params.my$grid.line.color, params.my$text.size);
  
  title(main = sampleID)
  
  if (!is.null(plot_title)) {
    title(paste(plot_title, sep=''), line=-1);
  }
  
  # substitutions
  if (exists("subs")) {
    RCircos.Scatter.Plot.color(scatter.data=subs, data.col=6, track.num=1, side="in", by.fold=0, scatter.colors = subs$color);
  }

  # Insertions
  params <- RCircos.Get.Plot.Parameters();
  params$line.color <- 'white'
  params$highlight.width <- 0.2
  params$max.layers <- 5
  params$tile.color <- 'darkgreen'
  RCircos.Reset.Plot.Parameters(params)
  
  if (exists("ins") && nrow(ins)>0) {
    my.RCircos.Tile.Plot(tile.data=ins, track.num=5, side="in");
  }
  
  # Deletions
  params <- RCircos.Get.Plot.Parameters();
  params$tile.color <- 'firebrick4'
  RCircos.Reset.Plot.Parameters(params)
  if (exists("dels") && nrow(dels)>0) {
    my.RCircos.Tile.Plot(tile.data=dels, track.num=6, side="in", tile.colors=dels$color);
  }
  
  
  # Copy number
  if (exists('cv.data') && (nrow(cv.data)>0)) {

    heatmap.ranges.major <-params.my$heatmap.ranges.gain
    heatmap.color.major <-params.my$heatmap.color.gain
    RCircos.Heatmap.Plot.my(heatmap.data=cv.data, data.col=5, track.num=7, side="in", heatmap.ranges=heatmap.ranges.major , heatmap.color=heatmap.color.major ); # major copy number

    heatmap.ranges.minor <-params.my$heatmap.ranges.loh
    heatmap.color.minor <-params.my$heatmap.color.loh
    RCircos.Heatmap.Plot.my(heatmap.data=cv.data, data.col=6, track.num=8, side="in", heatmap.ranges=heatmap.ranges.minor , heatmap.color=heatmap.color.minor ); # minor copy number

  }
  
  # rearrangement
  #Chromosome chromStart  chromEnd Chromosome.1 chromStart.1 chromEnd.1 type
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
  
  dev.off()

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

# genomePlot('/home/dapregi/tmp/snvs.tsv', '/home/dapregi/tmp/indels.tsv', '/home/dapregi/tmp/cnvs.tsv', '/home/dapregi/tmp/rearrs.tsv', 
#            'sampleID', genome.v="hg19", plot_title = '', 
#            no_copynumber = FALSE, no_rearrangements = FALSE, no_indels = FALSE, out_format = "png", out_path = "/home/dapregi/tmp/")