library(optparse)

#' The BEDPE data fram should contain the following columns: "chrom1", "start1", "end1", "chrom2", "start2", "end2" and "sample" (sample name). 

clustering <- function(sv_bedpe,
                       out_fpath,
                       kmin,
                       kmin.samples,
                       gamma.sdev,
                       PEAK.FACTOR,
                       thresh.dist,
                       gamma,
                       kmin.filter) {
  sv_bedpe <- read.table(args[1], sep = "\t", header = TRUE, stringsAsFactors = FALSE, check.names = FALSE)
  clustering.result <- rearrangement.clustering_bedpe(sv_bedpe=sv_bedpe,
                                                      kmin=kmin,
                                                      kmin.samples=kmin.samples,
                                                      gamma.sdev=gamma.sdev,
                                                      PEAK.FACTOR=PEAK.FACTOR,
                                                      thresh.dist=thresh.dist,
                                                      gamma=gamma,
                                                      kmin.filter=kmin.filter)
  sv_bedpe <- clustering.result$sv_bedpe
  write.table(sv_bedpe, file = out_fpath, row.names = FALSE, sep = "\t", quote = FALSE)
}

calcIntermutDist <- function (subs.type, first.chrom.na = FALSE) {
  
  subs.type.processed <- data.frame()
  for (c in unique(subs.type$chr)) {
    # choose subs from only one chromosome at a time
    
    subs.type.chrom <- subset(subs.type, subset=subs.type$chr==c)
    # sort the subs by position
    subs.type.chrom <- subs.type.chrom [order(subs.type.chrom$position),]
    
    if (first.chrom.na) {
      subs.type.chrom$prevPos <- c(NA,subs.type.chrom$position[1:nrow(subs.type.chrom)-1])
    } else {
      subs.type.chrom$prevPos <- c(0,subs.type.chrom$position[1:nrow(subs.type.chrom)-1])        
    }
    subs.type.chrom$distPrev  <- subs.type.chrom$position -  subs.type.chrom$prevPos
    
    subs.type.processed <- rbind(subs.type.processed,subs.type.chrom)
  }
  
  subs.type.processed$distPrev[subs.type.processed$distPrev==0] <- 1
  subs.type.processed 
}

assignPvalues <- function(kat.regions, chrom.bps, bp.rate=NA) {
  
  if (is.na(bp.rate)) { # estimate the chromosome rate
    left.bp <- min(chrom.bps$pos)
    right.bp <-  max(chrom.bps$pos)
    bp.rate <- nrow(chrom.bps)/ (right.bp - left.bp)
  }
  
  # assume binomial distribution
  kat.regions$pvalue <- 1-pbinom(kat.regions$number.bps, kat.regions$end.bp - kat.regions$start.bp, bp.rate)
  
  kat.regions$d.seg<- (kat.regions$number.bps/( kat.regions$end.bp - kat.regions$start.bp))
  
  kat.regions$rate.factor <- kat.regions$d.seg/bp.rate
  
  kat.regions
}

hotspotInfo <- function(kat.regions.all, subs, segInterDist=c()) {
  if(nrow(kat.regions.all)>0){
    for(r in 1:nrow(kat.regions.all)){
      
      # indices of the breakpoints in the hotspot
      subs.hotspot <-subs[kat.regions.all$firstBp[r]:kat.regions.all$lastBp[r],]
      
      kat.regions.all[r,'start.bp'] <- min(subs.hotspot$pos)
      kat.regions.all[r,'end.bp'] <- max(subs.hotspot$pos)
      kat.regions.all[r,'length.bp'] <-  kat.regions.all[r,'end.bp'] - kat.regions.all[r,'start.bp'] 
      kat.regions.all[r,'number.bps'] <- nrow(subs.hotspot)
      kat.regions.all[r,'number.bps.clustered'] <- sum(subs.hotspot$is.clustered)
      
      if (length(segInterDist)>0 & is.na(kat.regions.all[r,'avgDist.bp'])) {
        kat.regions.all[r,'avgDist.bp'] <- mean(segInterDist[kat.regions.all$firstBp[r]:kat.regions.all$lastBp[r]])
      }
      kat.regions.all[r,'no.samples'] <- length(unique(subs.hotspot$sample))
      
      if ('pf' %in% colnames(subs.hotspot)){
        kat.regions.all[r,'no.del'] <- nrow(subset(subs.hotspot, pf==2))
        kat.regions.all[r,'no.dup'] <- nrow(subset(subs.hotspot, pf==4))
        kat.regions.all[r,'no.inv'] <- nrow(subset(subs.hotspot, pf==1 | pf==8))
        kat.regions.all[r,'no.trn'] <- nrow(subset(subs.hotspot, pf==32))
      }
      
    } # for all peaks
  } # if there is at least one peak
  kat.regions.all
}

extract.kat.regions <- function (res, imd, subs,  kmin.samples=10, pvalue.thresh=1, rate.factor.thresh=1, doMerging=FALSE, kmin.filter=NA, bp.rate=NA) {
  
  segInterDist <-  res$yhat
  kataegis.threshold <- imd
  
  kat.regions.all = data.frame()	
  
  chr <- as.character(subs$chr[1])
  
  positions <- subs$pos
  
  katLoci = (segInterDist<=kataegis.threshold) # flag specifying if a point is in a peak
  
  if(sum(katLoci)>0) {
    
    start.regions = which(katLoci[-1] & !(katLoci[-(length(katLoci))]) # katLoci breakpoints			
                          | (katLoci[-1] & katLoci[-(length(katLoci))] & segInterDist[-1] != segInterDist[-length(katLoci)] )
    )+1 # endpoints between peaks
    if (katLoci[1]) {start.regions <- c(1, start.regions)}
    
    end.regions = which(!(katLoci[-1]) & katLoci[-(length(katLoci))] #
                        | (katLoci[-1] & katLoci[-(length(katLoci))] & segInterDist[-1] != segInterDist[-length(katLoci)] )
    ) #
    if (katLoci[length(katLoci)]) {end.regions <- c( end.regions, length(katLoci))}
    
    start.regions.init <- start.regions
    end.regions.init <- end.regions 
    
    # handling special cases
    if(length(end.regions)+length(start.regions)>0) {  # if there are any discontinuities in the segmentation at all 						
      if (length(end.regions)==1 & length(start.regions)==0){ 
        start.regions <- 1                                    
      } else if (length(start.regions)==1 & length(end.regions)==0){                                    
        end.regions <- length(positions)                                    
      } else if ((end.regions[1]<start.regions[1])&& (start.regions[length(start.regions)]>end.regions[length(end.regions)])) {
        # starts and ends are the same length, but missing both endpoints
        
        start.regions <- c(1,start.regions)
        end.regions <- c(end.regions,  length(positions))
        
      } else if (end.regions[1]<start.regions[1]){
        # starts will be one shorter
        start.regions <- c(1, start.regions)
        
      } else if (start.regions[length(start.regions)]>end.regions[length(end.regions)]){
        # ends will be one shorter
        
        end.regions <- c(end.regions,  length(positions))
      }
      
      if (length(start.regions)!=length(end.regions)) {
        browser()
      }
      
      
      
      # prepare a data structure that will be later filled up
      kat.regions.all <- data.frame(
        chr=subs$chr[1],
        start.bp=rep(NA,length(start.regions)), # start coordinate [bp]
        end.bp=rep(NA,length(start.regions)), # end coordinate [bp]
        length.bp=rep(NA,length(start.regions)), # length [bp]
        number.bps=rep(NA,length(start.regions)),
        number.bps.clustered=rep(NA,length(start.regions)),
        avgDist.bp=rep(NA,length(start.regions)),
        no.samples=rep(NA,length(start.regions)),
        no.del =rep(NA,length(start.regions)),
        no.dup =rep(NA,length(start.regions)),
        no.inv= rep(NA,length(start.regions)),
        no.trn = rep(NA,length(start.regions)),
        firstBp=start.regions,
        lastBp=end.regions                                    )
      
      kat.regions.all <- hotspotInfo(kat.regions.all, subs, segInterDist)
      
      step.segInterDist.left <- rep(NA, length(segInterDist))
      step.segInterDist.left[2:length(segInterDist)] <- segInterDist[2:length(segInterDist)]- segInterDist[1:(length(segInterDist)-1)]       
      step.segInterDist.right <- rep(NA, length(segInterDist))
      step.segInterDist.right[1:(length(segInterDist)-1)] <- segInterDist[1:(length(segInterDist)-1)]- segInterDist[2:(length(segInterDist))]
      
      kat.regions.all$step.left <-  step.segInterDist.left[start.regions]
      kat.regions.all$step.right <-  step.segInterDist.right[end.regions]
      
      
      # run the filters on the regions of increased frequency
      # make sure there are at least kmin samples
      
      if ((!is.null(kat.regions.all)) && (nrow(kat.regions.all)>0)) {
        kat.regions.all <- subset(kat.regions.all, no.samples>=kmin.samples)
      }
      
      
      # make sure there are at least kmin.filter breakpoints
      if (!is.na(kmin.filter)) {
        kat.regions.all <- subset(kat.regions.all, number.bps>=kmin.filter)
      }
      
      
      
      # make sure the p-value is less than somethng
      if ((!is.null(kat.regions.all)) && (nrow(kat.regions.all)>0)) {
        kat.regions.all <- assignPvalues(kat.regions.all, subs, bp.rate=bp.rate)
        kat.regions.all <- subset(kat.regions.all, pvalue<=pvalue.thresh)
        # only keep the hotspots that exceed the theshold
        kat.regions.all <- subset(kat.regions.all, rate.factor>=rate.factor.thresh)
      }  
      
      
      
      
      
      # merge segments if both were found to be peaks
      if (doMerging) {
        if(nrow(kat.regions.all)>1){
          for(r in 2:nrow(kat.regions.all)){
            if (kat.regions.all$lastBp[r-1] == (kat.regions.all$firstBp[r]-1)) {
              # merge two segments
              kat.regions.all$firstBp[r] <- kat.regions.all$firstBp[r-1]
              kat.regions.all$firstBp[r-1] <- NA
              kat.regions.all$lastBp[r-1] <- NA
              kat.regions.all$avgDist.bp[r] <- NA # this will need to be updated as segments are being merged
            }
          }        
        }
        # remove some of the merged segments
        kat.regions.all <- subset(kat.regions.all, !is.na(firstBp) & !is.na(lastBp))
        
        # update the info on hotspots that might have changed when they were merged
        kat.regions.all <- hotspotInfo( kat.regions.all ,  subs, segInterDist)
        kat.regions.all <- assignPvalues(kat.regions.all, subs, bp.rate=bp.rate)
      } # end merging
      
      
      
      
    } # end if there are discontinuities in the segmentation
  } # if there are any points under the inter-mutation distance threshold
  
  kat.regions.all
  
}

#PCF-ALGORITHM (KL):
### EXACT version
exactPcf <- function(y, kmin=5, gamma, yest) {
  ## Implementaion of exact PCF by Potts-filtering
  ## x: input array of (log2) copy numbers
  ## kmin: Mininal length of plateaus
  ## gamma: penalty for each discontinuity
  N <- length(y)
  yhat <- rep(0,N);
  if (N < 2*kmin) {
    if (yest) {
      return(list(Lengde = N, sta = 1, mean = mean(y), nIntervals=1, yhat=rep(mean(y),N)))
    } else {
      return(list(Lengde = N, sta = 1, mean = mean(y), nIntervals=1))
    }
  }
  initSum <- sum(y[1:kmin])
  initKvad <- sum(y[1:kmin]^2)
  initAve <- initSum/kmin;
  bestCost <- rep(0,N)
  bestCost[kmin] <- initKvad - initSum*initAve
  bestSplit <- rep(0,N)
  bestAver <- rep(0,N)
  bestAver[kmin] <- initAve
  Sum <- rep(0,N)
  Kvad <- rep(0,N)
  Aver <- rep(0,N)
  Cost <- rep(0,N)
  kminP1=kmin+1
  for (k in (kminP1):(2*kmin-1)) {
    Sum[kminP1:k]<-Sum[kminP1:k]+y[k]
    Aver[kminP1:k] <- Sum[kminP1:k]/((k-kmin):1)
    Kvad[kminP1:k] <- Kvad[kminP1:k]+y[k]^2
    bestAver[k] <- (initSum+Sum[kminP1])/k
    bestCost[k] <- (initKvad+Kvad[kminP1])-k*bestAver[k]^2
  }
  for (n in (2*kmin):N) {
    yn <- y[n]
    yn2 <- yn^2
    Sum[kminP1:n] <- Sum[kminP1:n]+yn
    Aver[kminP1:n] <- Sum[kminP1:n]/((n-kmin):1)
    Kvad[kminP1:n] <- Kvad[kminP1:n]+yn2
    nMkminP1=n-kmin+1
    Cost[kminP1:nMkminP1] <- bestCost[kmin:(n-kmin)]+Kvad[kminP1:nMkminP1]-Sum[kminP1:nMkminP1]*Aver[kminP1:nMkminP1]+gamma
    Pos <- which.min(Cost[kminP1:nMkminP1])+kmin
    cost <- Cost[Pos]
    aver <- Aver[Pos]
    totAver <- (Sum[kminP1]+initSum)/n
    totCost <- (Kvad[kminP1]+initKvad) - n*totAver*totAver
    
    if (length(totCost)==0 || length(cost)==0) {
      browser()
    }
    if (totCost < cost) {
      Pos <- 1
      cost <- totCost
      aver <- totAver
    }
    bestCost[n] <- cost
    bestAver[n] <- aver
    bestSplit[n] <- Pos-1
  }
  n <- N
  antInt <- 0
  if(yest){
    while (n > 0) {
      yhat[(bestSplit[n]+1):n] <- bestAver[n]
      n <- bestSplit[n]
      antInt <- antInt+1
    }
  } else {
    while (n > 0) {
      n <- bestSplit[n]
      antInt <- antInt+1
    }
  }
  n <- N  #nProbes 
  lengde <- rep(0,antInt)
  start <- rep(0,antInt)
  verdi <- rep(0,antInt)
  oldSplit  <- n
  antall <- antInt
  while (n > 0) {
    start[antall] <- bestSplit[n]+1
    lengde[antall] <- oldSplit-bestSplit[n]
    verdi[antall] <- bestAver[n]
    n <- bestSplit[n]
    oldSplit <- n
    antall <- antall-1
  }
  if (yest) {
    return(list(Lengde = lengde, sta = start, mean = verdi, nIntervals=antInt, yhat=yhat))
  } else {
    return(list(Lengde = lengde, sta = start, mean = verdi, nIntervals=antInt))
  }
}



selectFastPcf <- function(x,kmin,gamma,yest){
  xLength <- length(x)
  if (xLength< 1000) {
    result<-runFastPcf(x,kmin,gamma,0.15,0.15,yest)
  } else {
    if (xLength < 15000){
      result<-runFastPcf(x,kmin,gamma,0.12,0.05,yest)
    } else  {
      result<-runPcfSubset(x,kmin,gamma,0.12,0.05,yest)
    }
  }
  return(result)
}


runFastPcf <- function(x,kmin,gamma,frac1,frac2,yest){
  antGen <- length(x)
  
  L <- min(8, floor(length(x)/6))
  
  mark<-filterMarkS4(x,kmin,L,1,frac1,frac2,0.02,0.9)
  mark[antGen]=TRUE
  dense <- compact(x,mark)
  #print(dense$Nr)
  #print(frac2)
  result<-PottsCompact(kmin,gamma,dense$Nr,dense$Sum,dense$Sq,yest)
  return(result)
}

runPcfSubset <- function(x,kmin,gamma,frac1,frac2,yest){
  SUBSIZE <- 5000
  antGen <- length(x)
  mark<-filterMarkS4(x,kmin,8,1,frac1,frac2,0.02,0.9)
  markInit<-c(mark[1:(SUBSIZE-1)],TRUE)
  compX<-compact(x[1:SUBSIZE],markInit)
  mark2 <- rep(FALSE,antGen)
  mark2[1:SUBSIZE] <- markWithPotts(kmin,gamma,compX$Nr,compX$Sum,compX$Sq,SUBSIZE)
  mark2[4*SUBSIZE/5]<-TRUE
  start <- 4*SUBSIZE/5+1
  while(start + SUBSIZE < antGen){
    slutt<-start+SUBSIZE-1
    markSub<-c(mark2[1:(start-1)],mark[start:slutt])
    markSub[slutt] <- TRUE
    compX<-compact(x[1:slutt],markSub)
    mark2[1:slutt] <- markWithPotts(kmin,gamma,compX$Nr,compX$Sum,compX$Sq,slutt)
    start <- start+4*SUBSIZE/5
    mark2[start-1]<-TRUE
  }
  markSub<-c(mark2[1:(start-1)],mark[start:antGen])
  compX<-compact(x,markSub)
  result <- PottsCompact(kmin,gamma,compX$Nr,compX$Sum,compX$Sq,yest)
  return(result)
}

PottsCompact <- function(kmin, gamma, nr, res, sq, yest) {
  ## Potts filtering on compact array;
  ## kmin: minimal length of plateau
  ## gamma: penalty for discontinuity
  ## nr: number of values between breakpoints
  ## res: sum of values between breakpoints
  ## sq: sum of squares of values between breakpoints
  
  N <- length(nr)
  Ant <- rep(0,N)
  Sum <- rep(0,N)
  Kvad <- rep(0,N)
  Cost <- rep(0,N)
  if (sum(nr) < 2*kmin){
    estim <- list()
    estim$yhat <- rep( sum(res)/sum(nr),sum(nr))
    return(estim)
  }
  initAnt <- nr[1]
  initSum <- res[1]
  initKvad <- sq[1]
  initAve <- initSum/initAnt
  bestCost <- rep(0,N)
  bestCost[1] <- initKvad - initSum*initAve
  bestSplit <- rep(0,N)
  k <- 2
  while(sum(nr[1:k]) < 2*kmin) {
    Ant[2:k] <- Ant[2:k]+nr[k]
    Sum[2:k]<-Sum[2:k]+res[k]
    Kvad[2:k] <- Kvad[2:k]+sq[k]
    bestCost[k] <- (initKvad+Kvad[2])-(initSum+Sum[2])^2/(initAnt+Ant[2])
    k <- k+1	
  }
  for (n in k:N) {
    Ant[2:n] <- Ant[2:n]+nr[n]
    Sum[2:n] <- Sum[2:n]+res[n]
    Kvad[2:n] <- Kvad[2:n]+sq[n]
    limit <- n
    while(limit > 2 & Ant[limit] < kmin) {limit <- limit-1}
    Cost[2:limit] <- bestCost[1:limit-1]+Kvad[2:limit]-Sum[2:limit]^2/Ant[2:limit]
    Pos <- which.min(Cost[2:limit])+ 1
    cost <- Cost[Pos]+gamma
    totCost <- (Kvad[2]+initKvad) - (Sum[2]+initSum)^2/(Ant[2]+initAnt)
    if (totCost < cost) {
      Pos <- 1
      cost <- totCost
    }
    bestCost[n] <- cost
    bestSplit[n] <- Pos-1
  }
  
  if (yest) {
    yhat<-rep(0,N)
    res<-findEst(bestSplit,N,nr,res,TRUE)
  } else {
    res<-findEst(bestSplit,N,nr,res,FALSE)
  }
  return(res)
}

compact <- function(y,mark){
  ## accumulates numbers of observations, sums and 
  ## sums of squares between potential breakpoints
  N <- length(y)
  tell<-seq(1:N)
  cCTell<-tell[mark]
  Ncomp<-length(cCTell)
  lowTell<-c(0,cCTell[1:(Ncomp-1)])
  ant<-cCTell-lowTell
  cy<-cumsum(y)
  cCcy<-cy[mark]
  lowcy<-c(0,cCcy[1:(Ncomp-1)])
  sum<-cCcy-lowcy
  y2<-y^2
  cy2<-cumsum(y2)
  cCcy2<-cy2[mark]
  lowcy2<-c(0,cCcy2[1:(Ncomp-1)])
  sq<-cCcy2-lowcy2
  return(list(Nr=ant,Sum=sum,Sq=sq))
}

findEst <- function(bestSplit,N,Nr,Sum,yest){
  n<-N
  lengde<-rep(0,N)
  antInt<-0
  while (n>0){
    antInt<-antInt+1
    lengde[antInt] <- n-bestSplit[n]
    n<-bestSplit[n]
  }
  lengde<-lengde[antInt:1]
  lengdeOrig<-rep(0,antInt)
  startOrig<-rep(1,antInt+1)
  verdi<-rep(0,antInt)
  start<-rep(1,antInt+1)
  for(i in 1:antInt){
    start[i+1] <- start[i]+lengde[i]
    lengdeOrig[i] <- sum(Nr[start[i]:(start[i+1]-1)])
    startOrig[i+1] <- startOrig[i]+lengdeOrig[i]
    verdi[i]<-sum(Sum[start[i]:(start[i+1]-1)])/lengdeOrig[i]
  }
  
  if(yest){
    yhat<-rep(0,startOrig[antInt+1]-1)
    for (i in 1:antInt){
      yhat[startOrig[i]:(startOrig[i+1]-1)]<-verdi[i]
    }
    startOrig<-startOrig[1:antInt]
    return(list(Lengde=lengdeOrig,sta=startOrig,mean=verdi,nIntervals=antInt,yhat=yhat))
  } else {
    startOrig<-startOrig[1:antInt]
    return(list(Lengde=lengdeOrig,sta=startOrig,mean=verdi,nIntervals=antInt))
  }
  
}


markWithPotts <- function(kmin, gamma, nr, res, sq, subsize) {
  ## Potts filtering on compact array;
  ## kmin: minimal length of plateau
  ## gamma: penalty for discontinuity
  ## nr: number of values between breakpoints
  ## res: sum of values between breakpoints
  ## sq: sum of squares of values between breakpoints
  
  N <- length(nr)
  Ant <- rep(0,N)
  Sum <- rep(0,N)
  Kvad <- rep(0,N)
  Cost <- rep(0,N)
  markSub <- rep(FALSE,N)
  initAnt <- nr[1]
  initSum <- res[1]
  initKvad <- sq[1]
  initAve <- initSum/initAnt
  bestCost <- rep(0,N)
  bestCost[1] <- initKvad - initSum*initAve
  bestSplit <- rep(0,N)
  k <- 2
  while(sum(nr[1:k]) < 2*kmin) {
    Ant[2:k] <- Ant[2:k]+nr[k]
    Sum[2:k]<-Sum[2:k]+res[k]
    Kvad[2:k] <- Kvad[2:k]+sq[k]
    bestCost[k] <- (initKvad+Kvad[2])-(initSum+Sum[2])^2/(initAnt+Ant[2])
    k <- k+1	
  }
  for (n in k:N) {
    Ant[2:n] <- Ant[2:n]+nr[n]
    Sum[2:n] <- Sum[2:n]+res[n]
    Kvad[2:n] <- Kvad[2:n]+sq[n]
    limit <- n
    while(limit > 2 & Ant[limit] < kmin) {limit <- limit-1}
    Cost[2:limit] <- bestCost[1:limit-1]+Kvad[2:limit]-Sum[2:limit]^2/Ant[2:limit]
    Pos <- which.min(Cost[2:limit])+ 1
    cost <- Cost[Pos]+gamma
    totCost <- (Kvad[2]+initKvad) - (Sum[2]+initSum)^2/(Ant[2]+initAnt)
    if (totCost < cost) {
      Pos <- 1
      cost <- totCost
    }
    bestCost[n] <- cost
    bestSplit[n] <- Pos-1
    markSub[Pos-1] <- TRUE
  }
  help<-findMarks(markSub,nr,subsize)
  return(help=help)
}


findMarks <- function(markSub,Nr,subsize){
  ## markSub: marks in compressed scale
  ## NR: number of observations between potenstial breakpoints
  mark<-rep(FALSE,subsize)  ## marks in original scale
  if(sum(markSub)<1) {return(mark)} else {	
    N<-length(markSub)
    ant <- seq(1:N)
    help <- ant[markSub]
    lengdeHelp<-length(help)
    help0 <- c(0,help[1:(lengdeHelp-1)])
    lengde <- help-help0
    start<-1
    oldStart<-1
    startOrig<-1
    for(i in 1:lengdeHelp){
      start <- start+lengde[i]
      lengdeOrig <- sum(Nr[oldStart:(start-1)])
      startOrig <- startOrig+lengdeOrig
      mark[startOrig-1]<-TRUE
      oldStart<-start
    }
    return(mark)
  }
  
}


compact <- function(y,mark){
  ## accumulates numbers of observations, sums and 
  ## sums of squares between potential breakpoints
  ## y:  array to be compacted
  ## mark:  logical array of potential breakpoints
  tell<-seq(1:length(y))
  cCTell<-tell[mark]
  Ncomp<-length(cCTell)
  lowTell<-c(0,cCTell[1:(Ncomp-1)])
  ant<-cCTell-lowTell
  cy<-cumsum(y)
  cCcy<-cy[mark]
  lowcy<-c(0,cCcy[1:(Ncomp-1)])
  sum<-cCcy-lowcy
  cy2<-cumsum(y^2)
  cCcy2<-cy2[mark]
  lowcy2<-c(0,cCcy2[1:(Ncomp-1)])
  sq<-cCcy2-lowcy2
  return(list(Nr=ant,Sum=sum,Sq=sq))
}

filterMarkS4 <- function(x,kmin,L,L2,frac1,frac2,frac3,thres){
  ## marks potential breakpoints, partially by a two 6*L and 6*L2 highpass
  ## filters (L>L2), then by a filter seaching for potential kmin long segments
  lengdeArr <- length(x)
  xc<-cumsum(x)
  xc<-c(0,xc)
  ind11<-1:(lengdeArr-6*L+1)
  ind12<-ind11+L
  ind13<-ind11+3*L
  ind14<-ind11+5*L
  ind15<-ind11+6*L
  
  cost1<-abs(4*xc[ind13]-xc[ind11]-xc[ind12]-xc[ind14]-xc[ind15])	
  cost1<-c(rep(0,3*L-1),cost1,rep(0,3*L))
  ##mark shortening in here
  in1<-1:(lengdeArr-6)
  in2<-in1+1
  in3<-in1+2
  in4<-in1+3
  in5<-in1+4
  in6<-in1+5
  in7<-in1+6
  test<-pmax(cost1[in1],cost1[in2],cost1[in3],cost1[in4],cost1[in5],cost1[in6],cost1[in7])
  test<-c(rep(0,3),test,rep(0,3))
  cost1B<-cost1[cost1>=thres*test]
  frac1B<-min(0.8,frac1*length(cost1)/length(cost1B))
  limit <- quantile(cost1B,(1-frac1B),names=FALSE)
  mark<-(cost1>limit)&(cost1>0.9*test)	
  
  
  ind21<-1:(lengdeArr-6*L2+1)
  ind22<-ind21+L2
  ind23<-ind21+3*L2
  ind24<-ind21+5*L2
  ind25<-ind21+6*L2
  cost2<-abs(4*xc[ind23]-xc[ind21]-xc[ind22]-xc[ind24]-xc[ind25])
  limit2 <- quantile(cost2,(1-frac2),names=FALSE)
  mark2<-(cost2>limit2)
  mark2<-c(rep(0,3*L2-1),mark2,rep(0,3*L2))
  if(3*L>kmin){
    mark[kmin:(3*L-1)]<-TRUE
    mark[(lengdeArr-3*L+1):(lengdeArr-kmin)]<-TRUE
  }
  else
  {
    mark[kmin]<- TRUE
    mark[lengdeArr-kmin]<-TRUE
  }
  
  if((kmin>1)&&(length(lengdeArr)>(3*kmin+1))){
    ind1<-1:(lengdeArr-3*kmin+1)
    ind2<-ind1+3*kmin
    ind3<-ind1+kmin
    ind4<-ind1+2*kmin
    shortAb <- abs(3*(xc[ind4]-xc[ind3])-(xc[ind2]-xc[ind1]))
    in1<-1:(length(shortAb)-6)
    in2<-in1+1
    in3<-in1+2
    in4<-in1+3
    in5<-in1+4
    in6<-in1+5
    in7<-in1+6
    test<-pmax(shortAb[in1],shortAb[in2],shortAb[in3],shortAb[in4],shortAb[in5],shortAb[in6],shortAb[in7])
    test<-c(rep(0,3),test,rep(0,3))
    cost1C<-shortAb[shortAb>=thres*test]
    frac1C<-min(0.8,frac3*length(shortAb)/length(cost1C))
    limit3 <- quantile(cost1C,(1-frac1C),names=FALSE)
    markH1<-(shortAb>limit3)&(shortAb>thres*test)
    markH2<-c(rep(FALSE,(kmin-1)),markH1,rep(FALSE,2*kmin))
    markH3<-c(rep(FALSE,(2*kmin-1)),markH1,rep(FALSE,kmin))
    mark<-mark|mark2|markH2|markH3
  } else {
    mark<-mark|mark2
  }
  
  if(3*L>kmin){
    mark[1:(kmin-1)]<-FALSE
    mark[kmin:(3*L-1)]<-TRUE
    mark[(lengdeArr-3*L+1):(lengdeArr-kmin)]<-TRUE
    mark[(lengdeArr-kmin+1):(lengdeArr-1)]<-FALSE
    mark[lengdeArr]<-TRUE	
  }
  else
  {
    mark[1:(kmin-1)]<-FALSE
    mark[(lengdeArr-kmin+1):(lengdeArr-1)]<-FALSE
    mark[lengdeArr]<-TRUE
    mark[kmin]<- TRUE
    mark[lengdeArr-kmin]<-TRUE
  }
  
  return(mark)
}

medianFilter <- function(x,k){
  n <- length(x)
  filtWidth <- 2*k + 1
  
  #Make sure filtWidth does not exceed n
  if(filtWidth > n){
    if(n==0){
      filtWidth <- 1
    }else if(n%%2 == 0){
      #runmed requires filtWidth to be odd, ensure this:
      filtWidth <- n - 1
    }else{
      filtWidth <- n
    }
  }
  
  runMedian <- runmed(x,k=filtWidth,endrule="median")
  
  return(runMedian)
}

getMad <- function(x,k=25){
  
  #Remove observations that are equal to zero; are likely to be imputed, should not contribute to sd:
  x <- x[x!=0]
  
  #Calculate runMedian  
  runMedian <- medianFilter(x,k)
  
  dif <- x-runMedian
  SD <- mad(dif)
  
  return(SD)
}

rearrangement.clustering_bedpe <- function(sv_bedpe,
                                           kmin=10,# how many points at minimum in a peak, for the pcf algorithm
                                           kmin.samples=kmin, # how many different samples at minimum in  a peak
                                           gamma.sdev=25,
                                           PEAK.FACTOR=4,
                                           thresh.dist=NA,
                                           gamma=NA,
                                           kmin.filter=kmin # if the pcf parameter is different from the definition of a peak
) { 
  
  #add an id to the rearrangement
  sv_bedpe$id <- 1:nrow(sv_bedpe)
  
  #functions below expect rows to be organised by chromosomes and ordered by position on the chromosome
  
  #prepare a dataframe for the calculation
  rearrs.left <- sv_bedpe[,c('chrom1','start1','sample')]
  names(rearrs.left ) <- NA
  rearrs.right <- sv_bedpe[,c('chrom2','start2','sample')]
  names(rearrs.right ) <- NA
  rearrs.cncd <- rbind(rearrs.left , rearrs.right  )
  colnames(rearrs.cncd) <- c('chr', 'position', 'sample')
  rearrs.cncd$isLeft <- c(rep(TRUE, nrow(rearrs.left)), rep(FALSE, nrow(rearrs.left)))
  rearrs.cncd$id <- c(sv_bedpe$id, sv_bedpe$id)
  # sample.bps <- rearrs.cncd
  #need to reorder
  sample.bps <- NULL
  for (chrom_i in unique(rearrs.cncd$chr)){
    tmptab <- rearrs.cncd[rearrs.cncd$chr==chrom_i,,drop=FALSE]
    tmptab <- tmptab[order(tmptab$position),,drop=FALSE]
    sample.bps <- rbind(sample.bps,tmptab)
  }
  rownames(sample.bps) <- 1:nrow(sample.bps)
  
  #run the algorithm
  genome.size <- 3 * 10^9
  MIN.BPS <- 10 # minimal number of breakpoints on a chromosome to do any any segmentation
  
  logScale <- FALSE   
  
  exp.dist <-genome.size/nrow(sample.bps)
  
  if (logScale) {
    sample.bps$intermut.dist <- log10(calcIntermutDist(sample.bps, first.chrom.na=FALSE)$distPrev) # calculate the distances between the breakpoints
    if (is.na(thresh.dist)) {
      thresh.dist <- log10(exp.dist/PEAK.FACTOR) # calculate the threshold to call a peak
    }
  } else {
    
    sample.bps$intermut.dist <- calcIntermutDist(sample.bps, first.chrom.na=FALSE)$distPrev
    if (is.na(thresh.dist)) {
      thresh.dist <- exp.dist/PEAK.FACTOR
    }
  }
  
  
  if (is.na(gamma) & !is.na(gamma.sdev)) {
    # compute the mean absolute deviation
    sdev <- getMad(sample.bps$intermut.dist);
    gamma <- gamma.sdev*sdev
  }
  
  
  
  sample.bps$is.clustered.single <- rep(FALSE, nrow(sample.bps))
  
  all.kat.regions <- data.frame()
  
  for (chrom in unique(sample.bps$chr)) { # loop over chromosomes     
    
    sample.bps.flag <- sample.bps$chr==chrom #   breakpoints on a current chromosome
    # sample.bps.chrom <- sample.bps[sample.bps.flag,]
    # sample.bps.chrom <- sample.bps.chrom[order(sample.bps.chrom$position),]
    # 
    if (sum(sample.bps.flag )>MIN.BPS ) { # if there are enough breakpoints on a chromosome to run pcf
      
      data.points <- sample.bps$intermut.dist[sample.bps.flag]
      # data.points <- sample.bps.chrom$intermut.dist
      
      res = exactPcf(data.points, kmin, gamma, T)
      
      #reorder results
      sample.bps$mean.intermut.dist[sample.bps.flag] <- res$yhat
      
      # prepare the points for pcf
      subs <- data.frame(chr=sample.bps$chr[sample.bps.flag], pos=sample.bps$position[sample.bps.flag], sample=sample.bps$sample[sample.bps.flag])
      kat.regions <- extract.kat.regions(res, thresh.dist, subs, doMerging=TRUE, kmin.samples=1,  kmin.filter= kmin.filter) # extract peaks, this is special case as we want at least kmin samples
      
      all.kat.regions <- rbind(all.kat.regions, kat.regions)
      if (!is.null(kat.regions) && nrow( kat.regions )>0) { # if there are any kataegis regions found on this chormosome
        for (k in 1:nrow(kat.regions)) {
          
          sample.bps$is.clustered.single[which(sample.bps.flag)[ kat.regions$firstBp[k] : kat.regions$lastBp[k]]] <- TRUE # call all breakpoints as clustered     
        }                
      }            
    } else {
      
      sample.bps$mean.intermut.dist[sample.bps.flag] <- mean(sample.bps$intermut.dist[sample.bps.flag])           
    }        
  }
  
  
  
  if (!logScale) { # even if pcf was run on non-logged distances, I log the output
    sample.bps$intermut.dist <- log10(sample.bps$intermut.dist)
    sample.bps$mean.intermut.dist <- log10(sample.bps$mean.intermut.dist)
  }
  
  # a rearrangement is in a cluster if any of its breakpoints are
  sample.bps$is.clustered <- sample.bps$is.clustered.single
  sample.bps$is.clustered[sample.bps$id %in% subset(sample.bps, is.clustered.single==TRUE)$id] <- TRUE
  
  # mark both breakpoints of a rearrangement as clustered if any is
  sv_bedpe$is.clustered <- sv_bedpe$id %in% sample.bps$id[sample.bps$is.clustered]
  
  result <- list()
  result$sv_bedpe <- sv_bedpe
  result$kat.regions <- all.kat.regions
  result
}


option_list <- list(
  make_option(c("--kmin"), type="integer", default=10, help="How many points at minimum in a peak, for the pcf algorithm"),
  make_option(c("--kmin_samples"), type="integer", default=1, help="How many different samples at minimum in a peak"),
  make_option(c("--gamma_sdev"), type="integer", default=25, help="Gamma standard deviation"),
  make_option(c("--peak_factor"), type="integer", default=10, help="Peak factor"),
  make_option(c("--thresh_dist"), type="integer", default=NA, help="Threshold distance"),
  make_option(c("--gamma"), type="integer", default=NA, help="Gamma"),
  make_option(c("--kmin_filter"), type="integer", default=10, help="Kmin filter")
)
parser <- OptionParser(usage = "%prog [options] sv_bedpe out_fpath", option_list=option_list)
arguments <- parse_args(parser, positional_arguments = 2)
opt <- arguments$options
args <- arguments$args

clustering(args[1],
           args[2],
           kmin=opt$kmin,
           kmin.samples=opt$kmin_samples,
           gamma.sdev=opt$gamma_sdev,
           PEAK.FACTOR=opt$peak_factor,
           thresh.dist=opt$thresh_dist,
           gamma=opt$gamma,
           kmin.filter=opt$kmin_filter)
