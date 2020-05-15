
################################################################################
#' GA4GHClient methods
#' @include commons.R

#' @description This function implements the OpenCGA calls for managing GA4GH
#' @param OpencgaR an object OpencgaR generated using initOpencgaR and/or opencgaLogin
#' @seealso \url{https://github.com/opencb/opencga/wiki} and the RESTful API documentation
#' \url{http://bioinfo.hpc.cam.ac.uk/opencga/webservices/}
#' @export


setMethod("ga4ghClient", "OpencgaR", function(OpencgaR, file, study, action, params=NULL, ...) {
    category <- "ga4gh"
    switch(action,
        # Endpoint: /{apiVersion}/ga4gh/reads/search

        searchReads=fetchOpenCGA(object=OpencgaR, category=category, categoryId=NULL, subcategory="reads",
                subcategoryId=NULL, action="search", params=params, httpMethod="POST", as.queryParam=NULL, ...),
        # Endpoint: /{apiVersion}/ga4gh/reads/{study}/{file}
        # @param file: File id, name or path.
        # @param study: Study [[user@]project:]study where study and project can be either the ID or UUID.
        # @param referenceName: Reference sequence name (Example: 'chr1', '1' or 'chrX'.
        # @param start: The start position of the range on the reference, 0-based, inclusive.
        # @param end: The end position of the range on the reference, 0-based, exclusive.
        # @param referenceGenome: Reference genome.
        fetchReads=fetchOpenCGA(object=OpencgaR, category=category, categoryId=study, subcategory=NULL,
                subcategoryId=file, action="NULL", params=params, httpMethod="GET", as.queryParam=NULL, ...),
        # Endpoint: /{apiVersion}/ga4gh/responses
        # @param chrom: Chromosome ID. Accepted values: 1-22, X, Y, MT. Note: For compatibility with conventions set by some of the existing beacons, an arbitrary prefix is accepted as well (e.g. chr1 is equivalent to chrom1 and 1).
        # @param pos: Coordinate within a chromosome. Position is a number and is 0-based.
        # @param allele: Any string of nucleotides A,C,T,G or D, I for deletion and insertion, respectively. Note: For compatibility with conventions set by some of the existing beacons, DEL and INS identifiers are also accepted.
        # @param ref: Genome ID. If not specified, all the genomes supported by the given beacons are queried. Note: For compatibility with conventions set by some of the existing beacons, both GRC or HG notation are accepted, case insensitive.
        # @param beacon: Beacon IDs. If specified, only beacons with the given IDs are queried. Responses from all the supported beacons are obtained otherwise. Format: [id1,id2].
        responses=fetchOpenCGA(object=OpencgaR, category=category, categoryId=NULL, subcategory=NULL,
                subcategoryId=NULL, action="responses", params=params, httpMethod="GET",
                as.queryParam=c("chrom","pos","allele","beacon"), ...),
        # Endpoint: /{apiVersion}/ga4gh/variants/search

        searchVariants=fetchOpenCGA(object=OpencgaR, category=category, categoryId=NULL, subcategory="variants",
                subcategoryId=NULL, action="search", params=params, httpMethod="POST", as.queryParam=NULL, ...),
    )
})