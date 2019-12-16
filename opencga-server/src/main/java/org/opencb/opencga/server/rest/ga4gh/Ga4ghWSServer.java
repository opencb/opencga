/*
 * Copyright 2015-2017 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.server.rest.ga4gh;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import org.apache.solr.common.StringUtils;
import org.ga4gh.methods.SearchReadsRequest;
import org.ga4gh.methods.SearchReadsResponse;
import org.ga4gh.methods.SearchVariantsRequest;
import org.ga4gh.methods.SearchVariantsResponse;
import org.ga4gh.models.ReadAlignment;
import org.ga4gh.models.Variant;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.tools.alignment.BamManager;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.utils.ListUtils;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.FileManager;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.exception.VersionException;
import org.opencb.opencga.core.models.File;
import org.opencb.opencga.server.rest.OpenCGAWSServer;
import org.opencb.opencga.storage.core.alignment.AlignmentDBAdaptor;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.analysis.alignment.AlignmentStorageManager;
import org.opencb.opencga.analysis.variant.manager.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.BeaconResponse;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

import static org.opencb.opencga.core.api.ParamConstants.REGION_CONTAINED_PARAM;
import static org.opencb.opencga.core.api.ParamConstants.REGION_PARAM;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.*;

/**
 * Created on 09/10/15
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */

@Path("/{apiVersion}/ga4gh")
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "GA4GH", position = 13, description = "Global Alliance for Genomics & Health RESTful API")
public class Ga4ghWSServer extends OpenCGAWSServer {

    public Ga4ghWSServer(@Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest, @Context HttpHeaders httpHeaders) throws IOException, VersionException {
        super(uriInfo, httpServletRequest, httpHeaders);
    }

    /* =================    BEACON     ===================*/
    @GET
    @Path("/responses")
    @ApiOperation(value = "Beacon webservice", position = 1)
    public Response getBeacon(
            @ApiParam(value = "Chromosome ID. Accepted values: 1-22, X, Y, MT. Note: For compatibility with conventions set by some of "
                    + "the existing beacons, an arbitrary prefix is accepted as well (e.g. chr1 is equivalent to chrom1 and 1).",
                    required = true) @QueryParam("chrom") String chrom,
            @ApiParam(value = "Coordinate within a chromosome. Position is a number and is 0-based.", required = true) @QueryParam("pos")
                    int pos,
            @ApiParam(value = "Any string of nucleotides A,C,T,G or D, I for deletion and insertion, respectively. Note: For compatibility"
                    + " with conventions set by some of the existing beacons, DEL and INS identifiers are also accepted.", required = true)
                @QueryParam("allele") String allele,
            @ApiParam(value = "Genome ID. If not specified, all the genomes supported by the given beacons are queried. Note: For "
                    + "compatibility with conventions set by some of the existing beacons, both GRC or HG notation are accepted, case "
                    + "insensitive.") @QueryParam("ref") String ref,
            @ApiParam(value = "Beacon IDs. If specified, only beacons with the given IDs are queried. Responses from all the supported "
                    + "beacons are obtained otherwise. Format: [id1,id2].", required = true) @QueryParam("beacon") String beaconsList)
            throws CatalogException, IOException, StorageEngineException {

        if (StringUtils.isEmpty(chrom) || StringUtils.isEmpty(allele) || StringUtils.isEmpty(beaconsList)) {
            return createErrorResponse("Beacon response", "Missing at least one of chromosome, position, allele and "
                    + "beacon parameters. All of them are mandatory.");
        }
        try {
            BeaconResponse.Query beaconQuery = new BeaconResponse.Query(chrom, pos, allele, ref);
            List<BeaconResponse> responses = variantManager.beacon(beaconsList, beaconQuery, token);
            return Response.ok(responses, MediaType.APPLICATION_JSON).build();
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    /* =================    VARIANTS     ===================*/

    @POST
    @Path("/variants/search")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Description", position = 1, notes = "Notes")
    public Response searchVariants(SearchVariantsRequest request) {
        String method = "ga4gh/variants/search";
        try {

            if (request.getVariantSetId() == null || request.getVariantSetId().isEmpty()) {
                return createErrorResponse(method, "Required referenceName or referenceId");
            }
            QueryOptions queryOptions = new QueryOptions(uriInfo.getQueryParameters(), true);
            queryOptions.append(STUDY.key(), request.getVariantSetId());

//        queryOptions.append(, request.getVariantName()); //TODO
            if (request.getCallSetIds() != null) {
                queryOptions.append(INCLUDE_SAMPLE.key(), request.getCallSetIds());
            }

            CharSequence chr = null;
            if (request.getReferenceName() != null) {
                chr = request.getReferenceName();
            }
            if (chr == null) {
                return createErrorResponse(method, "Required referenceName or referenceId");
            }
            if (request.getStart() == null || request.getStart() < 0) {
                return createErrorResponse(method, "Required start position");
            }
            if (request.getEnd() == null || request.getEnd() < 0) {
                return createErrorResponse(method, "Required end position");
            }
            long delta = request.getEnd() - request.getStart();
            if (delta < 0/* || delta > 20000*/) {
                return createErrorResponse(method, "End must be behind the start");
            }
            queryOptions.append(REGION.key(), new Region(chr.toString(), request.getStart().intValue(), request.getEnd().intValue()));

            if (request.getPageSize() == null || request.getPageSize() <= 0 || request.getPageSize() > 4000) {
                this.queryOptions.add(QueryOptions.LIMIT, 1000);
            } else {
                this.queryOptions.add(QueryOptions.LIMIT, request.getPageSize());
            }

            int page = 0;
            if (request.getPageToken() != null) {
                try {
                    page = Integer.parseInt(request.getPageToken().toString());
                    this.queryOptions.put(QueryOptions.SKIP, this.queryOptions.getInt(QueryOptions.LIMIT) * page);
                } catch (Exception e) {
                    return createErrorResponse(method, "Invalid page token \"" + request.getPageToken() + "\"");
                }
            }
            // Get all query options
            SearchVariantsResponse response = new SearchVariantsResponse();
            Query query = VariantStorageManager.getVariantQuery(queryOptions);

            List<Variant> variants = variantManager.get(query, queryOptions, token, Variant.class).getResults();
            response.setNextPageToken(Integer.toString(++page));
            response.setVariants(variants);
            return buildResponse(Response.ok(response.toString(), MediaType.APPLICATION_JSON_TYPE));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

    /* =================    ALIGNMENTS     ===================*/

    @GET
    @Path("/reads/{study}/{file}")
    @ApiOperation(value = "Fetch alignment files using HTSget protocol")
    @Produces("text/plain")
    public Response getHtsget(
            @Context HttpHeaders headers,
            @ApiParam(value = "File id, name or path") @PathParam("file") String fileIdStr,
            @ApiParam(value = ParamConstants.STUDY_DESCRIPTION) @PathParam(ParamConstants.STUDY_PARAM) String studyStr,
            @ApiParam(value = "Reference sequence name (Example: 'chr1', '1' or 'chrX'") @QueryParam("referenceName") String reference,
            @ApiParam(value = "The start position of the range on the reference, 0-based, inclusive.") @QueryParam("start") int start,
            @ApiParam(value = "The end position of the range on the reference, 0-based, exclusive.") @QueryParam("end") int end,
            @ApiParam(value = "Reference genome.") @QueryParam("referenceGenome") String referenceGenome) {
        try {
            File file = catalogManager.getFileManager().get(studyStr, fileIdStr, FileManager.EXCLUDE_FILE_ATTRIBUTES, token).first();
            java.nio.file.Path referencePath = null;

            if (file.getFormat() == File.Format.CRAM) {
                if (org.apache.commons.lang3.StringUtils.isNotEmpty(referenceGenome)) {
                    referencePath = Paths.get(catalogManager.getFileManager().get(studyStr, referenceGenome, FileManager.INCLUDE_FILE_URI_PATH, token).first().getUri().getPath());
                } else if (ListUtils.isNotEmpty(file.getRelatedFiles())) {
                    for (File.RelatedFile relatedFile : file.getRelatedFiles()) {
                        if (relatedFile.getRelation() == File.RelatedFile.Relation.REFERENCE_GENOME) {
                            referencePath = Paths.get(relatedFile.getFile().getUri().getPath());
                            break;
                        }
                    }

                    if (referencePath == null) {
                        // Look for a reference genome in the study
                        Query referenceQuery = new Query(FileDBAdaptor.QueryParams.BIOFORMAT.key(), File.Bioformat.REFERENCE_GENOME);
                        DataResult<File> fileDataResult = catalogManager.getFileManager().search(studyStr, referenceQuery, FileManager.INCLUDE_FILE_URI_PATH, token);
                        if (fileDataResult.getNumResults() == 0 || fileDataResult.getNumResults() > 1) {
                            throw new CatalogException("Missing referenceGenome field for CRAM file");
                        }
                        referencePath = Paths.get(fileDataResult.first().getUri().getPath());
                    }
                }
            }
            try (BamManager bamManager = new BamManager(Paths.get(file.getUri().getPath()), referencePath)) {
                List<String> chunkOffsetList = bamManager.getBreakpoints(new Region(reference, start, end));

                String url = uriInfo.getBaseUri().toString() + apiVersion + "/utils/ranges/" + fileIdStr + "?study=" + studyStr;
                List<ObjectMap> urls = new ArrayList<>(chunkOffsetList.size() + 2);

                // Add header
                urls.add(new ObjectMap()
                        .append("url", "data:application/octet-stream;base64," + Base64.getEncoder().encodeToString(bamManager.compressedHeader()))
                        .append("class", "header"));

                // Add urls to ranges
                for (String byteRange : chunkOffsetList) {
                    urls.add(new ObjectMap()
                            .append("url", url)
                            .append("headers", new ObjectMap()
                                    .append("Authorization", "Bearer " + token)
                                    .append("Range", "bytes=" + byteRange)
                            )
                            .append("class", "body")
                    );
                }

                // Add EOF marker
                urls.add(new ObjectMap("url", "data:application/octet-stream;base64,H4sIBAAAAAAA/wYAQkMCABsAAwAAAAAAAAAAAA=="));

                ObjectMap result = new ObjectMap("htsget", new ObjectMap()
                        .append("format", file.getFormat())
                        .append("urls", urls)
                );
                return createRawOkResponse(result);
            }
        } catch (CatalogException | IOException e) {
            return createErrorResponse(e);
        }
    }

    @POST
    @Path("/reads/search")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Description", position = 1, notes = "Notes")
    public Response searchAlignments(SearchReadsRequest request) {
        String method = "ga4gh/reads/search";
        try {
            if (request.getReadGroupIds() == null || request.getReadGroupIds().size() == 0) {
                return createErrorResponse(method, "Required at least one group id.");
            }

            if (request.getReadGroupIds().size() > 1) {
                return createErrorResponse(method, "Several read group ids yet not supported.");
            }

            if (request.getReferenceId() == null || request.getReferenceId().isEmpty()) {
                return createErrorResponse(method, "Required reference id");
            }

            if (request.getStart() == null || request.getStart() <= 0)  {
                return createErrorResponse(method, "Required start position");
            }

            if (request.getEnd() == null || request.getEnd() <= 0)  {
                return createErrorResponse(method, "Required end position");
            }

            Query query = new Query();
            query.put(REGION_PARAM,
                    request.getReferenceId() + ":" + request.getStart().intValue() + "-" + request.getEnd().intValue());

            this.queryOptions.put(REGION_CONTAINED_PARAM, true);

            if (request.getPageSize() == null || request.getPageSize() <= 0 || request.getPageSize() > 4000) {
                this.queryOptions.put(QueryOptions.LIMIT, 1000);
            } else {
                this.queryOptions.put(QueryOptions.LIMIT, request.getPageSize());
            }

            int page = 0;
            if (request.getPageToken() != null) {
                try {
                    page = Integer.parseInt(request.getPageToken().toString());
                    this.queryOptions.put(QueryOptions.SKIP,
                            this.queryOptions.getInt(QueryOptions.LIMIT) * page);
                } catch (Exception e) {
                    return createErrorResponse(method, "Invalid page token \"" + request.getPageToken() + "\"");
                }
            }

            SearchReadsResponse response = new SearchReadsResponse();

            AlignmentStorageManager alignmentStorageManager = new AlignmentStorageManager(catalogManager, storageEngineFactory);

            DataResult<ReadAlignment> queryResult = alignmentStorageManager
                    .query("", request.getReadGroupIds().get(0), query, queryOptions, token);

            response.setAlignments(queryResult.getResults());
            response.setNextPageToken(Integer.toString(++page));

            return buildResponse(Response.ok(response.toString(), MediaType.APPLICATION_JSON_TYPE));
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

}
