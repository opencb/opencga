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

package org.opencb.opencga.server.rest.utils;

import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import org.apache.commons.lang3.time.StopWatch;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.opencga.core.exception.VersionException;
import org.opencb.opencga.core.models.File;
import org.opencb.opencga.server.rest.OpenCGAWSServer;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.*;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Path("/{apiVersion}/utils")
@Produces("application/json")
public class FileRanges extends OpenCGAWSServer {

    public FileRanges(@Context UriInfo uriInfo, @Context HttpServletRequest httpServletRequest, @Context HttpHeaders headerParam) throws IOException, VersionException {
        super(uriInfo, httpServletRequest, headerParam);
    }

    @GET
    @Path("/ranges/{file}")
    @ApiOperation(value = "Fetch alignment files using HTTP Ranges protocol")
    @Produces("text/plain")
    public Response getRanges(@Context HttpHeaders headers,
                              @ApiParam(value = "File id, name or path") @PathParam("file") String fileIdStr,
                              @ApiParam(value = "Study [[user@]project:]study where study and project can be either the id or alias")
                              @QueryParam("study") String studyStr) {

        try {
            DataResult<File> queryResult = catalogManager.getFileManager().get(studyStr, fileIdStr, this.queryOptions, sessionId);
            File file = queryResult.getResults().get(0);

            List<String> rangeList = headers.getRequestHeader("range");
            if (rangeList != null) {
                long from;
                long to;
                String[] acceptedRanges = rangeList.get(0).split("=")[1].split("-");
                from = Long.parseLong(acceptedRanges[0]);
                to = Long.parseLong(acceptedRanges[1]);
                int length = (int) (to - from) + 1;
                ByteBuffer buf = ByteBuffer.allocate(length);

                logger.debug("from: {} , to: {}, length:{}", from, to, length);
                StopWatch t = StopWatch.createStarted();

                java.nio.file.Path filePath = Paths.get(file.getUri());
                try (FileChannel fc = (FileChannel.open(filePath, StandardOpenOption.READ))) {
                    fc.position(from);
                    fc.read(buf);
                }

                t.stop();
                logger.debug("Skip {}B and read {}B in {}s", from, length, t.getTime(TimeUnit.MILLISECONDS) / 1000.0);

                return Response.ok(buf.array(), MediaType.APPLICATION_OCTET_STREAM_TYPE)
                        .header("Accept-Ranges", "bytes")
                        .header("Access-Control-Allow-Origin", "*")
                        .header("Access-Control-Allow-Headers", "x-requested-with, content-type, range")
                        .header("Access-Control-Allow-Credentials", "true")
                        .header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
                        .header("Content-Range", "bytes " + from + "-" + to + "/" + file.getSize())
                        .header("Content-length", to - from + 1)
                        .status(Response.Status.PARTIAL_CONTENT).build();

            } else {
                DataInputStream stream = catalogManager.getFileManager().download(studyStr, fileIdStr, -1, -1, sessionId);
                return createOkResponse(stream, MediaType.APPLICATION_OCTET_STREAM_TYPE, file.getName());
            }
        } catch (Exception e) {
            return createErrorResponse(e);
        }
    }

}
