/*
* Copyright 2015-2020 OpenCB
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

package org.opencb.opencga.client.rest;

import java.io.DataInputStream;
import org.opencb.commons.datastore.core.FacetField;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.client.config.ClientConfiguration;
import org.opencb.opencga.client.exceptions.ClientException;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.file.FileAclUpdateParams;
import org.opencb.opencga.core.models.file.FileCreateParams;
import org.opencb.opencga.core.models.file.FileLinkParams;
import org.opencb.opencga.core.models.file.FileTree;
import org.opencb.opencga.core.models.file.FileUpdateParams;
import org.opencb.opencga.core.models.job.Job;
import org.opencb.opencga.core.response.RestResponse;


/**
 * This class contains methods for the File webservices.
 *    Client version: 2.0.0
 *    PATH: files
 */
public class FileClient extends AbstractParentClient {

    public FileClient(String token, ClientConfiguration configuration) {
        super(token, configuration);
    }

    /**
     * Link an external file into catalog.
     * @param data File parameters.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<File> link(FileLinkParams data, ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        params.put("body", data);
        return execute("files", null, null, null, "link", params, POST, File.class);
    }

    /**
     * List of accepted file formats.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<File.Format> formats() throws ClientException {
        ObjectMap params = new ObjectMap();
        return execute("files", null, null, null, "formats", params, GET, File.Format.class);
    }

    /**
     * Refresh metadata from the selected file or folder. Return updated files.
     * @param file File id, name or path. Paths must be separated by : instead of /.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<File> refresh(String file, ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        return execute("files", file, null, null, "refresh", params, GET, File.class);
    }

    /**
     * Delete existing files and folders.
     * @param files Comma separated list of file ids, names or paths.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<Job> delete(String files, ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        return execute("files", files, null, null, "delete", params, DELETE, Job.class);
    }

    /**
     * List all the files inside the folder.
     * @param folder Folder id, name or path.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<File> list(String folder, ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        return execute("files", folder, null, null, "list", params, GET, File.class);
    }

    /**
     * File search method.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<File> search(ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        return execute("files", null, null, null, "search", params, GET, File.class);
    }

    /**
     * Unlink linked files and folders.
     * @param files Comma separated list of file ids, names or paths.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<Job> unlink(String files, ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        return execute("files", files, null, null, "unlink", params, DELETE, Job.class);
    }

    /**
     * File info.
     * @param files Comma separated list of file ids or names up to a maximum of 100.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<File> info(String files, ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        return execute("files", files, null, null, "info", params, GET, File.class);
    }

    /**
     * Show the content of a file (up to a limit).
     * @param file File id, name or path. Paths must be separated by : instead of /.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<String> content(String file, ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        return execute("files", file, null, null, "content", params, GET, String.class);
    }

    /**
     * Obtain a tree view of the files and folders within a folder.
     * @param folder Folder id, name or path. Paths must be separated by : instead of /.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<FileTree> tree(String folder, ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        return execute("files", folder, null, null, "tree", params, GET, FileTree.class);
    }

    /**
     * Update some file attributes.
     * @param files Comma separated list of file ids, names or paths. Paths must be separated by : instead of /.
     * @param data Parameters to modify.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<File> update(String files, FileUpdateParams data, ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        params.put("body", data);
        return execute("files", files, null, null, "update", params, POST, File.class);
    }

    /**
     * Return the acl defined for the file or folder. If member is provided, it will only return the acl for the member.
     * @param files Comma separated list of file ids or names up to a maximum of 100.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<ObjectMap> acl(String files, ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        return execute("files", files, null, null, "acl", params, GET, ObjectMap.class);
    }

    /**
     * Update the set of permissions granted for the member.
     * @param members Comma separated list of user or group ids.
     * @param data JSON containing the parameters to add ACLs.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<ObjectMap> updateAcl(String members, FileAclUpdateParams data, ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        params.put("body", data);
        return execute("files", members, null, null, "update", params, POST, ObjectMap.class);
    }

    /**
     * Fetch catalog file stats.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<FacetField> aggregationStats(ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        return execute("files", null, null, null, "aggregationStats", params, GET, FacetField.class);
    }

    /**
     * Create file or folder.
     * @param data File parameters.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<File> create(FileCreateParams data, ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        params.put("body", data);
        return execute("files", null, null, null, "create", params, POST, File.class);
    }

    /**
     * Download an external file to catalog and register it.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<Job> fetch(ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        return execute("files", null, null, null, "fetch", params, POST, Job.class);
    }

    /**
     * List of accepted file bioformats.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<File.Bioformat> bioformats() throws ClientException {
        ObjectMap params = new ObjectMap();
        return execute("files", null, null, null, "bioformats", params, GET, File.Bioformat.class);
    }

    /**
     * Resource to upload a file by chunks.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<File> upload(ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        return execute("files", null, null, null, "upload", params, POST, File.class);
    }

    /**
     * Download file.
     * @param file File id, name or path. Paths must be separated by : instead of /.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<DataInputStream> download(String file, ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        return execute("files", file, null, null, "download", params, GET, DataInputStream.class);
    }

    /**
     * Filter lines of the file containing a match of the pattern [NOT TESTED].
     * @param file File id, name or path. Paths must be separated by : instead of /.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<String> grep(String file, ObjectMap params) throws ClientException {
        params = params != null ? params : new ObjectMap();
        return execute("files", file, null, null, "grep", params, GET, String.class);
    }

    /**
     * Update annotations from an annotationSet.
     * @param file File id, name or path. Paths must be separated by : instead of /.
     * @param annotationSet AnnotationSet id to be updated.
     * @param data Json containing the map of annotations when the action is ADD, SET or REPLACE, a json with only the key 'remove'
     *     containing the comma separated variables to be removed as a value when the action is REMOVE or a json with only the key 'reset'
     *     containing the comma separated variables that will be set to the default value when the action is RESET.
     * @param params Map containing any additional optional parameters.
     * @return a RestResponse object.
     * @throws ClientException ClientException if there is any server error.
     */
    public RestResponse<File> updateAnnotations(String file, String annotationSet, ObjectMap data, ObjectMap params)
            throws ClientException {
        params = params != null ? params : new ObjectMap();
        params.put("body", data);
        return execute("files", file, "annotationSets", annotationSet, "annotations/update", params, POST, File.class);
    }
}
