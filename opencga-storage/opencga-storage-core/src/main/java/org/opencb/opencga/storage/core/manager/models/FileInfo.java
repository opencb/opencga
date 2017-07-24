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

package org.opencb.opencga.storage.core.manager.models;

import org.opencb.opencga.catalog.models.File;

import java.nio.file.Path;

/**
 * Created by pfurio on 24/11/16.
 */
public class FileInfo {

    private String name;
    private Path path; // Physical path to the file or folder (equivalent to URI in catalog)
    private long fileId;
    private File.Bioformat bioformat;
    private File.Format format;


    public FileInfo() {
        this.fileId = -1;
    }

    public String getName() {
        return name;
    }

    public FileInfo setName(String name) {
        this.name = name;
        return this;
    }

    public Path getPath() {
        return path;
    }

    public FileInfo setPath(Path path) {
        this.path = path;
        return this;
    }

    public long getFileId() {
        return fileId;
    }

    public FileInfo setFileId(long fileId) {
        this.fileId = fileId;
        return this;
    }

    public File.Bioformat getBioformat() {
        return bioformat;
    }

    public FileInfo setBioformat(File.Bioformat bioformat) {
        this.bioformat = bioformat;
        return this;
    }

    public File.Format getFormat() {
        return format;
    }

    public FileInfo setFormat(File.Format format) {
        this.format = format;
        return this;
    }
}
