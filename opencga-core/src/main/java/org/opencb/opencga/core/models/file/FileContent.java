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

package org.opencb.opencga.core.models.file;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

public class FileContent {

    /**
     * File id.
     */
    @DataField(description = ParamConstants.FILE_CONTENT_FILE_ID_DESCRIPTION)
    private String fileId;

    /**
     * Flag indicating whether the content has reached the end of file.
     */
    @DataField(description = ParamConstants.FILE_CONTENT_EOF_DESCRIPTION)
    private boolean eof;

    /**
     * Final byte of the file read.
     */
    @DataField(description = ParamConstants.FILE_CONTENT_OFFSET_DESCRIPTION)
    private long offset;

    /**
     * Number of bytes returned.
     */
    @DataField(description = ParamConstants.FILE_CONTENT_SIZE_DESCRIPTION)
    private int size;

    /**
     * Number of lines read.
     */
    @DataField(description = ParamConstants.FILE_CONTENT_LINES_DESCRIPTION)
    private int lines;

    /**
     * Partial or full content of the file.
     */
    @DataField(description = ParamConstants.FILE_CONTENT_CONTENT_DESCRIPTION)
    private String content;

    public FileContent() {
    }

    public FileContent(String fileId, boolean eof, long offset, int size, String content) {
        this.fileId = fileId;
        this.eof = eof;
        this.offset = offset;
        this.size = size;
        this.content = content;
    }

    public FileContent(String fileId, boolean eof, long offset, int size, int lines, String content) {
        this.fileId = fileId;
        this.eof = eof;
        this.offset = offset;
        this.size = size;
        this.lines = lines;
        this.content = content;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("FileContent{");
        sb.append("fileId='").append(fileId).append('\'');
        sb.append(", eof=").append(eof);
        sb.append(", offset=").append(offset);
        sb.append(", size=").append(size);
        sb.append(", lines=").append(lines);
        sb.append(", content='").append(content).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getFileId() {
        return fileId;
    }

    public FileContent setFileId(String fileId) {
        this.fileId = fileId;
        return this;
    }

    public boolean isEof() {
        return eof;
    }

    public FileContent setEof(boolean eof) {
        this.eof = eof;
        return this;
    }

    public long getOffset() {
        return offset;
    }

    public FileContent setOffset(long offset) {
        this.offset = offset;
        return this;
    }

    public int getSize() {
        return size;
    }

    public FileContent setSize(int size) {
        this.size = size;
        return this;
    }

    public int getLines() {
        return lines;
    }

    public FileContent setLines(int lines) {
        this.lines = lines;
        return this;
    }

    public String getContent() {
        return content;
    }

    public FileContent setContent(String content) {
        this.content = content;
        return this;
    }
}
