package org.opencb.opencga.core.models.file;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

public class FileReferenceParam {

    @DataField(description = ParamConstants.FILE_REFERENCE_PARAM_ID_DESCRIPTION)
    private String id;

    public FileReferenceParam() {
    }

    public FileReferenceParam(String id) {
        this.id = id;
    }

    public static FileReferenceParam of(File file) {
        return new FileReferenceParam(file.getId());
    }

    public File toFile() {
        return new File().setId(id);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("FileReferenceParam{");
        sb.append("id='").append(id).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public FileReferenceParam setId(String id) {
        this.id = id;
        return this;
    }
}
