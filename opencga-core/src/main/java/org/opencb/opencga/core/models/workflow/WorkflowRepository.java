package org.opencb.opencga.core.models.workflow;

public class WorkflowRepository {

    private String image;

    public WorkflowRepository() {
    }

    public WorkflowRepository(String image) {
        this.image = image;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("WorkflowRepository{");
        sb.append("image='").append(image).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getImage() {
        return image;
    }

    public WorkflowRepository setImage(String image) {
        this.image = image;
        return this;
    }
}
