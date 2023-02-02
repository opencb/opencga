package org.opencb.opencga.catalog.utils;

import org.opencb.opencga.core.api.ParamConstants;

import java.util.Objects;

public class FqnUtils {

    public static class FQN {
        private final String user;
        private final String project;
        private final String study;
        private final String projectFqn;

        public FQN(String fqn) {
            if (fqn == null || fqn.isEmpty()) {
                throw new IllegalArgumentException("Empty FQN");
            }
            int idx1 = fqn.indexOf(ParamConstants.USER_PROJECT_SEPARATOR);
            int idx2 = fqn.indexOf(ParamConstants.PROJECT_STUDY_SEPARATOR, idx1 + 1);
            if (idx1 < 0) {
                throw new IllegalArgumentException("Invalid FQN. Expected <user>@<project>[:<study>]. Got '" + fqn + "'");
            }
            user = fqn.substring(0, idx1);
            if (idx2 < 0) {
                project = fqn.substring(idx1 + 1);
                projectFqn = fqn;
                study = null;
            } else {
                project = fqn.substring(idx1 + 1, idx2);
                projectFqn = fqn.substring(0, idx2);
                study = fqn.substring(idx2 + 1);
            }
        }

        public FQN(String user, String project) {
            this(user, project, null);
        }

        public FQN(String user, String project, String study) {
            this.user = Objects.requireNonNull(user);
            this.project = Objects.requireNonNull(project);
            projectFqn = user + ParamConstants.USER_PROJECT_SEPARATOR + project;
            this.study = study;
        }

        public String getUser() {
            return user;
        }

        public String getProject() {
            return project;
        }

        public String getProjectFqn() {
            return projectFqn;
        }

        public String getStudy() {
            return study;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder(user)
                    .append(ParamConstants.USER_PROJECT_SEPARATOR)
                    .append(project);
            if (study != null) {
                sb.append(ParamConstants.PROJECT_STUDY_SEPARATOR)
                        .append(study);
            }
            return sb.toString();
        }
    }

    public static FQN parse(String fqn) {
        return new FQN(fqn);
    }

    public static String getUser(String fqn) {
        return new FQN(fqn).getUser();
    }

    public static String getProject(String fqn) {
        return new FQN(fqn).getProject();
    }

    public static String getStudy(String fqn) {
        return new FQN(fqn).getStudy();
    }

    public static String toProjectFqn(String fqn) {
        return new FQN(fqn).getProjectFqn();
    }

    public static String buildFqn(String user, String project) {
        return new FQN(user, project).toString();
    }

    public static String buildFqn(String user, String project, String study) {
        return new FQN(user, project, study).toString();
    }

}
