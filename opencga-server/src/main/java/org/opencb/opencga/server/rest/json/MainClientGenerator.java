package org.opencb.opencga.server.rest.json;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.opencb.opencga.server.rest.*;
import org.opencb.opencga.server.rest.admin.AdminWSServer;
import org.opencb.opencga.server.rest.analysis.AlignmentWebService;
import org.opencb.opencga.server.rest.analysis.ClinicalWebService;
import org.opencb.opencga.server.rest.analysis.VariantWebService;
import org.opencb.opencga.server.rest.ga4gh.Ga4ghWSServer;
import org.opencb.opencga.server.rest.json.beans.Category;
import org.opencb.opencga.server.rest.operations.VariantOperationWebService;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

public class MainClientGenerator {

    public static void main(String[] args) {
        List<Class> classes = new ArrayList<>();
        classes.add(UserWSServer.class);
        classes.add(ProjectWSServer.class);
        classes.add(StudyWSServer.class);
        classes.add(FileWSServer.class);
        classes.add(JobWSServer.class);
        classes.add(SampleWSServer.class);
        classes.add(IndividualWSServer.class);
        classes.add(FamilyWSServer.class);
        classes.add(CohortWSServer.class);
        classes.add(PanelWSServer.class);
        classes.add(AlignmentWebService.class);
        classes.add(VariantWebService.class);
        classes.add(ClinicalWebService.class);
        classes.add(VariantOperationWebService.class);
        classes.add(MetaWSServer.class);
        classes.add(Ga4ghWSServer.class);
        classes.add(AdminWSServer.class);
        List<Category> categories = JSONManager.getCategories(classes);
        for (Category category : categories) {
            System.out.println(category);
        }
    }

    private static List<Category> getCategories(List<LinkedHashMap<String, Object>> data) {
        Gson gson = new Gson();
        String json = gson.toJson(data, List.class);
        Type type = new TypeToken<List<Category>>() {
        }.getType();
        List<Category> result = gson.fromJson(json, type);
        return result;
    }
}
