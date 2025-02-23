package org.opencb.opencga.app.cli.main.options;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.DynamicParameter;
import com.beust.jcommander.ParametersDelegate;

import java.util.HashMap;
import java.util.Map;
import java.util.List;

import static org.opencb.opencga.app.cli.GeneralCliOptions.*;


/*
* WARNING: AUTOGENERATED CODE
*
* This code was generated by a tool.
*
* Manual changes to this file may cause unexpected behavior in your application.
* Manual changes to this file will be overwritten if the code is regenerated.
*  
*/

/**
 * This class contains methods for the Organizations command line.
 *    PATH: /{apiVersion}/organizations
 */
@Parameters(commandNames = {"organizations"}, commandDescription = "Organizations commands")
public class OrganizationsCommandOptions {

        public JCommander jCommander;
        public CommonCommandOptions commonCommandOptions;

        public CreateCommandOptions createCommandOptions;
        public CreateNotesCommandOptions createNotesCommandOptions;
        public SearchNotesCommandOptions searchNotesCommandOptions;
        public DeleteNotesCommandOptions deleteNotesCommandOptions;
        public UpdateNotesCommandOptions updateNotesCommandOptions;
        public UserUpdateStatusCommandOptions userUpdateStatusCommandOptions;
        public UpdateUserCommandOptions updateUserCommandOptions;
        public UpdateConfigurationCommandOptions updateConfigurationCommandOptions;
        public InfoCommandOptions infoCommandOptions;
        public UpdateCommandOptions updateCommandOptions;


    public OrganizationsCommandOptions(CommonCommandOptions commonCommandOptions, JCommander jCommander) {
    
        this.jCommander = jCommander;
        this.commonCommandOptions = commonCommandOptions;
        this.createCommandOptions = new CreateCommandOptions();
        this.createNotesCommandOptions = new CreateNotesCommandOptions();
        this.searchNotesCommandOptions = new SearchNotesCommandOptions();
        this.deleteNotesCommandOptions = new DeleteNotesCommandOptions();
        this.updateNotesCommandOptions = new UpdateNotesCommandOptions();
        this.userUpdateStatusCommandOptions = new UserUpdateStatusCommandOptions();
        this.updateUserCommandOptions = new UpdateUserCommandOptions();
        this.updateConfigurationCommandOptions = new UpdateConfigurationCommandOptions();
        this.infoCommandOptions = new InfoCommandOptions();
        this.updateCommandOptions = new UpdateCommandOptions();
    
    }
    
    @Parameters(commandNames = {"create"}, commandDescription ="Create a new organization")
    public class CreateCommandOptions {
    
        @ParametersDelegate
        public CommonCommandOptions commonOptions = commonCommandOptions;
    
        @Parameter(names = {"--json-file"}, description = "File with the body data in JSON format. Note, that using this parameter will ignore all the other parameters.", required = false, arity = 1)
        public String jsonFile;
    
        @Parameter(names = {"--json-data-model"}, description = "Show example of file structure for body data.", help = true, arity = 0)
        public Boolean jsonDataModel = false;
    
        @Parameter(names = {"--include", "-I"}, description = "Fields included in the response, whole JSON path must be provided", required = false, arity = 1)
        public String include; 
    
        @Parameter(names = {"--exclude", "-E"}, description = "Fields excluded in the response, whole JSON path must be provided", required = false, arity = 1)
        public String exclude; 
    
        @Parameter(names = {"--include-result"}, description = "Flag indicating to include the created or updated document result in the response", required = false, help = true, arity = 0)
        public boolean includeResult = false; 
    
        @Parameter(names = {"--id"}, description = "Organization unique identifier.", required = true, arity = 1)
        public String id;
    
        @Parameter(names = {"--name", "-n"}, description = "Organization name.", required = false, arity = 1)
        public String name;
    
        @Parameter(names = {"--creation-date", "--cd"}, description = "Autogenerated date following the format YYYYMMDDhhmmss containing the date when the entry was first registered.", required = false, arity = 1)
        public String creationDate;
    
        @Parameter(names = {"--modification-date", "--md"}, description = "Autogenerated date following the format YYYYMMDDhhmmss containing the date when the entry was last modified.", required = false, arity = 1)
        public String modificationDate;
    
        @Parameter(names = {"--configuration-default-user-expiration-date"}, description = "The body web service defaultUserExpirationDate parameter", required = false, arity = 1)
        public String configurationDefaultUserExpirationDate;
    
        @DynamicParameter(names = {"--attributes"}, description = "You can use this field to store any other information, keep in mind this is not indexed so you cannot search by attributes.. Use: --attributes key=value", required = false)
        public java.util.Map<java.lang.String,java.lang.Object> attributes = new HashMap<>(); //Dynamic parameters must be initialized;
    
    }

    @Parameters(commandNames = {"notes-create"}, commandDescription ="Create a new note")
    public class CreateNotesCommandOptions {
    
        @ParametersDelegate
        public CommonCommandOptions commonOptions = commonCommandOptions;
    
        @Parameter(names = {"--json-file"}, description = "File with the body data in JSON format. Note, that using this parameter will ignore all the other parameters.", required = false, arity = 1)
        public String jsonFile;
    
        @Parameter(names = {"--json-data-model"}, description = "Show example of file structure for body data.", help = true, arity = 0)
        public Boolean jsonDataModel = false;
    
        @Parameter(names = {"--include", "-I"}, description = "Fields included in the response, whole JSON path must be provided", required = false, arity = 1)
        public String include; 
    
        @Parameter(names = {"--exclude", "-E"}, description = "Fields excluded in the response, whole JSON path must be provided", required = false, arity = 1)
        public String exclude; 
    
        @Parameter(names = {"--include-result"}, description = "Flag indicating to include the created or updated document result in the response", required = false, help = true, arity = 0)
        public boolean includeResult = false; 
    
        @Parameter(names = {"--id"}, description = "The body web service id parameter", required = false, arity = 1)
        public String id;
    
        @Parameter(names = {"--type"}, description = "Enum param allowed values: VARIANT, GENE, TRANSCRIPT, PROTEIN, JOB, FILE, SAMPLE, INDIVIDUAL, FAMILY, COHORT, DISEASE_PANEL, CLINICAL_ANALYSIS, WORKFLOW, ORGANIZATION, OTHER, UNKNOWN", required = false, arity = 1)
        public String type;
    
        @Parameter(names = {"--tags"}, description = "The body web service tags parameter", required = false, arity = 1)
        public String tags;
    
        @Parameter(names = {"--visibility"}, description = "Enum param allowed values: PUBLIC, PRIVATE", required = false, arity = 1)
        public String visibility;
    
        @Parameter(names = {"--value-type"}, description = "Enum param allowed values: OBJECT, ARRAY, STRING, INTEGER, DOUBLE", required = false, arity = 1)
        public String valueType;
    
    }

    @Parameters(commandNames = {"notes-search"}, commandDescription ="Search for notes of scope ORGANIZATION")
    public class SearchNotesCommandOptions {
    
        @ParametersDelegate
        public CommonCommandOptions commonOptions = commonCommandOptions;
    
        @Parameter(names = {"--include", "-I"}, description = "Fields included in the response, whole JSON path must be provided", required = false, arity = 1)
        public String include; 
    
        @Parameter(names = {"--exclude", "-E"}, description = "Fields excluded in the response, whole JSON path must be provided", required = false, arity = 1)
        public String exclude; 
    
        @Parameter(names = {"--creation-date", "--cd"}, description = "Creation date. Format: yyyyMMddHHmmss. Examples: >2018, 2017-2018, <201805", required = false, arity = 1)
        public String creationDate; 
    
        @Parameter(names = {"--modification-date", "--md"}, description = "Modification date. Format: yyyyMMddHHmmss. Examples: >2018, 2017-2018, <201805", required = false, arity = 1)
        public String modificationDate; 
    
        @Parameter(names = {"--id"}, description = "Note unique identifier.", required = false, arity = 1)
        public String id; 
    
        @Parameter(names = {"--type"}, description = "Note type.", required = false, arity = 1)
        public String type; 
    
        @Parameter(names = {"--scope"}, description = "Scope of the Note.", required = false, arity = 1)
        public String scope; 
    
        @Parameter(names = {"--visibility"}, description = "Visibility of the Note.", required = false, arity = 1)
        public String visibility; 
    
        @Parameter(names = {"--uuid"}, description = "Unique 32-character identifier assigned automatically by OpenCGA.", required = false, arity = 1)
        public String uuid; 
    
        @Parameter(names = {"--user-id"}, description = "User that wrote that Note.", required = false, arity = 1)
        public String userId; 
    
        @Parameter(names = {"--tags"}, description = "Note tags.", required = false, arity = 1)
        public String tags; 
    
        @Parameter(names = {"--version"}, description = "Autoincremental version assigned to the registered entry. By default, updates does not create new versions. To enable versioning, users must set the `incVersion` flag from the /update web service when updating the document.", required = false, arity = 1)
        public String version; 
    
    }

    @Parameters(commandNames = {"notes-delete"}, commandDescription ="Delete note")
    public class DeleteNotesCommandOptions {
    
        @ParametersDelegate
        public CommonCommandOptions commonOptions = commonCommandOptions;
    
        @Parameter(names = {"--id"}, description = "Note unique identifier.", required = true, arity = 1)
        public String id; 
    
        @Parameter(names = {"--include-result"}, description = "Flag indicating to include the created or updated document result in the response", required = false, help = true, arity = 0)
        public boolean includeResult = false; 
    
    }

    @Parameters(commandNames = {"notes-update"}, commandDescription ="Update a note")
    public class UpdateNotesCommandOptions {
    
        @ParametersDelegate
        public CommonCommandOptions commonOptions = commonCommandOptions;
    
        @Parameter(names = {"--json-file"}, description = "File with the body data in JSON format. Note, that using this parameter will ignore all the other parameters.", required = false, arity = 1)
        public String jsonFile;
    
        @Parameter(names = {"--json-data-model"}, description = "Show example of file structure for body data.", help = true, arity = 0)
        public Boolean jsonDataModel = false;
    
        @Parameter(names = {"--include", "-I"}, description = "Fields included in the response, whole JSON path must be provided", required = false, arity = 1)
        public String include; 
    
        @Parameter(names = {"--exclude", "-E"}, description = "Fields excluded in the response, whole JSON path must be provided", required = false, arity = 1)
        public String exclude; 
    
        @Parameter(names = {"--id"}, description = "Note unique identifier.", required = true, arity = 1)
        public String id; 
    
        @Parameter(names = {"--tags-action"}, description = "Action to be performed if the array of tags is being updated.", required = false, arity = 1)
        public String tagsAction = "ADD"; 
    
        @Parameter(names = {"--include-result"}, description = "Flag indicating to include the created or updated document result in the response", required = false, help = true, arity = 0)
        public boolean includeResult = false; 
    
        @Parameter(names = {"--type"}, description = "Enum param allowed values: VARIANT, GENE, TRANSCRIPT, PROTEIN, JOB, FILE, SAMPLE, INDIVIDUAL, FAMILY, COHORT, DISEASE_PANEL, CLINICAL_ANALYSIS, WORKFLOW, ORGANIZATION, OTHER, UNKNOWN", required = false, arity = 1)
        public String type;
    
        @Parameter(names = {"--tags"}, description = "The body web service tags parameter", required = false, arity = 1)
        public String tags;
    
        @Parameter(names = {"--visibility"}, description = "Enum param allowed values: PUBLIC, PRIVATE", required = false, arity = 1)
        public String visibility;
    
    }

    @Parameters(commandNames = {"update-status-user"}, commandDescription ="Update the user status")
    public class UserUpdateStatusCommandOptions {
    
        @ParametersDelegate
        public CommonCommandOptions commonOptions = commonCommandOptions;
    
        @Parameter(names = {"--json-file"}, description = "File with the body data in JSON format. Note, that using this parameter will ignore all the other parameters.", required = false, arity = 1)
        public String jsonFile;
    
        @Parameter(names = {"--json-data-model"}, description = "Show example of file structure for body data.", help = true, arity = 0)
        public Boolean jsonDataModel = false;
    
        @Parameter(names = {"--include", "-I"}, description = "Fields included in the response, whole JSON path must be provided", required = false, arity = 1)
        public String include; 
    
        @Parameter(names = {"--exclude", "-E"}, description = "Fields excluded in the response, whole JSON path must be provided", required = false, arity = 1)
        public String exclude; 
    
        @Parameter(names = {"--user", "-u"}, description = "User ID", required = true, arity = 1)
        public String user; 
    
        @Parameter(names = {"--organization"}, description = "Organization id", required = false, arity = 1)
        public String organization; 
    
        @Parameter(names = {"--include-result"}, description = "Flag indicating to include the created or updated document result in the response", required = false, help = true, arity = 0)
        public boolean includeResult = false; 
    
        @Parameter(names = {"--status"}, description = "The body web service status parameter", required = false, arity = 1)
        public String status;
    
    }

    @Parameters(commandNames = {"user-update"}, commandDescription ="Update the user information")
    public class UpdateUserCommandOptions {
    
        @ParametersDelegate
        public CommonCommandOptions commonOptions = commonCommandOptions;
    
        @Parameter(names = {"--json-file"}, description = "File with the body data in JSON format. Note, that using this parameter will ignore all the other parameters.", required = false, arity = 1)
        public String jsonFile;
    
        @Parameter(names = {"--json-data-model"}, description = "Show example of file structure for body data.", help = true, arity = 0)
        public Boolean jsonDataModel = false;
    
        @Parameter(names = {"--include", "-I"}, description = "Fields included in the response, whole JSON path must be provided", required = false, arity = 1)
        public String include; 
    
        @Parameter(names = {"--exclude", "-E"}, description = "Fields excluded in the response, whole JSON path must be provided", required = false, arity = 1)
        public String exclude; 
    
        @Parameter(names = {"--user", "-u"}, description = "User ID", required = true, arity = 1)
        public String user; 
    
        @Parameter(names = {"--organization"}, description = "Organization id", required = false, arity = 1)
        public String organization; 
    
        @Parameter(names = {"--include-result"}, description = "Flag indicating to include the created or updated document result in the response", required = false, help = true, arity = 0)
        public boolean includeResult = false; 
    
        @Parameter(names = {"--name", "-n"}, description = "The body web service name parameter", required = false, arity = 1)
        public String name;
    
        @Parameter(names = {"--email"}, description = "The body web service email parameter", required = false, arity = 1)
        public String email;
    
        @Parameter(names = {"--quota-disk-usage"}, description = "The body web service diskUsage parameter", required = false, arity = 1)
        public Long quotaDiskUsage;
    
        @Parameter(names = {"--quota-cpu-usage"}, description = "The body web service cpuUsage parameter", required = false, arity = 1)
        public Integer quotaCpuUsage;
    
        @Parameter(names = {"--quota-max-disk"}, description = "The body web service maxDisk parameter", required = false, arity = 1)
        public Long quotaMaxDisk;
    
        @Parameter(names = {"--quota-max-cpu"}, description = "The body web service maxCpu parameter", required = false, arity = 1)
        public Integer quotaMaxCpu;
    
        @DynamicParameter(names = {"--attributes"}, description = "The body web service attributes parameter. Use: --attributes key=value", required = false)
        public java.util.Map<java.lang.String,java.lang.Object> attributes = new HashMap<>(); //Dynamic parameters must be initialized;
    
    }

    @Parameters(commandNames = {"configuration-update"}, commandDescription ="Update the Organization configuration attributes")
    public class UpdateConfigurationCommandOptions {
    
        @ParametersDelegate
        public CommonCommandOptions commonOptions = commonCommandOptions;
    
        @Parameter(names = {"--json-file"}, description = "File with the body data in JSON format. Note, that using this parameter will ignore all the other parameters.", required = false, arity = 1)
        public String jsonFile;
    
        @Parameter(names = {"--json-data-model"}, description = "Show example of file structure for body data.", help = true, arity = 0)
        public Boolean jsonDataModel = false;
    
        @Parameter(names = {"--include", "-I"}, description = "Fields included in the response, whole JSON path must be provided", required = false, arity = 1)
        public String include; 
    
        @Parameter(names = {"--exclude", "-E"}, description = "Fields excluded in the response, whole JSON path must be provided", required = false, arity = 1)
        public String exclude; 
    
        @Parameter(names = {"--organization"}, description = "Organization id", required = true, arity = 1)
        public String organization; 
    
        @Parameter(names = {"--include-result"}, description = "Flag indicating to include the created or updated document result in the response", required = false, help = true, arity = 0)
        public boolean includeResult = false; 
    
        @Parameter(names = {"--authentication-origins-action"}, description = "Action to be performed if the array of authenticationOrigins is being updated.", required = false, arity = 1)
        public String authenticationOriginsAction = "ADD"; 
    
        @Parameter(names = {"--default-user-expiration-date"}, description = "The body web service defaultUserExpirationDate parameter", required = false, arity = 1)
        public String defaultUserExpirationDate;
    
        @Parameter(names = {"--optimizations-simplify-permissions"}, description = "The body web service simplifyPermissions parameter", required = false, help = true, arity = 0)
        public boolean optimizationsSimplifyPermissions = false;
    
        @Parameter(names = {"--token-algorithm"}, description = "The body web service algorithm parameter", required = false, arity = 1)
        public String tokenAlgorithm;
    
        @Parameter(names = {"--token-secret-key"}, description = "The body web service secretKey parameter", required = false, arity = 1)
        public String tokenSecretKey;
    
        @Parameter(names = {"--token-expiration"}, description = "The body web service expiration parameter", required = false, arity = 1)
        public Long tokenExpiration;
    
    }

    @Parameters(commandNames = {"info"}, commandDescription ="Return the organization information")
    public class InfoCommandOptions {
    
        @ParametersDelegate
        public CommonCommandOptions commonOptions = commonCommandOptions;
    
        @Parameter(names = {"--include", "-I"}, description = "Fields included in the response, whole JSON path must be provided", required = false, arity = 1)
        public String include; 
    
        @Parameter(names = {"--exclude", "-E"}, description = "Fields excluded in the response, whole JSON path must be provided", required = false, arity = 1)
        public String exclude; 
    
        @Parameter(names = {"--organization"}, description = "Organization id", required = true, arity = 1)
        public String organization; 
    
    }

    @Parameters(commandNames = {"update"}, commandDescription ="Update some organization attributes")
    public class UpdateCommandOptions {
    
        @ParametersDelegate
        public CommonCommandOptions commonOptions = commonCommandOptions;
    
        @Parameter(names = {"--json-file"}, description = "File with the body data in JSON format. Note, that using this parameter will ignore all the other parameters.", required = false, arity = 1)
        public String jsonFile;
    
        @Parameter(names = {"--json-data-model"}, description = "Show example of file structure for body data.", help = true, arity = 0)
        public Boolean jsonDataModel = false;
    
        @Parameter(names = {"--include", "-I"}, description = "Fields included in the response, whole JSON path must be provided", required = false, arity = 1)
        public String include; 
    
        @Parameter(names = {"--exclude", "-E"}, description = "Fields excluded in the response, whole JSON path must be provided", required = false, arity = 1)
        public String exclude; 
    
        @Parameter(names = {"--organization"}, description = "Organization id", required = true, arity = 1)
        public String organization; 
    
        @Parameter(names = {"--include-result"}, description = "Flag indicating to include the created or updated document result in the response", required = false, help = true, arity = 0)
        public boolean includeResult = false; 
    
        @Parameter(names = {"--admins-action"}, description = "Action to be performed if the array of admins is being updated.", required = false, arity = 1)
        public String adminsAction = "ADD"; 
    
        @Parameter(names = {"--name", "-n"}, description = "Organization name.", required = false, arity = 1)
        public String name;
    
        @Parameter(names = {"--owner"}, description = "Owner of the organization.", required = false, arity = 1)
        public String owner;
    
        @Parameter(names = {"--admins"}, description = "Administrative users of the organization.", required = false, arity = 1)
        public String admins;
    
        @Parameter(names = {"--creation-date", "--cd"}, description = "Autogenerated date following the format YYYYMMDDhhmmss containing the date when the entry was first registered.", required = false, arity = 1)
        public String creationDate;
    
        @Parameter(names = {"--modification-date", "--md"}, description = "Autogenerated date following the format YYYYMMDDhhmmss containing the date when the entry was last modified.", required = false, arity = 1)
        public String modificationDate;
    
        @DynamicParameter(names = {"--attributes"}, description = "You can use this field to store any other information, keep in mind this is not indexed so you cannot search by attributes.. Use: --attributes key=value", required = false)
        public java.util.Map<java.lang.String,java.lang.Object> attributes = new HashMap<>(); //Dynamic parameters must be initialized;
    
    }

}