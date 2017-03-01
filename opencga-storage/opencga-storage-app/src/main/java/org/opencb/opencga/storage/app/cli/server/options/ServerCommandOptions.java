package org.opencb.opencga.storage.app.cli.server.options;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import org.opencb.opencga.storage.app.cli.GeneralCliOptions;

/**
 * Created by joaquin on 2/2/17.
 */
@Parameters(commandNames = {"server"}, commandDescription = "REST and gRPC server management.")
public class ServerCommandOptions {

    public RestServerCommandOptions restServerCommandOptions;
    public GrpcServerCommandOptions grpcServerCommandOptions;

    public JCommander jCommander;
    public GeneralCliOptions.CommonOptions commonCommandOptions;

    public ServerCommandOptions(GeneralCliOptions.CommonOptions commonOptions, JCommander jCommander) {
        this.jCommander = jCommander;
        this.commonCommandOptions = commonOptions;

        this.restServerCommandOptions = new RestServerCommandOptions();
        this.grpcServerCommandOptions = new GrpcServerCommandOptions();
    }

    /**
     * Generic server options (for REST and gRPC)
     */
    public class GenericServerOptions {
        @Parameter(names = {"--start"}, description = "Start server.")
        public boolean start;

        @Parameter(names = {"--stop"}, description = "Stop server.")
        public boolean stop;

        @Parameter(names = {"--status"}, description = "Status server.")
        public boolean status;

        @Parameter(names = {"--port"}, description = "Port number.", arity = 1)
        public int port;

        @Parameter(names = {"--auth-manager"}, description = "Authentication manager.", arity = 1)
        public String authManager;
    }

    /**
     * REST command
     */
    @Parameters(commandNames = {"rest"}, commandDescription = "REST server")
    public class RestServerCommandOptions extends GenericServerOptions {

        @ParametersDelegate
        public GeneralCliOptions.CommonOptions commonOptions = commonCommandOptions;
    }

    /**
     * GRPC command
     */
    @Parameters(commandNames = {"grpc"}, commandDescription = "gRPC server")
    public class GrpcServerCommandOptions extends GenericServerOptions {

        @ParametersDelegate
        public GeneralCliOptions.CommonOptions commonOptions = commonCommandOptions;
    }

    public JCommander getJCommander() {
        return this.jCommander;
    }
}
