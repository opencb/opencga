#' @title SSO Login
#' @description Perform SSO authentication using a Python script
#' @param client_config_file Path to client configuration file
#' @param host_name Host name when multiple hosts exist in client config file
#' @param host_url Host URL to override configuration file info
#' @return Invisible NULL, creates a session file at ~/.opencga/
#' @export
ssoLogin <- function(client_config_file = NULL, host_name = NULL, host_url = NULL) {
  # Check that at least one of client_config_file or host_url is provided
  if (is.null(client_config_file) && is.null(host_url)) {
    stop("Please provide either client_config_file or host_url")
  }

  # First time import of reticulate will trigger dependency check
  if (!requireNamespace("reticulate", quietly = TRUE)) {
    stop("Package 'reticulate' is required for SSO login. Please install it.")
  }

  # Setup and activate the Python environment
  .setupPythonEnvironment()

  # Get the path to the Python script in the package
  py_script <- system.file("python/sso_login.py", package = "opencgaEnterpriseR")
  if (py_script == "") {
    stop("Python script not found in package")
  }

  # Build command arguments
  args <- c()
  if (!is.null(client_config_file)) {
    args <- c(args, "--client_config_file", client_config_file)
  }
  if (!is.null(host_name)) {
    args <- c(args, "--host_name", host_name)
  }
  if (!is.null(host_url)) {
    args <- c(args, "--host_url", host_url)
  }

  # Execute the Python script using reticulate
  reticulate::py_run_string(sprintf("import sys; sys.argv = ['%s', %s]", py_script, paste(sprintf("'%s'", args), collapse = ", ")))
  tryCatch(
    reticulate::py_run_file(py_script),
    error = function(e) {
      if (inherits(e, "python.builtin.SystemExit")) {
        # Ignore SystemExit
        invisible(NULL)
      } else {
        stop(e)
      }
    }
  )

  # Load generated session file and return instance of OpencgaR
  ocga <- initOpencgaR(host = host_url, opencgaConfig = client_config_file)
  return (ocga)
}