# Package initialization functions

#' @title Setup Python environment for SSO login
#' @description Creates and configures a Python virtual environment with required dependencies
#' @keywords internal
.setupPythonEnvironment <- function() {
  if (!requireNamespace("reticulate", quietly = TRUE)) {
    stop("Package 'reticulate' is required. Please install it with install.packages('reticulate')")
  }

  venv_dir <- file.path(system.file(package = "opencgaEnterpriseR"), "python-env")

  # Create virtual environment if it doesn't exist
  if (!dir.exists(venv_dir)) {
    message("Setting up Python environment for SSO login (this may take a minute)...")
    reticulate::virtualenv_create(envname = venv_dir,
                                 packages = c("flask", "pyyaml"))
  }

  # Activate the virtual environment
  reticulate::use_virtualenv(venv_dir, required = TRUE)

  invisible(venv_dir)
}

.onLoad <- function(libname, pkgname) {
  # Configure Python path but don't create environment yet
  # This defers the actual environment creation until ssoLogin is called
  venv_dir <- file.path(system.file(package = "opencgaEnterpriseR"), "python-env")

  if (dir.exists(venv_dir)) {
    reticulate::configure_environment(pkgname)
  }
}