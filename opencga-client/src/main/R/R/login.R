user_login <- function() {
  require(miniUI)
  require(shiny)
  ui <- miniPage(
    gadgetTitleBar("Please enter your username and password"),
    miniContentPanel(
      textInput("username","Username"),
      passwordInput("password", "Password")

    )
  )

  server <- function(input, output) {
    observeEvent(input$done, {
      user <- input$username
      pass <- input$password
      res <- list(user=user, pass=pass)
      stopApp(res)
    })
    observeEvent(input$cancel, {
      stopApp(stop("No password.", call. = FALSE))
    })
  }

  runGadget(ui, server, viewer = dialogViewer("user_login"))
}



