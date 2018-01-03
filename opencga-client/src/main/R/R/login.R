if(requireNamespace("miniUI", quietly = TRUE)){
    if(requireNamespace("shiny", quietly = TRUE)){
        user_login <- function() {
            ui <- miniUI::miniPage(
                miniUI::gadgetTitleBar("Please enter your username and password"),
                miniUI::miniContentPanel(
                    shiny::textInput("username", "Username"),
                    shiny::passwordInput("password", "Password")
                    
                )
            )
            
            server <- function(input, output) {
                shiny::observeEvent(input$done, {
                    user <- input$username
                    pass <- input$password
                    res <- list(user=user, pass=pass)
                    shiny::stopApp(res)
                })
                shiny::observeEvent(input$cancel, {
                    shiny::stopApp(stop("No password.", call. = FALSE))
                })
            }
            
            shiny::runGadget(ui, server, viewer=shiny::dialogViewer("user_login"))
        }
    }else{
        stop("ERROR: The 'miniUI' and 'shiny' packages are required to run the 
              interactive login, please install it and try again.
              To install 'miniUI': install.packages('miniUI')
              To install 'shiny': install.packages('shiny')")
    }
}
    
    
    