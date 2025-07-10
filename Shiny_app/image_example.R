library(shiny)

# Define UI
ui <- fluidPage(
  # Create a row
  fluidRow(
    # Column for text
    column(6, 
           h3("Your Text Here"),
           p("This is an example of text appearing on the left side of the screen.")
    ),
    # Column for the image
    column(6,
           tags$img(src = "digital_abstract.png", height = "400px", width = "auto")
    )
  )
)

# Define server logic
server <- function(input, output) { }

# Run the app
shinyApp(ui = ui, server = server)
