# Load required libraries
library(shiny)
library(shinythemes)
library(shinyjs)
library(dplyr)
library(leaflet)
library(sf)
library(ggiraph)
library(ggplot2)
library(plotly)
library(DT)
library(htmltools)
library(DiagrammeR)

# Source functions.R file
source("functions.R")

#==============================================================================
# UI Components - Helper Functions
#==============================================================================

#' Create custom CSS styles for the app
#' 
#' @return HTML object with CSS styles
create_custom_css <- function() {
  tags$head(
    tags$style(HTML("
      .modal-content {
        box-shadow: 0 4px 8px 0 rgba(0,0,0,0.2);
        background-color: #f7f7f7;
        border-radius: 8px;
        font-family: 'Arial', sans-serif;
      }
      .modal-header, .modal-footer {
        background-color: #e9e9e9;
        border-bottom: 1px solid #ddd;
      }
      .modal-title {
        color: #333;
      }
      .modal-body {
        color: #555;
      }
      .fade {
        animation-name: fadeIn;
        -webkit-animation-name: fadeIn; 
        animation-duration: 0.5s; 
        -webkit-animation-duration: 0.5s;
        animation-timing-function: ease-in-out; 
        -webkit-animation-timing-function: ease-in-out;           
      }
      @keyframes fadeIn {
        0% {opacity: 0;}
        100% {opacity: 1;}
      }
      @-webkit-keyframes fadeIn {
        0% {opacity: 0;}
        100% {opacity: 1;}
      }
      
      .shiny-progress-container {
        top: 0px;
      }
      .progress-bar {
        background-color: #337ab7;
      }
      
      /* Manual Labeling/Micro-Planning styles */
      .classification-box {
        padding: 15px;
        background-color: #fff;
        border-radius: 8px;
        box-shadow: 0 2px 6px rgba(0,0,0,0.1);
        margin-bottom: 15px;
      }
      .classification-instructions {
        padding: 15px;
        background-color: #f8f9fa;
        border-radius: 8px;
        margin-bottom: 15px;
      }
      .classification-summary {
        background-color: #e9ecef;
        border-radius: 8px;
        padding: 15px;
        margin-top: 15px;
      }
      .classification-badge {
        display: inline-block;
        padding: 5px 10px;
        border-radius: 15px;
        color: white;
        font-weight: bold;
        margin: 2px;
      }
      .badge-formal {
        background-color: #0074D9;
      }
      .badge-informal {
        background-color: #FF4136;
      }
      .badge-avoid {
        background-color: #2ECC40;
      }
      .badge-unclassified {
        background-color: #AAAAAA;
        color: #333;
      }
      .urban-threshold-box {
        background-color: #f0f4f8;
        border-left: 4px solid #4a6fa5;
        padding: 15px;
        margin-bottom: 15px;
        border-radius: 4px;
      }
      .reprioritized-box {
        background-color: #fff0f0;
        border-left: 4px solid #dc3545;
        padding: 15px;
        margin-bottom: 15px;
        border-radius: 4px;
      }
      
      /* Net Distribution styles */
      .net-distribution-container {
        font-family: 'Source Sans Pro', sans-serif;
        color: #333;
        padding: 0 15px;
      }
      .dashboard-card {
        background: white;
        border-radius: 8px;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        padding: 20px;
        margin-bottom: 20px;
        transition: all 0.3s ease;
      }
      .dashboard-card:hover {
        box-shadow: 0 6px 12px rgba(0,0,0,0.15);
      }
      .section-header {
        border-left: 4px solid #5D4E6D;
        padding-left: 10px;
        margin-bottom: 20px;
        color: #5D4E6D;
      }
      .action-button {
        background-color: #5D4E6D;
        color: white;
        border: none;
        border-radius: 4px;
        padding: 8px 15px;
        transition: background-color 0.3s ease;
      }
      .action-button:hover {
        background-color: #4A3D57;
      }
      .coverage-container {
        margin: 30px 0;
      }
      .coverage-bar {
        height: 30px;
        border-radius: 15px;
        background: #f0f0f0;
        overflow: hidden;
        box-shadow: inset 0 2px 5px rgba(0,0,0,0.1);
      }
      .coverage-fill {
        height: 100%;
        background: linear-gradient(to right, #4CAF50, #8BC34A);
        border-radius: 15px;
        transition: width 1s ease-in-out;
        display: flex;
        align-items: center;
        justify-content: center;
        color: white;
        font-weight: bold;
        text-shadow: 1px 1px 2px rgba(0,0,0,0.3);
      }
      .stat-panel {
        text-align: center;
        padding: 15px;
        border-radius: 8px;
        margin-bottom: 15px;
        color: white;
      }
      .stat-panel.population {
        background: linear-gradient(135deg, #3949AB, #1E88E5);
      }
      .stat-panel.households {
        background: linear-gradient(135deg, #00897B, #26A69A);
      }
      .stat-panel.nets {
        background: linear-gradient(135deg, #E53935, #EF5350);
      }
      .stat-panel.coverage {
        background: linear-gradient(135deg, #43A047, #66BB6A);
      }
      .stat-value {
        font-size: 24px;
        font-weight: bold;
      }
      .stat-label {
        text-transform: uppercase;
        font-size: 12px;
        opacity: 0.9;
      }
      .strategy-card {
        background: #f9f9f9;
        border: 1px solid #ddd;
        border-radius: 8px;
        padding: 15px;
        margin-bottom: 15px;
        transition: all 0.3s ease;
      }
      .strategy-card.selected {
        background: #e8f5e9;
        border-color: #4CAF50;
        box-shadow: 0 0 8px rgba(76, 175, 80, 0.3);
      }
      .strategy-card:hover {
        box-shadow: 0 4px 8px rgba(0,0,0,0.1);
      }
      .distribution-phases {
        background-color: #f8f9fa;
        border-radius: 8px;
        padding: 15px;
        margin-top: 20px;
      }
      .phase {
        padding: 10px;
        margin-bottom: 10px;
        border-radius: 5px;
      }
      .phase-1 {
        background-color: #e8f5e9;
        border-left: 4px solid #4caf50;
      }
      .phase-2 {
        background-color: #fff3e0;
        border-left: 4px solid #ff9800;
      }
      
      /* Classification badge colors */
      .badge-formal {
        background-color: #0074D9;
      }
      .badge-informal {
        background-color: #FF4136;
      }
      .badge-avoid {
        background-color: #2ECC40;
      }
      .badge-unclassified {
        background-color: #AAAAAA;
        color: #333;
      }
    "))
  )
}

#' Create footer content
#' 
#' @return HTML content for the footer
create_footer_content <- function() {
  tagList(
    tags$br(), tags$br(), tags$br(), tags$br(), tags$br(), tags$br(),
    hr(),
    div(
      style='text-align: center; margin-top: 20px;',
      'Created by the', 
      shiny::HTML('<a href=\'https://www.urban-malaria.com/\' 
                 target=\'_blank\'> Urban Malaria Lab </a>'),
      '@ Loyola University Chicago, Parkinson School of Public Health, Department of Health Informatics and Data Science'
    ),
    br(),
    div(
      style='text-align: center; font-style: italic; font-size: 1.1em; margin-bottom: 20px;',
      tags$strong('If you use this tool in your work, please cite:'),
      br(),
      'Mhlanga, L.,* Boateng, B.O.,* Jamiu, Y., Bamgboye, E.A., Adeniji, H., Ademu, C., Okoronkwo, C., Enang, G. & Ozodiegwu, I.D. (2024).',
      br(),
      'Malaria Reprioritization Tool. Urban Malaria Lab, Loyola University Chicago.',
      br(),
      'Available at: ',
      tags$a(href="https://urbanmalaria.shinyapps.io/mrpt/", 
             target="_blank", 
             "https://urbanmalaria.shinyapps.io/mrpt/")
    ),
    br()
  )
}

#==============================================================================
# UI Tab Components
#==============================================================================

#' Create Instructions tab content
#' 
#' @return tabPanel content for Instructions tab
instructions_tab <- function() {
  tabPanel("Instructions",
           tags$br(),
           fluidRow(
             column(6,
                    h3("Instructions for Using the Malaria Reprioritization Tool"),
                    p("Follow these steps to use the tool effectively:"),
                    h4("1. Preparing Your Data"),
                    tags$ul(
                      tags$li("Ensure you have a CSV file with variable data and the corresponding shapefile for your region of interest."),
                      tags$li("The CSV file should include columns for WardName and various risk factors (e.g., settlement type, enhanced vegetation index, rainfall, etc.)."),
                      tags$li("Compress the shapefile components into a zip file.")
                    ),
                    h4("2. Input Variables (Data and Shapefiles) Tab"),
                    tags$ul( 
                      tags$li("Upload your CSV file and zipped shapefile using the respective upload buttons."),
                      tags$li("Select a variable to visualize from the dropdown menu and click 'Plot Maps' to see its distribution."),
                      tags$li("Use the 'Click to fill in missing values' button if your data has missing values.")
                    ),
                    h4("3. Normalization Tab"),
                    tags$ul( 
                      tags$li("Define how each variable relates to malaria risk (direct or inverse)."),
                      tags$li("Visualize normalized variables to understand their distribution.")
                    ),
                    h4("4. Composite Score Distribution Tab"),
                    tags$ul(
                      tags$li("Select variables to include in your composite risk score."),
                      tags$li("Generate and examine composite score maps for different variable combinations.")
                    ),
                    h4("5. Box and Whisker Plot Tab"),
                    tags$ul( 
                      tags$li("Analyze the distribution of vulnerability scores across wards."),
                      tags$li("Toggle between box plot and map views to understand risk distribution.")
                    ),
                    h4("6. Decision Tree Tab"),
                    tags$ul(
                      tags$li("Explore a visual representation of the decision-making process in creating the malaria risk maps."),
                      tags$li("Understand how variable selections impact the final risk assessment.")
                    ),
                    h4("7. Manual Labelling Tab"),
                    tags$ul(
                      tags$li("Visualize detailed maps of individual wards based on rankings from previous analyses."),
                      tags$li("Examine settlement types and other factors within specific wards.")
                    ),
                    h4("Data Privacy"),
                    p("Please ensure that you have the necessary permissions to use and upload your data. Do not upload sensitive or personal data without proper authorization."),
                    p(
                      "For issues or technical support with the application, email ",
                      tags$a(href="mailto:lmhlanga@luc.edu", "lmhlanga@luc.edu"),
                      " or ",
                      tags$a(href="mailto:bboateng1@luc.edu", "bboateng1@luc.edu")
                    ),
                    h4("Funding"),
                    p("This work and the authors are supported by the Bill and Melinda Gates Foundation (Investment ID: INV-036449).")
             ),
             column(6, 
                    tags$br(), tags$br(), tags$br(), tags$br(), 
                    tags$img(src = "digital_abstract.png", height = "600px", width = "auto"),
                    style = 'text-align: center',
                    tags$h6("The figure above shows a digital abstract first presented in Ozodiegwu et-al (in-press), 
                and it shows how the application was used to develop the malaria risk map in Ilorin")
             )
           )
  )
}

#' Create Input Variables tab content
#' 
#' @return tabPanel content for Input Variables tab
input_variables_tab <- function() {
  tabPanel("Input variables (data and shapefiles)", 
           fluidRow(
             column(3,
                    h3("Upload the shapefile and analysis data:"),
                    fileInput("file_csv", "Choose CSV File", 
                              accept = c(".csv", ".xlsx", ".xls")),
                    fileInput("file_shp", "Choose a Zipped Shapefile", 
                              accept = c('application/zip', 'application/x-zip-compressed', 
                                         'multipart/x-zip', 'application/x-compress', 
                                         'application/x-compressed', 'application/gzip')),
                    uiOutput("reopen_mismatch_modal"),
                    # Add the duplicate info UI
                    #uiOutput("duplicate_info"),
                    hr(),
                    h4("Select Variable to Visualize"),
                    uiOutput("variable_select"),
                    actionButton("plot_data", "Plot Maps"),
                    hr(),
                    actionButton("data_cleaning", "Click to fill in missing values")
             ),
             column(9,
                    fluidRow(
                      uiOutput("plotLayout")
                    ),
                    htmlOutput("visualizationExplanation"),
                    tags$p(style = "font-style: italic; margin-top: 10px;", 
                           "Hover over the maps to see detailed information for each ward.")
             )
           )
  )
}

#' Create Normalization tab content
#' 
#' @return tabPanel content for Normalization tab
normalization_tab <- function() {
  tabPanel("Normalization", 
           sidebarLayout(
             sidebarPanel(
               actionButton("specify_relationships", "SPECIFY VARIABLE RELATIONSHIPS"),
               tags$br(), 
               tags$br(),
               uiOutput("normalized_variable_select"),
               tags$br(),
               actionButton("plot_normalized", "PLOT NORMALIZED MAP")
             ),
             mainPanel(
               conditionalPanel(
                 condition = "input.plot_normalized > 0", 
                 girafeOutput("normalizationplot")
               )
             )
           ),
           conditionalPanel(
             condition = "input.plot_normalized > 0",
             tags$br(),
             tags$p(style = "font-style: italic; margin-top: 10px;",
                    "Hover over the map to see detailed information for each ward."),
             tags$div(
               style = "margin-top: 20px; padding: 10px; background-color: #f0f0f0; border-radius: 5px;",
               tags$h4("Understanding the Normalized Plot", style = "text-align: center;"),
               tags$p("The plot shows the distribution of the variables selected for
                   evaluation across the region of interest after they have been
                   normalized using the min-max method. The values are now all on
                   the same scale ranging from 0 to 1. This range of values are put
                   into 5 classes (see legend). The plot highlights which of the
                   variables if used in the algorithm will have more influence in the
                   composite score.", style = "text-align: justify;")
             )
           )
  )
}

#' Create Composite Score Distribution tab content
#' 
#' @return tabPanel content for Composite Score Distribution tab
composite_score_tab <- function() {
  tabPanel("Composite Score distribution", 
           tags$br(), tags$br(),
           sidebarLayout(
             sidebarPanel(
               tags$h4("Select a variable in the dataset to visualise:"),
               tags$p("Select at least two variables:", style = "color: black;"),
               uiOutput("composite_variable_select"),
               div(id = "progress-bar-container", style = "margin-top: 10px;"),
               actionButton("plot_button", "Calculate")
             ),
             mainPanel(
               uiOutput("mapPlot"),
               conditionalPanel(
                 condition = "input.plot_button > 0",
                 tags$br(),
                 tags$div(
                   style = "text-align: justify; font-size: 12px; color: #666;",
                   "The maps above show the distribution of malaria risk scores across different wards, calculated using various combinations of variables. Each map represents a different model, with the variables used listed in the title. The color scale ranges from yellow (very low risk) to dark red (very high risk). This visualization helps identify areas of high concern and compare how different combinations of factors affect the risk assessment."
                 ) #,
                 # tags$br(),
                 # tableOutput("flagged_models_table"),
                 # tags$p(style = "font-style: italic; margin-top: 10px; font-size: 12px;",
                        # "Hover over the maps to see detailed information for each ward.")
               )
             )
           )
  )
}

#' Create Box and Whisker Plot tab content
#' 
#' @return tabPanel content for Box and Whisker Plot tab
box_whisker_tab <- function() {
  tabPanel("Box and Whisker Plot", 
           fluidRow(
             column(12,
                    h3("Ward Vulnerability Distribution"),
                    p("This visualization shows the distribution of vulnerability scores across wards, helping to identify areas of high and low risk."),
                    div(style = "margin-bottom: 20px;",
                        checkboxInput("show_map", "Show Map View", value = FALSE)
                    ),
                    
                    # Fixed urban threshold info - only shown when map is displayed
                    conditionalPanel(
                      condition = "input.show_map == true",
                      wellPanel(
                        style = "background-color: #f7f7f7; border-radius: 8px; padding: 15px; margin-bottom: 20px;",
                        div(style = "display: flex; align-items: center; justify-content: space-between;",
                            div(
                              h4(style = "margin-top: 0;", "Urban Extent Filter"),
                              p(style = "color: #666; margin-bottom: 5px;", 
                                "Set the urban extent threshold for determining ward prioritization.")
                            ),
                            div(
                              numericInput("urban_threshold", "Urban Threshold (%)", 
                                           value = 30, min = 0, max = 100, step = 5)
                            ),
                            div(
                              checkboxInput("show_threshold_map", "Apply Urban Threshold", value = FALSE)
                            )
                        )
                      )
                    )
             )
           ),
           fluidRow(
             column(8,
                    # Use the dynamic content UI
                    uiOutput("boxWhiskerContent")
             ),
             column(4,
                    conditionalPanel(
                      condition = "input.show_map == false",
                      wellPanel(
                        style = "background-color: #f5f5f5; border: 1px solid #e3e3e3; border-radius: 4px; padding: 15px; height: 600px; overflow-y: auto;",
                        h4("Understanding the Box and Whisker Plot"),
                        p("This plot visualizes the distribution of median vulnerability scores across wards, ranked from most vulnerable (top) to least vulnerable (bottom)."),
                        p("Each horizontal bar represents a ward:"),
                        tags$ul(
                          tags$li("The box shows the interquartile range (IQR) of vulnerability scores."),
                          tags$li("The vertical line inside the box represents the median score."),
                          tags$li("The whiskers extend to show the full range of scores, excluding outliers."),
                          tags$li("Any points beyond the whiskers are considered outliers.")
                        ),
                        p("This visualization helps identify which areas may need more attention or resources in malaria prevention efforts."),
                        
                        # Add pagination explanation
                        hr(),
                        h5("Pagination Controls"),
                        p("For datasets with many wards, the visualization is split into pages for better readability."),
                        tags$ul(
                          tags$li("Use the arrow buttons to navigate between pages"),
                          tags$li("Adjust 'Wards per page' to show more or fewer wards at once"),
                          tags$li("The most vulnerable wards appear on the first page")
                        )
                      )
                    ),
                    conditionalPanel(
                      condition = "input.show_map == true && input.show_threshold_map == false",
                      wellPanel(
                        style = "background-color: #f5f5f5; border: 1px solid #e3e3e3; border-radius: 4px; padding: 15px; height: 500px; overflow-y: auto;",
                        h4("Understanding the Vulnerability Map"),
                        p("This map displays the overall vulnerability rank for each ward, providing a geographic perspective on malaria risk."),
                        p("Key features:"),
                        tags$ul(
                          tags$li("Color intensity represents the vulnerability rank, with darker colors indicating higher vulnerability (higher rank) and lighter colors indicating lower vulnerability (lower rank)."),
                          tags$li("The color scale ranges from light yellow (least vulnerable, lowest rank, 1) to dark purple (most vulnerable, highest rank)."),
                          tags$li("Hover over each ward to see its name and exact rank."),
                          tags$li("The map allows for easy identification of high-risk areas and spatial patterns in vulnerability.")
                        ),
                        p("Use this map to identify priority areas for intervention. Wards with higher ranks (darker colors) may require more immediate attention and resources in malaria prevention efforts.")
                      )
                    ),
                    conditionalPanel(
                      condition = "input.show_map == true && input.show_threshold_map == true",
                      wellPanel(
                        style = "background-color: #f5f5f5; border: 1px solid #e3e3e3; border-radius: 4px; padding: 15px; height: 500px; overflow-y: auto;",
                        h4("Urban Extent Filtered Map"),
                        uiOutput("threshold_explanation_text"),
                        p("Key features:"),
                        tags$ul(
                          tags$li("Colored wards meet the urban threshold and are prioritized for net distribution."),
                          tags$li("Gray wards are below the urban threshold and are candidates for de-prioritization."),
                          tags$li("Color intensity of prioritized wards still represents vulnerability rank, with darker colors indicating higher vulnerability."),
                          tags$li("Hover over wards to see detailed information, including urban percentage and rank.")
                        ),
                        hr(),
                        h5("Prioritization Statistics"),
                        tableOutput("deprioritization_stats")
                      )
                    )
             )
           )
  )
}

#' Create Decision Tree tab content
#' 
#' @return tabPanel content for Decision Tree tab
decision_tree_tab <- function() {
  tabPanel("Decision Tree",
           fluidRow(
             column(12,
                    div(
                      style = "padding: 20px; background-color: #FFFFFF; border-radius: 8px; margin-bottom: 20px;",
                      
                      # Download buttons
                      div(
                        style = "margin-bottom: 20px;",
                        tags$head(
                          tags$script("
                $(document).on('shiny:error', function(event) {
                  if(event.name === 'downloadPNG' || event.name === 'downloadPDF') {
                    alert('Error during download. Please try again.');
                  }
                });
              ")
                        ),
                        downloadButton("downloadPNG", "Download PNG", 
                                       style = "margin-right: 10px; background-color: #5D4E6D; color: white;"),
                        downloadButton("downloadPDF", "Download PDF",
                                       style = "background-color: #5D4E6D; color: white;")
                      ),
                      
                      # Main visualization container
                      div(
                        style = "background-color: white; padding: 20px; border-radius: 8px;",
                        grVizOutput("decisionTree", width = "100%", height = "600px")
                      ),
                      
                      # Help text
                      div(
                        style = "margin-top: 20px; padding: 15px; background-color: #F8F9FA; border-radius: 8px;",
                        p("Click the download buttons above to save the decision tree in your preferred format.",
                          style = "color: #666; font-style: italic;"),
                        p("Note: The download may take a few seconds to complete.",
                          style = "color: #666; font-style: italic;")
                      )
                    )
             )
           )
  )
}

#' Create Manual Labeling tab content
#' 
#' @return tabPanel content for Manual Labeling tab
manual_labeling_tab <- function() {
  tabPanel("Manual Labeling/Micro-Planning",
           div(class = "container-fluid",
               fluidRow(
                 # Left sidebar
                 column(4,
                        div(class = "urban-threshold-box",
                            h4("Urban Extent Threshold", style = "margin-top: 0;"),
                            # Use uiOutput to dynamically render this text with current threshold
                            uiOutput("dynamic_threshold_text"),
                            
                            div(style = "background-color: #e9f0f6; padding: 15px; border-radius: 5px; margin-top: 10px;",
                                h5("Statistics", style = "margin-top: 0; color: #3949AB;"),
                                uiOutput("manual_deprioritization_stats")
                            )
                        ),
                        
                        # Ward selection container - simplified to only show wards above threshold
                        div(class = "classification-box",
                            h3("Ward Selection", style = "margin-top: 0;"),
                            p("Select a ward for manual inspection and labeling:"),
                            
                            # Dynamic ward selection that only shows wards meeting the threshold
                            uiOutput("filtered_ward_select"),
                            
                            # Urban extent info for selected ward
                            conditionalPanel(
                              condition = "input.selected_ward",
                              div(style = "margin: 15px 0; padding: 10px; background-color: #e8f4f8; border-radius: 5px;",
                                  uiOutput("selected_ward_urban_info")
                              )
                            ),
                            
                            hr(),
                            
                            # Grid settings
                            h4("Grid Settings"),
                            checkboxInput("enable_grid", "Enable Grid Analysis", value = TRUE),
                            
                            conditionalPanel(
                              condition = "input.enable_grid == true",
                              div(
                                selectInput("grid_cell_size", "Grid Cell Size:", 
                                            choices = c("500m × 500m" = 500), 
                                            selected = 500),
                                actionButton("plot_ward_map", "GENERATE GRID & PLOT MAP", 
                                             icon = icon("map-marked-alt"),
                                             style = "width: 100%; margin-bottom: 15px;")
                              )
                            ),
                            
                            # Classification count badges
                            conditionalPanel(
                              condition = "input.plot_ward_map > 0",
                              div(class = "classification-summary",
                                  h4("Classification Summary"),
                                  uiOutput("classification_badges"),
                                  p(style = "margin-top: 10px; font-style: italic; color: #666; font-size: 12px;", 
                                    "Click on grid cells in the map to classify them.")
                              )
                            )
                        ),
                        
                        # Control buttons - UPDATED with Download Map button
                        div(class = "classification-box",
                            div(style = "display: flex; margin-bottom: 10px;",
                                actionButton("clear_annotations", "CLEAR ALL ANNOTATIONS", 
                                             icon = icon("eraser"),
                                             style = "flex: 1;")
                            ),
                            div(style = "display: flex;",
                                downloadButton("download_grid", "DOWNLOAD GRID SHAPEFILE",
                                               style = "flex: 1; margin-right: 5px;"),
                                downloadButton("download_annotations", "DOWNLOAD ANNOTATIONS",
                                               style = "flex: 1; margin-left: 5px;")
                            ),
                            # NEW: Add Download Map button
                            br(),
                            downloadButton("download_map", "DOWNLOAD CLASSIFIED MAP",
                                           icon = icon("image"),
                                           style = "width: 100%; margin-top: 10px; background-color: #5D4E6D; color: white;")
                        ),
                        
                        # Instructions box with updated explanation
                        div(class = "classification-instructions",
                            h4("Classification Purpose", style = "margin-top: 0;"),
                            p("This tool allows you to identify habitable areas within re-prioritized wards (areas that meet the selected urban threshold) that may still need bed nets."),
                            p("By classifying grid cells, you help ensure that populated areas receive appropriate coverage despite the ward's overall urban status."),
                            h4("Classification Criteria"),
                            tags$ul(
                              tags$li(tags$strong("Formal:"), " Planned layout, good road access, regular building patterns, typically with infrastructure"),
                              tags$li(tags$strong("Informal:"), " Unplanned layout, limited road access, irregular building patterns"),
                              tags$li(tags$strong("No Buildings/Avoid Area:"), " Areas without structures or areas to exclude from bed net distribution")
                            ),
                            p("Click on any grid cell in the map to classify it using the integrated checklist.")
                        )
                 ),
                 
                 # Right panel with map
                 column(8,
                        div(id = "status_box", class = "reprioritized-box",
                            h4("Re-prioritized Area Analysis", style = "margin-top: 0;"),
                            uiOutput("dynamic_status_text")
                        ),
                        div(style = "position: relative;",
                            div(style = "position: absolute; top: 10px; right: 10px; z-index: 1000;",
                                actionButton("grid_help", "", icon = icon("question-circle"), 
                                             style = "padding: 6px 10px; background-color: white; border-radius: 50%;")
                            ),
                            div(id = "grid_help_content", style = "display: none; position: absolute; top: 50px; right: 10px; z-index: 1000; background-color: white; border-radius: 8px; padding: 15px; width: 300px; box-shadow: 0 2px 8px rgba(0,0,0,0.2);",
                                h4("How to Use the Grid", style = "margin-top: 0;"),
                                uiOutput("dynamic_grid_help_content"),
                                actionButton("close_help", "Close", style = "width: 100%;")
                            ),
                            leafletOutput("ward_map", height = 700)
                        )
                 )
               )
           )
  )
}

#' Create Net Distribution tab content
#' 
#' @return tabPanel content for Net Distribution tab
net_distribution_tab <- function() {
  tabPanel("Net Distribution", 
           div(class = "net-distribution-container",
               fluidRow(
                 column(12,
                        div(class = "dashboard-card",
                            h2(icon("mosquito-net"), "Net Distribution Planning", 
                               style = "color: #5D4E6D; margin-top: 0;"),
                            p("Plan optimal distribution of bed nets based on the urban extent threshold and ward vulnerability rankings.", 
                              style = "font-size: 16px; color: #666;")
                        ),
                        uiOutput("population_data_source")
                 )
               ),
               
               fluidRow(
                 # Left column - Settings inputs
                 column(4,
                        div(class = "dashboard-card",
                            h3(class = "section-header", "Distribution Settings"),
                            
                            # Simple inputs
                            h4("Resource Allocation", style = "color: #E53935;"),
                            numericInput("total_nets", "Total Bed Nets Available", 
                                         value = 10000, min = 1,
                                         width = "100%"),
                            
                            numericInput("avg_household_size", "Average Household Size", 
                                         value = 5, min = 1, max = 10, step = 0.1,
                                         width = "100%"),
                            
                            helpText("Standard distribution is 1 net per 1.8 people, with a minimum of 1 net per household"),
                            
                            # Distribution strategy selection
                            h4("Distribution Strategy", style = "color: #5D4E6D;"),
                            
                            # Using a two-phase approach explanation with DYNAMIC threshold
                            div(class = "distribution-phases",
                                h5("Two-Phase Distribution Approach"),
                                # Dynamic phase descriptions
                                uiOutput("phase1_description"),
                                uiOutput("phase2_description")
                            ),
                            
                            hr(style = "margin: 20px 0; border-color: #eee;"),
                            
                            # Action buttons
                            actionButton("calculate_distribution", "Calculate Distribution", 
                                         class = "btn-primary action-button", 
                                         style = "width: 100%; margin-bottom: 15px; font-size: 16px;"),
                            downloadButton("download_distribution", "Download Distribution Plan", 
                                           style = "width: 100%; background-color: #00897B;")
                        )
                 ),
                 
                 # Right column - Results
                 column(8,
                        div(class = "dashboard-card",
                            # Tabs for different results views
                            tabsetPanel(
                              tabPanel("Dashboard",
                                       br(),
                                       
                                       # Statistics cards in a grid
                                       fluidRow(
                                         column(3, 
                                                div(class = "stat-panel population",
                                                    uiOutput("stat_population")
                                                )
                                         ),
                                         column(3, 
                                                div(class = "stat-panel households",
                                                    uiOutput("stat_households")
                                                )
                                         ),
                                         column(3, 
                                                div(class = "stat-panel nets",
                                                    uiOutput("stat_nets")
                                                )
                                         ),
                                         column(3, 
                                                div(class = "stat-panel coverage",
                                                    uiOutput("stat_coverage")
                                                )
                                         )
                                       ),
                                       
                                       # Coverage visualization
                                       div(class = "coverage-container",
                                           h4("Population Coverage"),
                                           uiOutput("population_coverage_bar")
                                       ),
                                       
                                       # Distribution summary info - now DYNAMIC
                                       uiOutput("distribution_summary"),
                                       
                                       # Map visualization
                                       div(class = "map-container",
                                           h4("Net Distribution Map", style = "margin-top: 0; margin-bottom: 10px;"),
                                           # Dynamic map description
                                           uiOutput("map_description"),
                                           leafletOutput("distribution_map", height = 400)
                                       )
                              ),
                              
                              tabPanel("Ward Coverage",
                                       br(),
                                       p("This view shows the distribution of nets across wards based on vulnerability ranking.", 
                                         style = "margin-bottom: 20px;"),
                                       
                                       # Dynamic description
                                       uiOutput("ward_coverage_description"),
                                       
                                       DTOutput("ward_coverage_table")
                              ),
                              
                              tabPanel("Manual Grid Classifications",
                                       br(),
                                       p("This view shows grid cell classifications that affect net distribution.", 
                                         style = "margin-bottom: 20px;"),
                                       
                                       div(
                                         style = "margin-top: 15px; padding: 10px; background-color: #f8f9fa; border-radius: 5px;",
                                         h5("About Grid Micro-Planning", style = "margin-top: 0; color: #5D4E6D;"),
                                         p("The grid system divides the ward into equal-sized square cells for detailed planning:"),
                                         tags$ul(
                                           tags$li("Each cell represents a configurable area on the ground (adjustable via the Grid Cell Size dropdown)"),
                                           tags$li("Classify cells based on settlement patterns to identify areas requiring bed nets"),
                                           tags$li("Downloaded data includes coordinates for field operations and implementation"),
                                           tags$li("This micro-planning approach allows for targeted intervention within re-prioritized wards")
                                         ),
                                         p("Note: Cell size can be adjusted based on operational requirements and local geography.")
                                       ),
                                       
                                       DTOutput("manual_override_table")
                              )
                            )
                        )
                 )
               )
           )
  )
}

#==============================================================================
# Main UI Definition
#==============================================================================

#' Define UI for the Malaria Reprioritization Tool
#' 
#' @return UI definition
ui <- fluidPage(
  theme = shinythemes::shinytheme("sandstone"),
  useShinyjs(),  
  titlePanel("Malaria Reprioritization Tool"),
  
  # Add custom CSS
  create_custom_css(),
  
  br(),
  
  # Main tabs
  tabsetPanel(
    instructions_tab(),
    input_variables_tab(),
    normalization_tab(),
    composite_score_tab(),
    box_whisker_tab(),
    decision_tree_tab(),
    manual_labeling_tab(),
    net_distribution_tab()
  ),
  
  # Footer 
  create_footer_content()
)

#==============================================================================
# Reactive Values Definition
#==============================================================================

#' Initialize reactive values for the app
#' 
#' @param session Shiny session
#' @return List of reactive values
initialize_reactives <- function(session) {
  rv <- reactiveValues(
    # Data storage
    raw_data = NULL,
    cleaned_data = NULL,
    normalized_data = reactiveVal(NULL),
    shp_data = NULL,
    mismatched_wards = NULL,
    
    # Processing settings and results
    na_handling_methods = list(),
    variable_relationships = list(),
    composite_scores = NULL,
    data = NULL,
    output_data = NULL,
    
    # Cleaning status
    cleaning_performed = reactiveVal(FALSE),
    
    # Grid and classification data
    gridded_wards = reactiveVal(NULL),
    grid_annotations = reactiveVal(data.frame(
      GridID = integer(), 
      WardName = character(), 
      Classification = character(),
      Timestamp = character(),
      Method = character(),
      stringsAsFactors = FALSE
    )),
    
    # Distribution results
    net_distribution_results = reactiveVal(NULL),
    grid_overrides = reactiveVal(NULL),
    ward_population = reactiveVal(NULL),
    
    # Miscellaneous
    flagged_models = reactiveVal(NULL),
    corrected_wardnames = reactiveVal(data.frame(
      original = character(), 
      corrected = character(), 
      stringsAsFactors = FALSE
    )),
    
    # Track progress for decision tree
    decision_tree_progress = reactiveVal(list(
      data_loaded = FALSE,
      variables_selected = FALSE,
      normalization_done = FALSE,
      composite_scores_calculated = FALSE
    )),
    
    # NEW: Pagination for box plot
    boxplot_pagination = NULL,
    current_page = 1,
    
    # NEW: Duplicate ward handling
    duplicate_wards_handled = FALSE,
    original_to_new_wardnames = NULL
  )
  
  # Add a function to check if cleaning is needed
  rv$needs_cleaning <- reactive({
    req(rv$raw_data, rv$shp_data) # Ensure both files are loaded
    length(rv$na_columns) > 0 || (!is.null(rv$mismatched_wards) && nrow(rv$mismatched_wards) > 0)
  })
  
  # Create a reactive for deprioritized wards
  rv$deprioritized_wards_list <- reactive({
    req(rv$ward_rankings, rv$shp_data, input$urban_threshold)
    
    # Get the threshold value
    threshold <- as.numeric(input$urban_threshold)
    
    # Only generate when threshold is selected
    if (threshold <= 0) return(NULL)
    
    # Apply urban extent threshold filtering
    filtered_data <- filter_by_urban_extent(rv$shp_data, threshold)
    
    # Join with vulnerability rankings
    map_data <- left_join(filtered_data, rv$ward_rankings, by = "WardName")
    
    # Get list of de-prioritized wards
    deprioritized_wards <- map_data %>%
      filter(!MeetsThreshold) %>%
      arrange(overall_rank) %>%
      select(WardName, UrbanPercent, overall_rank)
    
    return(deprioritized_wards)
  })
  
  # Add a reactive to track de-prioritized wards based on urban extent threshold
  rv$deprioritized_wards <- reactiveVal(NULL)
  
  return(rv)
}



#==============================================================================
# Server Function
#==============================================================================

server <- function(input, output, session) {
  
  #============================================================================
  # Initialize reactive values
  #============================================================================
  rv <- initialize_reactives(session)
  
  #============================================================================
  # Initial App Setup
  #============================================================================
  
  # Show initial welcome modal
  showModal(modalDialog(
    title = "Information",
    HTML("Welcome to the reprioritization web application, a powerful tool for 
    visualizing the distribution of variables associated with malaria risk 
    at a granular level. It uniquely focuses on a finer scale, analyzing data 
    down to the ward level rather than the broader Local Government Area (LGA).
    <br><br>
    This detailed approach, combined with geocoding technology, allows for 
    enhanced reprioritization, enabling stakeholders to make decisions at the 
    smallest scale of settlement type. This specificity ensures that 
    \"reprioritization of resources like bed nets happens effectively, targeting 
    areas with the highest need and optimizing malaria control efforts.\"")
  ))
  
  # Auto-close modal after 5 minutes
  shinyjs::runjs("setTimeout(function() { $('.modal').modal('hide'); }, 300000);")
  
  #============================================================================
  # Data Upload and Processing
  #============================================================================
  
  #' Process CSV file upload
  #' Process CSV file upload
  observeEvent(input$file_csv, {
    req(input$file_csv)
    
    # Process CSV with duplicate handling
    rv$raw_data <- process_csv_with_duplicate_handling(input$file_csv$datapath)
    
    # Check for missing values
    missing_data <- check_missing_values(rv$raw_data)
    rv$na_columns <- missing_data$columns
    rv$raw_data <- missing_data$data
    
    # Set cleaned_data to raw_data initially
    rv$cleaned_data <- rv$raw_data
    
    # Get columns after WardName
    columns_after_wardname <- get_columns_after_wardname(rv$cleaned_data)
    
    # Populate variable relationships for columns after WardName
    rv$variable_relationships <- setNames(rep("direct", length(columns_after_wardname)), columns_after_wardname)
    
    # Store information about duplicate handling
    if ("OriginalWardName" %in% names(rv$raw_data)) {
      rv$duplicate_wards_handled <- TRUE
      rv$original_to_new_wardnames <- rv$raw_data %>%
        select(OriginalWardName, WardName) %>%
        distinct()
      
      showNotification(paste("Detected and resolved", 
                             nrow(rv$original_to_new_wardnames) - length(unique(rv$original_to_new_wardnames$OriginalWardName)), 
                             "duplicate ward names."), 
                       type = "warning", duration = 5)
    } else {
      rv$duplicate_wards_handled <- FALSE
    }
    
    # Check for missing values notification
    if (rv$needs_cleaning()) {
      if (length(rv$na_columns) > 0) {
        showNotification(paste("Warning: Missing values (NAs) found in columns:", paste(rv$na_columns, collapse = ", ")), 
                         type = "warning")
      }
    } else {
      showNotification("Data is already clean. No cleaning necessary.", type = "message")
    }
  })
  
  #' Process shapefile upload
  observeEvent(input$file_shp, {
    req(input$file_shp)
    
    temp_dir <- tempdir()
    unzip(input$file_shp$datapath, exdir = temp_dir)
    shapefile_files <- list.files(temp_dir, pattern = "\\.shp$", full.names = TRUE)
    
    if (length(shapefile_files) > 0) {
      tryCatch({
        # Process shapefile with duplicate handling, using CSV data if available
        if (!is.null(rv$raw_data)) {
          rv$shp_data <- process_shapefile_with_duplicate_handling(shapefile_files[1], rv$raw_data)
        } else {
          # If CSV not loaded yet, just read the shapefile normally
          # We'll handle duplicates later when both files are available
          rv$shp_data <- st_read(shapefile_files[1], quiet = TRUE)
          
          if ("Ward" %in% names(rv$shp_data) && !"WardName" %in% names(rv$shp_data)) {
            rv$shp_data <- rv$shp_data %>% rename(WardName = Ward)
          }
        }
        
        # Ensure Urban is character type
        rv$shp_data <- rv$shp_data %>% 
          mutate(Urban = as.character(Urban))
        
        showNotification("Shapefile loaded successfully.", type = "message")
        
        # If both files are loaded, check for ward name mismatches
        if (!is.null(rv$raw_data)) {
          rv$mismatched_wards <- check_wardname_mismatches(rv$raw_data, rv$shp_data)
          if (!is.null(rv$mismatched_wards) && nrow(rv$mismatched_wards) > 0) {
            showModal(wardNameMismatchModal(rv$mismatched_wards))
          }
        }
      }, error = function(e) {
        showNotification(paste("Error loading shapefile:", e$message), type = "error")
      })
    } else {
      showNotification("No .shp file found in the uploaded zip file.", type = "error")
    }
  })
  
  #' Generate UI for ward name mismatch modal
  #' 
  #' @param mismatches Data frame of mismatched ward names
  #' @return Modal dialog UI
  wardNameMismatchModal <- function(mismatches) {
    req(mismatches)
    corrected_names <- rv$corrected_wardnames()
    
    # Sort mismatches alphabetically by CSV_WardName
    mismatches <- mismatches[order(mismatches$CSV_WardName), ]
    
    modalDialog(
      title = "Data Cleaning",
      
      tags$div(
        tags$h4("Warning: Wardname Mismatches Detected"),
        tags$p("The following ward names in the scoring data do not match with the shapefile:"),
        tags$ul(
          lapply(mismatches$CSV_WardName, function(ward) {
            tags$li(ward)
          })
        ),
        tags$p("Please correct these mismatches before proceeding.")
      ),
      
      lapply(1:nrow(mismatches), function(i) {
        ward <- mismatches$CSV_WardName[i]
        options <- c("None", sort(mismatches$Shapefile_Options[[i]]))  # Sort options alphabetically
        
        selected_value <- if (!is.null(corrected_names) && nrow(corrected_names) > 0) {
          corrected <- corrected_names$corrected[corrected_names$original == ward]
          if (length(corrected) > 0) corrected else "None"
        } else {
          "None"
        }
        
        tags$div(
          tags$h4(ward),
          selectInput(paste0("mismatch_", i), label = NULL, choices = options, selected = selected_value)
        )
      }),
      
      footer = tagList(
        actionButton("apply_corrections", "Apply Corrections"),
        modalButton("Cancel")
      ),
      size = "l"
    )
  }
  
  #' Apply ward name corrections
  observeEvent(input$apply_corrections, {
    req(rv$mismatched_wards, rv$raw_data)
    
    corrected_names <- data.frame(
      original = rv$raw_data$WardName,
      corrected = rv$raw_data$WardName,
      stringsAsFactors = FALSE
    )
    
    for (i in 1:nrow(rv$mismatched_wards)) {
      old_name <- rv$mismatched_wards$CSV_WardName[i]
      new_name <- input[[paste0("mismatch_", i)]]
      
      if (new_name != "None") {
        corrected_names$corrected[corrected_names$original == old_name] <- new_name
      }
    }
    
    rv$corrected_wardnames(corrected_names)
    rv$raw_data$WardName <- corrected_names$corrected
    
    # Check for remaining mismatches
    remaining_mismatches <- check_wardname_mismatches(rv$raw_data, rv$shp_data)
    
    if (is.null(remaining_mismatches)) {
      removeModal()
      showNotification("All ward name mismatches have been corrected.", type = "message")
    } else {
      rv$mismatched_wards <- remaining_mismatches
      showModal(wardNameMismatchModal(remaining_mismatches))
    }
  })
  
  #' UI for reopening ward name mismatch modal
  output$reopen_mismatch_modal <- renderUI({
    req(rv$mismatched_wards)
    if (!is.null(rv$mismatched_wards) && nrow(rv$mismatched_wards) > 0) {
      actionButton("reopen_mismatch", "Review Ward Name Mismatches", 
                   style = "margin-top: 10px; margin-bottom: 10px;")
    }
  })
  
  #' Reopen ward name mismatch modal
  observeEvent(input$reopen_mismatch, {
    req(rv$mismatched_wards)
    showModal(wardNameMismatchModal(rv$mismatched_wards))
  })
  
  #' Generate UI for variable selection dropdown
  output$variable_select <- renderUI({
    req(rv$raw_data)
    columns_after_wardname <- get_columns_after_wardname(rv$raw_data)
    selectInput("visualize_var", "Select Variable to Visualize", 
                choices = columns_after_wardname,
                selected = columns_after_wardname[1])
  })
  
  #============================================================================
  # Data Cleaning
  #============================================================================
  
  
  # Add to UI in the Input Variables tab
  output$duplicate_info <- renderUI({
    req(rv$duplicate_wards_handled, rv$original_to_new_wardnames)
    
    if (rv$duplicate_wards_handled) {
      n_duplicates <- nrow(rv$original_to_new_wardnames) - length(unique(rv$original_to_new_wardnames$OriginalWardName))
      
      if (n_duplicates > 0) {
        div(
          style = "margin-top: 15px; padding: 10px; background-color: #fff3cd; border-radius: 5px; border-left: 5px solid #ffc107;",
          h4("Duplicate Ward Names Detected", style = "margin-top: 0; color: #856404;"),
          p("The tool detected and resolved duplicate ward names in your data. The following wards have been renamed to ensure uniqueness:"),
          div(
            style = "max-height: 200px; overflow-y: auto; margin-top: 10px; background-color: #fff; padding: 10px; border-radius: 5px; border: 1px solid #ddd;",
            renderTable({
              dups <- rv$original_to_new_wardnames %>%
                group_by(OriginalWardName) %>%
                filter(n() > 1) %>%
                ungroup() %>%
                select(OriginalWardName, WardName) %>%
                rename(`Original Ward Name` = OriginalWardName, 
                       `New Unique Identifier` = WardName)
              dups
            }, rownames = FALSE, width = "100%")
          ),
          p(style = "font-style: italic; margin-top: 10px;", 
            "These unique identifiers will be used throughout all analysis steps.")
        )
      } else {
        NULL  # If no duplicates found, don't show anything
      }
    } else {
      NULL
    }
  })
  
  
  #' Show data cleaning modal
  observeEvent(input$data_cleaning, {
    req(rv$raw_data, rv$shp_data)
    
    showModal(modalDialog(
      title = "Data Cleaning",
      
      h4("Columns with Missing Values (NAs)"),
      tags$ul(
        tags$li(strong("Spatial neighbor mean:"), "Replaces missing values with the mean of neighboring wards."),
        tags$li(strong("Region mean:"), "Replaces missing values with the mean of the entire region."),
        tags$li(strong("Region mode:"), "Replaces missing values with the most common value in the region.")
      ),
      lapply(rv$na_columns, function(col) {
        fluidRow(
          column(6, h5(col)),
          column(6, 
                 selectInput(paste0("na_handling_", col), NULL,
                             choices = c("Spatial neighbor mean" = "spatial neighbor mean",
                                         "Region mean" = "region mean",
                                         "Region mode" = "region mode"),
                             selected = rv$na_handling_methods[[col]] %||% "spatial neighbor mean")
          )
        )
      }),
      
      footer = tagList(
        modalButton("Cancel"),
        actionButton("apply_na_handling", "Apply")
      )
    ))
  })
  
  #' Update NA handling method
  observeEvent(input$na_handling, {
    rv$na_handling_method <- input$na_handling
  })
  
  #' Apply data cleaning
  observeEvent(input$apply_na_handling, {
    req(rv$raw_data, rv$na_columns)
    
    rv$cleaned_data <- rv$raw_data
    
    # Pre-pull all methods at once
    methods_selected <- setNames(
      lapply(rv$na_columns, function(col) input[[paste0("na_handling_", col)]]),
      rv$na_columns
    )
    
    # Save all methods at once
    rv$na_handling_methods <- methods_selected
    
    # Process all columns sequentially
    for (col in rv$na_columns) {
      method <- methods_selected[[col]]
      
      # Update cleaned_data only once per column
      if (method == "spatial neighbor mean") {
        rv$cleaned_data <- handle_na_neighbor_mean(rv$cleaned_data, rv$shp_data, col)
      } else if (method == "region mean") {
        rv$cleaned_data <- handle_na_region_mean(rv$cleaned_data, col)
      } else if (method == "region mode") {
        rv$cleaned_data <- handle_na_region_mode(rv$cleaned_data, col)
      }
    }
    
    
    rv$cleaning_performed(TRUE)
    removeModal()
    showNotification("NA handling methods applied and stored.", type = "message")
    
    # Trigger plot update
    rv$cleaned_data <- rv$cleaned_data # This triggers reactivity
    
    # Update variable selection if needed
    updateSelectInput(session, "visualize_var", selected = input$visualize_var)
  })
  
  #============================================================================
  # Data Visualization
  #============================================================================
  
  #' Plot data maps
  observeEvent(c(input$plot_data, rv$cleaned_data, input$visualize_var), {
    req(rv$raw_data, rv$shp_data, input$visualize_var)
    
    # Check if the selected variable had missing values originally
    raw_data_has_na <- any(is.na(rv$raw_data[[input$visualize_var]]))
    
    # Check if cleaning was performed and no NAs exist now
    cleaned_ready <- rv$cleaning_performed() &&
      !any(is.na(rv$cleaned_data[[input$visualize_var]]))
    
    if (!raw_data_has_na) {
      # No NAs originally → show only one plot
      output$plotLayout <- renderUI({
        column(12, align = "center", girafeOutput("singlePlot", height = "500px"))
      })
      
      output$singlePlot <- renderGirafe({
        plot_map_00(
          variable_name = input$visualize_var,
          shp_data_reactive = rv$shp_data,
          dataframe_reactive = rv$raw_data,
          title = "Original Data (No Cleaning Needed)",
          na_handling_method = NULL
        )
      })
      
    } else if (cleaned_ready) {
      # Had NAs before, now cleaned → show side-by-side
      output$plotLayout <- renderUI({
        fluidRow(
          column(6, girafeOutput("rawDataPlot", height = "500px")),
          column(6, girafeOutput("cleanedDataPlot", height = "500px"))
        )
      })
      
      output$rawDataPlot <- renderGirafe({
        plot_map_00(
          variable_name = input$visualize_var,
          shp_data_reactive = rv$shp_data,
          dataframe_reactive = rv$raw_data,
          title = "Raw Data (Before Cleaning)",
          na_handling_method = NULL
        )
      })
      
      output$cleanedDataPlot <- renderGirafe({
        plot_map_00(
          variable_name = input$visualize_var,
          shp_data_reactive = rv$shp_data,
          dataframe_reactive = rv$cleaned_data,
          title = "Cleaned Data (After NA Handling)",
          na_handling_method = rv$na_handling_methods[[input$visualize_var]] %||% "None"
        )
      })
      
    } else {
      # Had NAs, still not fully cleaned → show only raw plot
      output$plotLayout <- renderUI({
        column(12, align = "center", girafeOutput("singlePlot", height = "500px"))
      })
      
      output$singlePlot <- renderGirafe({
        plot_map_00(
          variable_name = input$visualize_var,
          shp_data_reactive = rv$shp_data,
          dataframe_reactive = rv$raw_data,
          title = "Original Data (Still Has Missing Values)",
          na_handling_method = NULL
        )
      })
    }
  #})
  
    
    # Update the explanation text
    output$visualizationExplanation <- renderText({
      if (!rv$cleaning_performed()) {
        "The map below shows the geographic distribution of the selected variable across various wards. If there are missing values in the plot, please click the (Fill in Missing Values) button to handle missing values. If no values are missing, proceed to the next tab."
      } else {
        paste("These maps showcase the effect of data cleaning on the selected variable's distribution. The left map represents the original, raw data. In contrast, the right map reveals the data after addressing missing values using the '",
              rv$na_handling_methods[[input$visualize_var]] %||% "None",
              "' method. The color gradients reflect the variable's magnitude, providing a clear visual comparison between the raw and cleaned datasets."
        )
      }
    })
  })
  
  #============================================================================
  # Normalization
  #============================================================================
  
  #' Show variable relationships modal
  observeEvent(input$specify_relationships, {
    req(rv$raw_data)
    
    data_to_use <- if(rv$cleaning_performed()) rv$cleaned_data else rv$raw_data
    
    showModal(modalDialog(
      title = "Specify Variable Relationships with Malaria Risk",
      tags$div(
        tags$h4("Understanding Variable Relationships"),
        tags$p("Specifying variable relationships helps determine how each factor contributes to malaria risk:"),
        tags$ul(
          tags$li(strong("Direct relationship:"), "As the variable increases, malaria risk increases."),
          tags$li(strong("Inverse relationship:"), "As the variable increases, malaria risk decreases.")
        ),
        tags$p("This information is crucial for accurately calculating the composite risk score."),
        
        uiOutput("variable_relationships")
      ),
      
      footer = tagList(
        modalButton("Cancel"),
        actionButton("apply_relationships", "Apply Relationships")
      )
    ))
  })
  
  #' Generate UI for variable relationships
  output$variable_relationships <- renderUI({
    req(rv$cleaned_data)
    columns_after_wardname <- get_columns_after_wardname(rv$cleaned_data)
    
    lapply(columns_after_wardname, function(var) {
      fluidRow(
        column(6, var),
        column(6, 
               radioButtons(paste0("relationship_", var), NULL,
                            choices = c("Direct" = "direct", "Inverse" = "inverse"),
                            selected = rv$variable_relationships[[var]] %||% "direct",
                            inline = TRUE)
        )
      )
    })
  })
  
  #' Generate UI for normalized variable selection
  output$normalized_variable_select <- renderUI({
    req(rv$normalized_data)
    norm_vars <- grep("^normalization_", names(rv$normalized_data()), value = TRUE)
    selectInput("visualize_normalized_var", "Select Variable to Visualize", 
                choices = norm_vars,
                selected = norm_vars[1])
  })
  
  #' Apply variable relationships and normalize data
  observeEvent(input$apply_relationships, {
    req(rv$raw_data)
    
    data_to_use <- if(rv$cleaning_performed()) rv$cleaned_data else rv$raw_data
    
    columns_after_wardname <- get_columns_after_wardname(data_to_use)
    
    # Store variable relationships
    for (var in columns_after_wardname) {
      relationship <- input[[paste0("relationship_", var)]]
      rv$variable_relationships[[var]] <- relationship
    }
    
    # Normalize the data
    rv$normalized_data <- reactive({
      normalize_data(data_to_use[, c("WardName", columns_after_wardname)], rv$variable_relationships) 
    })
    
    removeModal()
    showNotification("Variable relationships applied and data normalized successfully.", type = "message")
    
    # Update the UI
    output$normalized_variable_select <- renderUI({
      norm_vars <- grep("^normalization_", names(rv$normalized_data()), value = TRUE)
      selectInput("visualize_normalized_var", "Select Normalized Variables to Visualize", 
                  choices = norm_vars,
                  selected = norm_vars[1])
    })
    
    # Update the normalization plot
    output$normalizationplot <- renderGirafe({
      req(rv$normalized_data(), input$visualize_normalized_var)
      plot_normalized_map(shp_data = rv$shp_data, 
                          processed_csv = rv$normalized_data(), 
                          selected_vars = input$visualize_normalized_var)
    })
  })
  
  #' Plot normalized map
  observeEvent(input$plot_normalized, {
    req(rv$normalized_data, input$visualize_normalized_var, rv$shp_data)
    
    output$normalizationplot <- renderGirafe({
      plot_normalized_map(shp_data = rv$shp_data, 
                          processed_csv = rv$normalized_data(), 
                          selected_vars = input$visualize_normalized_var)
    })
  })
  
  #============================================================================
  # Composite Score Calculation
  #============================================================================
  
  #' Generate UI for composite variable selection
  output$composite_variable_select <- renderUI({
    req(rv$cleaned_data)
    columns_after_wardname <- get_columns_after_wardname(rv$cleaned_data)
    selectInput("composite_vars", "Select Variables for Composite Score",
                choices = columns_after_wardname,
                selected = NULL, multiple = TRUE)
  })
  
  

  
  #' Calculate composite scores
  observeEvent(input$plot_button, {
    req(rv$cleaned_data, input$composite_vars)
    
    if (length(input$composite_vars) < 2) {
      showNotification("Please select at least two variables for composite score calculation.", type = "warning")
      return()
    }
    
    withProgress(message = 'Calculating composite scores', value = 0, {
      tryCatch({
        # Step 1: Get variable impacts
        incProgress(0.1, detail = "Getting variable impacts...")
        variable_impacts <- sapply(input$composite_vars, function(var) rv$variable_relationships[[var]])
        
        # Step 2: Normalize data
        incProgress(0.2, detail = "Normalizing data...")
        normalized_data <- normalize_data(rv$cleaned_data[, c("WardName", input$composite_vars)], variable_impacts)
        
        if (is.null(normalized_data)) {
          showNotification("Error in data normalization. Check the console for details.", type = "error")
          return()
        }
        
        # Step 3: Calculate composite scores
        incProgress(0.3, detail = "Calculating composite scores...")
        composite_scores <- composite_score_models(normalized_data, selected_vars = input$composite_vars, shp_data = rv$shp_data)
        
        if (is.null(composite_scores)) {
          showNotification("Error in composite score calculation. Check the console for details.", type = "error")
          return()
        }
        
        # Step 4: Process composite scores for plotting
        incProgress(0.4, detail = "Processing scores...")
        processed_scores <- process_model_score(composite_scores$final_data)
        
        rv$flagged_models(processed_scores %>% 
                            filter(flag_not_ideal) %>% 
                            group_by(variable) %>% 
                            summarise(flagged_wards = paste(WardName, collapse = ", ")))
        
        # Step 5: Join with shapefile data
        incProgress(0.5, detail = "Joining with shapefile data...")
        combined_data <- left_join(processed_scores, rv$shp_data, by = "WardName")
        rv$data <- combined_data
        
        # Step 6: Generate model formulas table
        incProgress(0.6, detail = "Generating model formulas...")
        model_formulae_table <- models_formulas(composite_scores$model_formula)
        rv$output_data <- model_formulae_table
        
        # Step 7: Update plots and tables
        incProgress(0.7, detail = "Updating plots and tables...")
        
        # Update mapPlot
        # Cache plot generation
        plots <- reactive({
          req(rv$data, rv$output_data)
          
          plot_model_score_map(
            shp_data = rv$shp_data,
            processed_csv = rv$data,
            model_formulas = rv$output_data,
            maps_per_page = 4
          )
        })
        
        # Create UI for the plots
        output$mapPlot <- renderUI({
          req(plots())
          
          do.call(tabsetPanel, c(
            id = "active_tab",   
            lapply(seq_along(plots()), function(i) {
              tabPanel(
                paste("Page", i),
                girafeOutput(paste0("mapPlot_", i))
              )
            })
          ))
        })
        
        # Dynamically render the plots
        observe({
          req(plots(), input$active_tab)
          
          current_tab_number <- as.numeric(gsub("Page ", "", input$active_tab))
          
          if (!is.na(current_tab_number)) {
            output[[paste0("mapPlot_", current_tab_number)]] <- renderGirafe({
              plots()[[current_tab_number]]
            })
          }
        })
        
        #@laurette edited the previous set up was plotting twice 
        # Update normalizationplot
        output$normalizationplot <- renderGirafe({
          req(rv$normalized_data(), input$visualize_normalized_var)
          plot_normalized_map(shp_data = rv$shp_data, 
                              processed_csv = rv$normalized_data(), 
                              selected_vars = input$visualize_normalized_var)
        })
        
        # Initialize pagination for box plot
        box_plot_results <- box_plot_function(rv$data, wards_per_page = 20)
        rv$ward_rankings <- box_plot_results$ward_rankings
        rv$boxplot_pagination <- box_plot_results$pagination
        rv$current_page <- 1
        
        # Update boxwhiskerPlots
        incProgress(0.8, detail = "Generating box plots...")
        output$boxwhiskerPlots <- renderPlotly({
          req(rv$data)
          
          # If pagination is not initialized yet, initialize it
          if (is.null(rv$boxplot_pagination)) {
            box_plot_results <- box_plot_function(rv$data, wards_per_page = 20)
            rv$ward_rankings <- box_plot_results$ward_rankings
            rv$boxplot_pagination <- box_plot_results$pagination
            rv$current_page <- 1
            return(box_plot_results$plot)
          } else {
            # Use the current page
            create_plot <- rv$boxplot_pagination$create_page_plot
            return(create_plot(
              rv$current_page, 
              rv$data, 
              rv$ward_rankings, 
              rv$boxplot_pagination$wards_per_page
            ))
          }
        })
        
        # Step 8: Show completion notification
        incProgress(1, detail = "Completed!")
        num_models <- nrow(model_formulae_table)
        if (length(input$composite_vars) == 2) {
          showNotification("Generated 1 model combination using the two selected variables.", type = "message")
        } else {
          showNotification(paste("Generated", num_models, "model combinations."), type = "message")
        }
        
      }, error = function(e) {
        showNotification(paste("Error:", e$message), type = "error")
      })
    })
  })
  
  #' #' Display table of flagged models
  #' output$flagged_models_table <- renderTable({
  #'   req(rv$flagged_models)
  #'   flagged <- rv$flagged_models()
  #'   
  #'   if (is.null(flagged) || nrow(flagged) == 0) {
  #'     return(data.frame("Message" = "No problematic models identified."))
  #'   }
  #'   
  #'   # Format for display
  #'   flagged %>%
  #'     mutate(
  #'       Model = gsub("model_", "Model ", variable),
  #'       `Flagged Wards` = flagged_wards
  #'     ) %>%
  #'     select(Model, `Flagged Wards`) %>%
  #'     mutate(
  #'       `Potential Issue` = "Non-urban wards in top 5 (likely not suitable for resource allocation)"
  #'     )
  #' }, striped = TRUE, bordered = TRUE, hover = TRUE, align = 'l')
  
  #============================================================================
  # Box and Whisker Plot / Vulnerability Map
  #============================================================================
  
  #' Generate vulnerability map
  output$vulnerabilityMap <- renderGirafe({
    req(rv$data, rv$ward_rankings, rv$shp_data)
    
    map_data <- left_join(rv$shp_data, rv$ward_rankings, by = "WardName")
    
    plot <- ggplot() +
      geom_sf_interactive(data = map_data, aes(fill = overall_rank, 
                                               tooltip = paste(WardName, "\nRank:", overall_rank))) +
      scale_fill_viridis_c(option = "plasma", direction = -1, 
                           name = "Vulnerability Rank",
                           guide = guide_colorbar(
                             title.position = "top",
                             title.hjust = 0.5,
                             label.theme = element_text(size = 12),
                             barwidth = 15,
                             barheight = 1
                           )) +
      theme_void() +
      labs(title = "Ward Vulnerability Map") +
      theme(legend.position = "bottom",
            legend.box = "vertical",
            legend.margin = margin(t = 10, b = 10),
            plot.title = element_text(size = 16, face = "bold", hjust = 0.5)) +
      annotate("text", x = -Inf, y = -Inf, label = "Low Vulnerability (Rank 1)",
               hjust = 0, vjust = -1, size = 4, color = "darkred", fontface = "bold") +
      annotate("text", x = Inf, y = -Inf, label = "High Vulnerability (Highest Rank)",
               hjust = 1, vjust = -1, size = 4, color = "darkblue", fontface = "bold")
    
    girafe(ggobj = plot, width_svg = 10, height_svg = 8)
  })
  
  #' Generate filtered vulnerability map
  output$filteredVulnerabilityMap <- renderGirafe({
    req(rv$data, rv$ward_rankings, rv$shp_data, input$urban_threshold)
    
    # Get threshold from numeric input - use this value consistently
    threshold <- as.numeric(input$urban_threshold)
    
    # Apply urban extent threshold filtering
    filtered_data <- filter_by_urban_extent(rv$shp_data, threshold)
    
    # Join with vulnerability rankings
    map_data <- left_join(filtered_data, rv$ward_rankings, by = "WardName")
    
    # Create plot with non-urban areas grayed out
    plot <- ggplot() +
      # First layer: All wards with gray for those below threshold
      geom_sf_interactive(data = map_data, 
                          aes(fill = ifelse(MeetsThreshold, overall_rank, NA),
                              tooltip = paste(WardName, 
                                              "\nUrban %:", round(UrbanPercent, 1),
                                              "\nRank:", overall_rank,
                                              "\nStatus:", ifelse(MeetsThreshold, "Prioritized", "De-prioritized"))),
                          color = "black") +
      # Layer for non-prioritized areas (below threshold)
      geom_sf_interactive(data = map_data[!map_data$MeetsThreshold, ], 
                          fill = "#CCCCCC", alpha = 0.7,
                          color = "black",
                          aes(tooltip = paste(WardName, 
                                              "\nUrban %:", round(UrbanPercent, 1),
                                              "\n(Below urban threshold)"))) +
      scale_fill_viridis_c(option = "plasma", direction = -1, 
                           name = "Vulnerability Rank",
                           na.value = "#CCCCCC",
                           guide = guide_colorbar(
                             title.position = "top",
                             title.hjust = 0.5,
                             label.theme = element_text(size = 12),
                             barwidth = 15,
                             barheight = 1
                           )) +
      theme_void() +
      labs(title = paste("Ward Vulnerability Map (Urban Threshold:", threshold, "%)")) +
      theme(legend.position = "bottom",
            legend.box = "vertical",
            legend.margin = margin(t = 10, b = 10),
            plot.title = element_text(size = 16, face = "bold", hjust = 0.5)) +
      # Annotations for clarity
      annotate("text", x = -Inf, y = -Inf, label = "Low Vulnerability (Rank 1)", 
               hjust = 0, vjust = -1, size = 4, color = "darkred", fontface = "bold") +
      annotate("text", x = Inf, y = -Inf, label = "High Vulnerability (Highest Rank)", 
               hjust = 1, vjust = -1, size = 4, color = "darkblue", fontface = "bold")
    
    girafe(ggobj = plot, width_svg = 10, height_svg = 8)
  })
  
  #' Create a table with deprioritization statistics
  output$deprioritization_stats <- renderTable({
    req(rv$ward_rankings, rv$shp_data, input$urban_threshold)
    
    # Get threshold from numeric input
    threshold <- as.numeric(input$urban_threshold)
    
    # Apply urban extent threshold filtering
    filtered_data <- filter_by_urban_extent(rv$shp_data, threshold)
    
    # Join with vulnerability rankings
    map_data <- left_join(filtered_data, rv$ward_rankings, by = "WardName")
    
    # Calculate statistics
    total_wards <- nrow(map_data)
    prioritized_wards <- sum(map_data$MeetsThreshold)
    deprioritized_wards <- total_wards - prioritized_wards
    
    # Create a statistics table
    data.frame(
      Metric = c("Total Wards", "Deprioritized Wards", "Prioritized Wards", "Prioritization %"),
      Value = c(
        total_wards,
        prioritized_wards,
        deprioritized_wards,
        paste0(round(deprioritized_wards / total_wards * 100, 1), "%")
      )
    )
  }, striped = TRUE, hover = TRUE, bordered = TRUE, align = 'l')
  
  #' Observer to update deprioritized wards list
  observe({
    req(rv$shp_data, rv$ward_rankings, input$urban_threshold)
    
    # Get the threshold value from the numeric input
    threshold <- as.numeric(input$urban_threshold)
    
    if(threshold > 0) {
      # Filter data based on threshold
      filtered_data <- filter_by_urban_extent(rv$shp_data, threshold)
      
      # Combine with vulnerability rankings
      combined_data <- left_join(filtered_data, rv$ward_rankings, by = "WardName")
      
      # Get list of de-prioritized wards (those below threshold)
      deprioritized <- combined_data %>%
        filter(!MeetsThreshold) %>%
        select(WardName, UrbanPercent, overall_rank) %>%
        arrange(desc(overall_rank))
      
      rv$deprioritized_wards(deprioritized)
    } else {
      rv$deprioritized_wards(NULL)
    }
  })
  
  output$threshold_explanation_text <- renderUI({
    req(input$urban_threshold)
    threshold <- as.numeric(input$urban_threshold)
    p(paste0("This map shows wards filtered by an urban extent threshold of ", threshold, "%."))
  })
  
  # Add pagination event handlers
  observeEvent(input$prev_page, {
    if (rv$current_page > 1) {
      rv$current_page <- rv$current_page - 1
    }
  })
  
  observeEvent(input$next_page, {
    req(rv$boxplot_pagination)
    if (rv$current_page < rv$boxplot_pagination$total_pages) {
      rv$current_page <- rv$current_page + 1
    }
  })
  
  observeEvent(input$wards_per_page, {
    req(rv$data)
    
    # Update wards per page and recalculate
    box_plot_results <- box_plot_function(rv$data, as.numeric(input$wards_per_page))
    
    # Store the results
    rv$ward_rankings <- box_plot_results$ward_rankings
    rv$boxplot_pagination <- box_plot_results$pagination
    
    # Reset to page 1
    rv$current_page <- 1
  })
  
  # Render the pagination UI
  output$boxPlotPagination <- renderUI({
    req(rv$boxplot_pagination)
    
    pagination <- rv$boxplot_pagination
    
    if (pagination$total_pages <= 1) {
      return(NULL) # No pagination needed
    }
    
    div(
      style = "display: flex; justify-content: center; margin-top: 15px;",
      div(
        style = "display: inline-flex; align-items: center; background-color: #f8f9fa; padding: 5px 10px; border-radius: 5px;",
        actionButton("prev_page", "←", 
                     style = "margin-right: 10px;",
                     class = if (rv$current_page <= 1) "btn-secondary disabled" else "btn-primary"),
        
        span(paste0("Page ", rv$current_page, " of ", pagination$total_pages),
             style = "margin: 0 15px; font-weight: bold;"),
        
        actionButton("next_page", "→", 
                     style = "margin-left: 10px;",
                     class = if (rv$current_page >= pagination$total_pages) "btn-secondary disabled" else "btn-primary"),
        
        div(
          style = "margin-left: 20px; display: flex; align-items: center;",
          span("Wards per page:", style = "margin-right: 10px;"),
          selectInput("wards_per_page", NULL, 
                      choices = c(10, 20, 30, 50, 100),
                      selected = pagination$wards_per_page,
                      width = "80px")
        )
      )
    )
  })
  
  # Add these observers to handle pagination controls:
  observeEvent(input$prev_page, {
    if (rv$current_page > 1) {
      rv$current_page <- rv$current_page - 1
      updateBoxPlot()
    }
  })
  
  observeEvent(input$next_page, {
    req(rv$boxplot_pagination)
    if (rv$current_page < rv$boxplot_pagination$total_pages) {
      rv$current_page <- rv$current_page + 1
      updateBoxPlot()
    }
  })
  
  observeEvent(input$wards_per_page, {
    req(rv$data)
    
    # Update wards per page and recalculate
    box_plot_results <- box_plot_function(rv$data, as.numeric(input$wards_per_page))
    
    # Store the results
    rv$ward_rankings <- box_plot_results$ward_rankings
    rv$boxplot_pagination <- box_plot_results$pagination
    
    # Reset to page 1
    rv$current_page <- 1
    
    # Update the plot
    updateBoxPlot()
  })
  
  # Function to update the box plot based on pagination
  updateBoxPlot <- function() {
    req(rv$data, rv$boxplot_pagination, rv$current_page)
    
    # Generate the plot for the current page
    output$boxwhiskerPlots <- renderPlotly({
      create_plot <- rv$boxplot_pagination$create_page_plot
      create_plot(
        rv$current_page, 
        rv$data, 
        rv$ward_rankings, 
        rv$boxplot_pagination$wards_per_page
      )
    })
  }
  
  
  #============================================================================
  # Decision Tree
  #============================================================================
  
  #' Update decision tree progress
  observe({
    rv$decision_tree_progress(modifyList(rv$decision_tree_progress(), list(
      data_loaded = TRUE,
      variables_selected = TRUE,
      normalization_done = TRUE,
      composite_scores_calculated = TRUE
    )))
  })
  
  #' Initialize the decision tree
  output$decisionTree <- renderGrViz({
    req(rv$cleaned_data)
    all_vars <- get_columns_after_wardname(rv$cleaned_data)
    selected_vars <- input$composite_vars
    excluded_vars <- setdiff(all_vars, selected_vars)
    
    top_5_wards <- if (!is.null(rv$ward_rankings)) {
      rv$ward_rankings %>%
        arrange(desc(overall_rank)) %>%
        slice_head(n = 5) %>%
        pull(WardName)
    } else {
      character(0)
    }
    
    decision_tree_function(all_vars, selected_vars, excluded_vars, rv$decision_tree_progress(), top_5_wards)
  })
  
  #' Update decision tree when parameters change
  observe({
    req(rv$decision_tree_progress())
    output$decisionTree <- renderGrViz({
      req(rv$cleaned_data)
      all_vars <- get_columns_after_wardname(rv$cleaned_data)
      selected_vars <- input$composite_vars
      excluded_vars <- setdiff(all_vars, selected_vars)
      
      top_5_wards <- if (!is.null(rv$ward_rankings)) {
        rv$ward_rankings %>%
          arrange(overall_rank) %>%
          slice_head(n = 5) %>%
          pull(WardName)
      } else {
        character(0)
      }
      
      decision_tree_function(all_vars, selected_vars, excluded_vars, rv$decision_tree_progress(), top_5_wards)
    })
  })
  
  #' Download decision tree as PNG
  output$downloadPNG <- downloadHandler(
    filename = function() {
      paste0("decision_tree_", format(Sys.time(), "%Y%m%d_%H%M%S"), ".png")
    },
    content = function(file) {
      tryCatch({
        # Generate the diagram
        graph <- decision_tree_function(
          all_variables = get_columns_after_wardname(rv$cleaned_data),
          selected_variables = input$composite_vars,
          excluded_variables = setdiff(get_columns_after_wardname(rv$cleaned_data), 
                                       input$composite_vars),
          progress = rv$decision_tree_progress(),
          top_5_wards = if (!is.null(rv$ward_rankings)) {
            rv$ward_rankings %>%
              arrange(overall_rank) %>%
              slice_head(n = 5) %>%
              pull(WardName)
          } else {
            character(0)
          }
        )
        
        # Convert to SVG first
        svg_content <- DiagrammeRsvg::export_svg(graph)
        
        # Write to a temporary SVG file
        temp_svg <- tempfile(fileext = ".svg")
        writeLines(svg_content, temp_svg)
        
        # Convert SVG to PNG using rsvg
        rsvg::rsvg_png(temp_svg, file, width = 1200)
        
      }, error = function(e) {
        # Log the error
        message("Error in PNG export: ", e$message)
        stop("Failed to create PNG file. Error: ", e$message)
      }, finally = {
        # Cleanup
        if (exists("temp_svg") && file.exists(temp_svg)) {
          unlink(temp_svg)
        }
      })
    }
  )
  
  #' Download decision tree as PDF
  output$downloadPDF <- downloadHandler(
    filename = function() {
      paste0("decision_tree_", format(Sys.time(), "%Y%m%d_%H%M%S"), ".pdf")
    },
    content = function(file) {
      tryCatch({
        # Generate the diagram
        graph <- decision_tree_function(
          all_variables = get_columns_after_wardname(rv$cleaned_data),
          selected_variables = input$composite_vars,
          excluded_variables = setdiff(get_columns_after_wardname(rv$cleaned_data), 
                                       input$composite_vars),
          progress = rv$decision_tree_progress(),
          top_5_wards = if (!is.null(rv$ward_rankings)) {
            rv$ward_rankings %>%
              arrange(overall_rank) %>%
              slice_head(n = 5) %>%
              pull(WardName)
          } else {
            character(0)
          }
        )
        
        # Convert to SVG first
        svg_content <- DiagrammeRsvg::export_svg(graph)
        
        # Write to a temporary SVG file
        temp_svg <- tempfile(fileext = ".svg")
        writeLines(svg_content, temp_svg)
        
        # Convert SVG to PDF using rsvg
        rsvg::rsvg_pdf(temp_svg, file)
        
      }, error = function(e) {
        # Log the error
        message("Error in PDF export: ", e$message)
        stop("Failed to create PDF file. Error: ", e$message)
      }, finally = {
        # Cleanup
        if (exists("temp_svg") && file.exists(temp_svg)) {
          unlink(temp_svg)
        }
      })
    }
  )
  
  output$boxwhiskerPlots <- renderUI({
    req(rv$data)
    
    result <- box_plot_function_improved(rv$data, max_display = 30, use_pagination = TRUE)
    rv$ward_rankings <- result$ward_rankings
    
    if ("plot" %in% names(result)) {
      # Single plot (smaller dataset)
      plotlyOutput("singleBoxPlot", height = "500px")
    } else {
      # Paginated plots (larger dataset)
      rv$box_plot_pages <- result$num_pages
      rv$box_plot_render <- result$plot_function
      
      tagList(
        div(style = "display: flex; justify-content: center; margin-bottom: 10px;",
            actionButton("prev_page", "← Previous", 
                         style = "margin-right: 10px;"),
            div(style = "padding: 6px 12px; background: #f8f9fa; border-radius: 4px;",
                textOutput("page_indicator")),
            actionButton("next_page", "Next →", 
                         style = "margin-left: 10px;")
        ),
        plotlyOutput("paginatedBoxPlot", height = "500px")
      )
    }
  })
  
  # Add these render functions:
  
  output$singleBoxPlot <- renderPlotly({
    req(rv$data)
    result <- box_plot_function_improved(rv$data, max_display = 30, use_pagination = FALSE)
    result$plot
  })
  
  # Initialize current page
  observe({
    if (is.null(rv$current_page)) {
      rv$current_page <- 1
    }
  })
  
  # Render current page
  output$paginatedBoxPlot <- renderPlotly({
    req(rv$box_plot_render, rv$current_page)
    rv$box_plot_render(rv$current_page)
  })
  
  # Update page indicator
  output$page_indicator <- renderText({
    req(rv$current_page, rv$box_plot_pages)
    paste0("Page ", rv$current_page, " of ", rv$box_plot_pages)
  })
  
  # Handle pagination buttons
  observeEvent(input$prev_page, {
    if (rv$current_page > 1) {
      rv$current_page <- rv$current_page - 1
    }
  })
  
  observeEvent(input$next_page, {
    if (rv$current_page < rv$box_plot_pages) {
      rv$current_page <- rv$current_page + 1
    }
  })
  
  
  # Render the box whisker content
  output$boxWhiskerContent <- renderUI({
    div(
      conditionalPanel(
        condition = "input.show_map == false",
        div(
          plotlyOutput("boxwhiskerPlots", height = "500px"),
          uiOutput("boxPlotPagination")
        )
      ),
      conditionalPanel(
        condition = "input.show_map == true && input.show_threshold_map == false",
        girafeOutput("vulnerabilityMap", height = "500px")
      ),
      conditionalPanel(
        condition = "input.show_map == true && input.show_threshold_map == true",
        girafeOutput("filteredVulnerabilityMap", height = "500px")
      )
    )
  })
  
  #============================================================================
  # Manual Labeling
  #============================================================================
  
  #' Display manual deprioritization stats
  output$manual_deprioritization_stats <- renderUI({
    req(rv$shp_data, input$urban_threshold)
    
    # Use the threshold from the input - DYNAMIC THRESHOLD
    threshold <- as.numeric(input$urban_threshold)
    
    # Force recalculation each time
    filtered_data <- filter_by_urban_extent(rv$shp_data, threshold)
    
    # Calculate statistics
    total_wards <- nrow(filtered_data)
    prioritized_wards <- sum(filtered_data$MeetsThreshold)
    deprioritized_wards <- total_wards - prioritized_wards
    deprioritized_percent <- round(deprioritized_wards / total_wards * 100, 1)
    
    # Create info box with dynamic threshold
    tagList(
      div(style = "margin-bottom: 10px;",
          p(style = "margin: 0;",
            HTML(paste0(
              "<strong>", prioritized_wards, "</strong> wards (", round(100 - deprioritized_percent, 1), "%) meet the threshold.<br>",
              "<strong>", deprioritized_wards, "</strong> wards (", deprioritized_percent, "%) are below the threshold."
            ))
          )
      ),
      p(style = "font-style: italic; color: #666; font-size: 12px; margin-bottom: 0;",
        "Select a ward type below to begin detailed analysis.")
    )
  })
  
  
  
  # dynamic urban percent selection
  output$dynamic_threshold_text <- renderUI({
    req(input$urban_threshold)
    threshold <- as.numeric(input$urban_threshold)
    
    p(paste0("Using ", threshold, "% urban extent threshold for bed net distribution planning:"))
  })
  
  output$dynamic_status_text <- renderUI({
    req(input$urban_threshold)
    threshold <- as.numeric(input$urban_threshold)
    
    p(id = "status_text", paste0("This tool allows you to examine re-prioritized wards (that meets the ", 
                                 threshold, "% urban threshold) in detail. Manual classification helps identify specific areas that may need bed nets despite the overall ward status."))
  })
  
  
  #' Create ward selection dropdown
  output$filtered_ward_select <- renderUI({
    # Require both shapefile data and ward rankings to be available
    req(rv$shp_data, rv$ward_rankings, input$urban_threshold)
    
    # Get threshold from input value
    threshold <- as.numeric(input$urban_threshold)
    
    # Get urban percent information directly from shapefile if available
    if (!"UrbanPercent" %in% names(rv$shp_data)) {
      if ("Urban" %in% names(rv$shp_data)) {
        # Convert from Yes/No to percentages if that's what's available
        rv$shp_data$UrbanPercent <- ifelse(rv$shp_data$Urban == "Yes", 100, 0)
      } else {
        # Show error message if no urban data available
        return(div(
          style = "padding: 10px; background-color: #f8d7da; border-radius: 5px; margin-bottom: 10px;",
          p(style = "margin: 0; color: #721c24;",
            "Error: No urban extent data available. Please ensure your shapefile contains urban information.")
        ))
      }
    }
    
    # Create a data frame with just WardName and UrbanPercent
    urban_data <- rv$shp_data %>%
      st_drop_geometry() %>%
      select(WardName, UrbanPercent)
    
    # Join with ward rankings
    combined_data <- rv$ward_rankings %>%
      inner_join(urban_data, by = "WardName")
    
    # Filter to include ONLY wards above the threshold
    filtered_wards <- combined_data %>%
      filter(UrbanPercent >= threshold) %>%
      arrange(overall_rank)
    
    # Check if any wards meet the threshold
    if(nrow(filtered_wards) == 0) {
      return(div(
        style = "padding: 10px; background-color: #f8d7da; border-radius: 5px; margin-bottom: 10px;",
        p(style = "margin: 0; color: #721c24;",
          paste0("No wards found above the ", threshold, "% urban threshold. Please check your data."))
      ))
    }
    
    # Create named vector for selectInput choices
    ward_choices <- setNames(
      filtered_wards$WardName,
      paste0(filtered_wards$WardName, " (Rank: ", 
             filtered_wards$overall_rank, ", Urban: ", 
             round(filtered_wards$UrbanPercent, 1), "%)")
    )
    
    # Create the select input with dynamic threshold in label
    selectInput("selected_ward", paste0("Select Ward (Meets ", threshold, "% Threshold):"), 
                choices = c("Select a ward" = "", ward_choices),
                selected = "")
  })
  
  
  
  # And add this in the server function
  output$dynamic_grid_help_content <- renderUI({
    req(input$urban_threshold) 
    threshold <- as.numeric(input$urban_threshold)
    
    tags$ol(
      tags$li(paste0("Select a ward from the dropdown (only wards above ", threshold, "% urban threshold are shown)")),
      tags$li("Click 'GENERATE GRID & PLOT MAP'"),
      tags$li("Click on any grid cell to open classification popup"),
      tags$li("Choose a classification (Formal, Informal, or No Buildings/Avoid)"),
      tags$li("Classified cells will appear colored on the map"),
      tags$li("Click 'DOWNLOAD CLASSIFIED MAP' to export your work")
    )
  })
  
  
  #' Show urban info for selected ward
  output$selected_ward_urban_info <- renderUI({
    req(input$selected_ward, rv$shp_data, input$urban_threshold)
    
    # Get urban info for the selected ward
    ward_data <- rv$shp_data %>% filter(WardName == input$selected_ward)
    
    if (nrow(ward_data) == 0) {
      return(p("No data available for this ward."))
    }
    
    # Get current threshold value
    threshold <- as.numeric(input$urban_threshold)
    
    # Apply urban extent threshold filtering with DYNAMIC threshold
    filtered_data <- filter_by_urban_extent(ward_data, threshold)
    
    # Get urban percentage and threshold status
    urban_percent <- filtered_data$UrbanPercent[1]
    meets_threshold <- filtered_data$MeetsThreshold[1]
    
    # Get vulnerability rank if available
    rank_text <- ""
    if (!is.null(rv$ward_rankings)) {
      ward_rank <- rv$ward_rankings %>%
        filter(WardName == input$selected_ward)
      
      if (nrow(ward_rank) > 0) {
        rank_text <- paste0("<br>Vulnerability Rank: <strong>", ward_rank$overall_rank[1], "</strong>")
      }
    }
    
    # Format display - UPDATED TERMINOLOGY
    tagList(
      HTML(paste0(
        "<div style='margin-bottom: 5px;'>",
        "<span>Ward: </span>",
        "<span style='font-weight: bold;'>", input$selected_ward, "</span>",
        "</div>",
        
        "<div style='margin-bottom: 5px;'>",
        "<span>Urban Extent: </span>",
        "<span style='font-weight: bold; color: ", 
        ifelse(meets_threshold, "#dc3545", "#28a745"), ";'>",  # Colors reversed
        round(urban_percent, 1), "%</span>",
        "</div>",
        
        "<div>",
        "<span>Status: </span>",
        "<span style='font-weight: bold; color: ", 
        ifelse(meets_threshold, "#dc3545", "#28a745"), ";'>",  # Colors reversed
        ifelse(meets_threshold, "Re-prioritized", "Prioritized"), "</span>",  # Terminology updated
        rank_text,
        "</div>"
      ))
    )
  })
  
  #' Handle grid help button clicks
  observeEvent(input$grid_help, {
    shinyjs::toggle("grid_help_content")
  })
  
  #' Handle close help button clicks
  observeEvent(input$close_help, {
    shinyjs::hide("grid_help_content")
  })
  
  #' Create ward grid and map
  observeEvent(input$plot_ward_map, {
    req(input$selected_ward, rv$shp_data)
    
    withProgress(message = 'Processing ward data...', value = 0, {
      incProgress(0.2, detail = "Loading ward data...")
      
      # Create grid for the selected ward
      if (input$enable_grid) {
        # Create the grid (using 500m as default if not specified)
        grid_data <- create_ward_grid(
          input$selected_ward, 
          rv$shp_data, 
          as.numeric(input$grid_cell_size %||% 500)
        )
        
        # Store the grid in the reactive value
        if (!is.null(grid_data)) {
          # If we already have grids stored, update or append
          existing_grids <- rv$gridded_wards()
          if (!is.null(existing_grids)) {
            # Remove any existing grid for this ward
            existing_grids <- existing_grids[existing_grids$WardName != input$selected_ward, ]
            # Combine with the new grid
            grid_data <- rbind(existing_grids, grid_data)
          }
          rv$gridded_wards(grid_data)
          
          incProgress(0.6, detail = "Grid created successfully!")
        } else {
          showNotification("Could not create grid for this ward. Check console for errors.", type = "error")
        }
      }
      
      # Determine if this is a prioritized or re-prioritized ward
      ward_data <- rv$shp_data %>%
        filter(WardName == input$selected_ward)
      
      threshold <- 30
      filtered_ward <- filter_by_urban_extent(ward_data, threshold)
      is_prioritized <- filtered_ward$MeetsThreshold[1]
      
      # Render the map with our enhanced function
      output$ward_map <- renderLeaflet({
        process_and_view_shapefile_and_csv_enhanced(
          input$selected_ward, 
          rv$shp_data,
          rv$grid_annotations(),
          enable_grid = input$enable_grid,
          grid_cell_size = as.numeric(input$grid_cell_size %||% 500)
        )
      })
      
      incProgress(1, detail = "Map created!")
    })
  })
  
  #' Observer for classification from popup
  # Observer for classification from popup
  observeEvent(input$classify_grid, {
    req(input$classify_grid, rv$gridded_wards())
    
    # Extract the classification data
    ward_name <- input$classify_grid$wardName
    grid_id <- as.integer(input$classify_grid$gridId)
    classification <- input$classify_grid$classification
    timestamp <- input$classify_grid$timestamp
    method <- input$classify_grid$method %||% "manual"
    
    # Create a data frame with the information
    new_annotation <- data.frame(
      WardName = ward_name,
      GridID = grid_id,
      Classification = classification,
      Timestamp = timestamp,
      Method = method,
      stringsAsFactors = FALSE
    )
    
    # Get current annotations
    annotations <- rv$grid_annotations()
    
    # Check if this grid cell is already annotated
    existing_idx <- which(annotations$WardName == ward_name & 
                            annotations$GridID == grid_id)
    
    if (length(existing_idx) > 0) {
      # Update existing annotation
      annotations$Classification[existing_idx] <- classification
      annotations$Timestamp[existing_idx] <- timestamp
      annotations$Method[existing_idx] <- method
    } else {
      # Add new annotation
      annotations <- rbind(annotations, new_annotation)
    }
    
    # Update annotations
    rv$grid_annotations(annotations)
    
    # Refresh the map with the updated classifications
    output$ward_map <- renderLeaflet({
      process_and_view_shapefile_and_csv_enhanced(
        input$selected_ward, 
        rv$shp_data,
        rv$grid_annotations(),
        enable_grid = input$enable_grid,
        grid_cell_size = as.numeric(input$grid_cell_size %||% 500)
      )
    })
    
    # Show notification
    showNotification(paste("Grid", grid_id, "classified as", classification), 
                     type = "message", duration = 2)
  })
  
  #' Display classification summary badges
  output$classification_badges <- renderUI({
    req(input$selected_ward, rv$grid_annotations())
    
    # Get classifications for the selected ward
    annotations <- rv$grid_annotations() %>%
      filter(WardName == input$selected_ward)
    
    # Count by classification type
    formal_count <- sum(annotations$Classification == "Formal")
    informal_count <- sum(annotations$Classification == "Informal")
    avoid_count <- sum(annotations$Classification == "No Buildings/Avoid Area")
    unclassified_count <- 0
    
    # If we have grid data, calculate unclassified count
    grid_data <- rv$gridded_wards()
    if (!is.null(grid_data)) {
      ward_grid <- grid_data %>% 
        filter(WardName == input$selected_ward)
      
      total_cells <- nrow(ward_grid)
      classified_cells <- nrow(annotations)
      unclassified_count <- total_cells - classified_cells
    }
    
    # Create badges with explanatory text
    tagList(
      div(style = "margin-bottom: 10px;", 
          p("Classifications help identify areas that may need nets despite the ward being above the threshold:")),
      div(style = "display: flex; flex-wrap: wrap; gap: 5px;",
          if (formal_count > 0) div(class = "classification-badge badge-formal", paste("Formal:", formal_count)),
          if (informal_count > 0) div(class = "classification-badge badge-informal", paste("Informal:", informal_count)),
          if (avoid_count > 0) div(class = "classification-badge badge-avoid", paste("No Buildings/Avoid:", avoid_count)),
          if (unclassified_count > 0) div(class = "classification-badge badge-unclassified", paste("Unclassified:", unclassified_count))
      ),
      div(style = "margin-top: 10px; font-style: italic; color: #666; font-size: 12px;", 
          "Formal and Informal areas will be considered for net distribution.")
    )
  })
  
  #' Clear all annotations
  observeEvent(input$clear_annotations, {
    req(input$selected_ward)
    
    # Get current annotations
    annotations <- rv$grid_annotations()
    
    # Filter out annotations for the current ward
    annotations <- annotations %>%
      filter(WardName != input$selected_ward)
    
    # Update annotations
    rv$grid_annotations(annotations)
    
    # Refresh the map
    output$ward_map <- renderLeaflet({
      process_and_view_shapefile_and_csv_enhanced(
        input$selected_ward, 
        rv$shp_data,
        rv$grid_annotations(),
        enable_grid = input$enable_grid,
        grid_cell_size = as.numeric(input$grid_cell_size %||% 500)
      )
    })
    
    showNotification("All annotations cleared for the selected ward.", type = "message")
  })
  
  #' Download grid shapefile
  output$download_grid <- downloadHandler(
    filename = function() {
      paste0("gridded_", gsub(" ", "_", input$selected_ward), ".zip")
    },
    content = function(file) {
      # Create a temporary directory
      temp_dir <- tempdir()
      grid_data <- rv$gridded_wards()
      
      if (is.null(grid_data)) {
        showNotification("No grid data available. Please generate a grid first.", type = "warning")
        return()
      }
      
      # Filter to just the selected ward's grid
      ward_grid <- grid_data[grid_data$WardName == input$selected_ward, ]
      
      if (nrow(ward_grid) == 0) {
        showNotification("No grid data available for the selected ward. Please generate a grid first.", type = "warning")
        return()
      }
      
      # Write shapefile to temp directory
      shapefile_dir <- file.path(temp_dir, "shapefile")
      dir.create(shapefile_dir, showWarnings = FALSE, recursive = TRUE)
      
      tryCatch({
        st_write(ward_grid, file.path(shapefile_dir, "grid.shp"), append = FALSE)
        
        # Create zip file
        files_to_zip <- list.files(shapefile_dir, full.names = TRUE)
        zip(file, files_to_zip)
      }, error = function(e) {
        showNotification(paste("Error creating shapefile:", e$message), type = "error")
      })
    }
  )
  
  #' Download grid annotations
  output$download_annotations <- downloadHandler(
    filename = function() {
      paste0("annotations_", gsub(" ", "_", input$selected_ward), "_", format(Sys.time(), "%Y%m%d_%H%M%S"), ".csv")
    },
    content = function(file) {
      # Get current annotations for selected ward
      annotations <- rv$grid_annotations()
      ward_annotations <- annotations[annotations$WardName == input$selected_ward, ]
      
      if (nrow(ward_annotations) == 0) {
        showNotification("No annotations available for this ward.", type = "warning")
        write.csv(data.frame(Message = "No annotations available for this ward"), file, row.names = FALSE)
        return()
      }
      
      # Get grid data for this ward to extract coordinates
      grid_data <- NULL
      if (!is.null(rv$gridded_wards())) {
        grid_data <- rv$gridded_wards() %>% 
          filter(WardName == input$selected_ward)
      }
      
      # Create a new data frame with coordinates
      export_data <- ward_annotations
      
      # Add coordinates if grid data is available
      if (!is.null(grid_data) && nrow(grid_data) > 0) {
        # Calculate centroids for each grid cell
        grid_centroids <- st_centroid(grid_data)
        # Extract coordinates
        coords <- st_coordinates(grid_centroids)
        # Create a lookup table
        coord_lookup <- data.frame(
          GridID = grid_data$GridID,
          Longitude = coords[, 1],
          Latitude = coords[, 2],
          stringsAsFactors = FALSE
        )
        
        # Join with annotations
        export_data <- left_join(export_data, coord_lookup, by = "GridID")
      }
      
      # Add metadata columns
      export_data$ExportDate <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
      export_data$GridCellSize <- input$grid_cell_size
      export_data$GridCellDescription <- paste0(input$grid_cell_size, "m × ", input$grid_cell_size, "m square (for operational purposes)")
      
      # Get urban info
      if (!is.null(rv$shp_data)) {
        ward_data <- rv$shp_data %>%
          filter(WardName == input$selected_ward)
        
        if (nrow(ward_data) > 0) {
          # Get urban percentage
          urban_percent <- if ("UrbanPercent" %in% names(ward_data)) {
            ward_data$UrbanPercent[1]
          } else if ("Urban" %in% names(ward_data)) {
            ifelse(ward_data$Urban[1] == "Yes", 100, 0)
          } else {
            NA
          }
          
          export_data$UrbanPercent <- urban_percent
        }
      }
      
      # Add grid statistics
      if (!is.null(rv$gridded_wards())) {
        ward_grid <- rv$gridded_wards() %>% 
          filter(WardName == input$selected_ward)
        
        if (nrow(ward_grid) > 0) {
          export_data$TotalGridCells <- nrow(ward_grid)
          export_data$PercentClassified <- round(nrow(ward_annotations) / nrow(ward_grid) * 100, 1)
        }
      }
      
      # Write to CSV
      write.csv(export_data, file, row.names = FALSE)
      
      showNotification(paste("Successfully downloaded annotations for", input$selected_ward), type = "message")
    }
  )
  
  
  # Create ward map with grid classifications
  observeEvent(input$plot_ward_map, {
    req(input$selected_ward, rv$shp_data)
    
    withProgress(message = 'Processing ward data...', value = 0, {
      incProgress(0.2, detail = "Loading ward data...")
      
      # Create grid for the selected ward
      if (input$enable_grid) {
        # Create the grid (using 500m as default if not specified)
        grid_data <- create_ward_grid(
          input$selected_ward, 
          rv$shp_data, 
          as.numeric(input$grid_cell_size %||% 500)
        )
        
        # Store the grid in the reactive value
        if (!is.null(grid_data)) {
          # If we already have grids stored, update or append
          existing_grids <- rv$gridded_wards()
          if (!is.null(existing_grids)) {
            # Remove any existing grid for this ward
            existing_grids <- existing_grids[existing_grids$WardName != input$selected_ward, ]
            # Combine with the new grid
            grid_data <- rbind(existing_grids, grid_data)
          }
          rv$gridded_wards(grid_data)
          
          incProgress(0.6, detail = "Grid created successfully!")
        } else {
          showNotification("Could not create grid for this ward. Check console for errors.", type = "error")
        }
      }
      
      # Determine if this is a prioritized or re-prioritized ward
      ward_data <- rv$shp_data %>%
        filter(WardName == input$selected_ward)
      
      threshold <- 30
      filtered_ward <- filter_by_urban_extent(ward_data, threshold)
      is_prioritized <- filtered_ward$MeetsThreshold[1]
      
      # Render the map with our enhanced function
      output$ward_map <- renderLeaflet({
        process_and_view_shapefile_and_csv_enhanced(
          input$selected_ward, 
          rv$shp_data,
          rv$grid_annotations(),
          enable_grid = input$enable_grid,
          grid_cell_size = as.numeric(input$grid_cell_size %||% 500)
        )
      })
      
      incProgress(1, detail = "Map created!")
    })
  })
  
  # Observer for classification from popup
  observeEvent(input$classify_grid, {
    req(input$classify_grid, rv$gridded_wards())
    
    # Extract the classification data
    ward_name <- input$classify_grid$wardName
    grid_id <- as.integer(input$classify_grid$gridId)
    classification <- input$classify_grid$classification
    timestamp <- input$classify_grid$timestamp
    method <- input$classify_grid$method %||% "manual"
    
    # Create a data frame with the information
    new_annotation <- data.frame(
      WardName = ward_name,
      GridID = grid_id,
      Classification = classification,
      Timestamp = timestamp,
      Method = method,
      stringsAsFactors = FALSE
    )
    
    # Get current annotations
    annotations <- rv$grid_annotations()
    
    # Check if this grid cell is already annotated
    existing_idx <- which(annotations$WardName == ward_name & 
                            annotations$GridID == grid_id)
    
    if (length(existing_idx) > 0) {
      # Update existing annotation
      annotations$Classification[existing_idx] <- classification
      annotations$Timestamp[existing_idx] <- timestamp
      annotations$Method[existing_idx] <- method
    } else {
      # Add new annotation
      annotations <- rbind(annotations, new_annotation)
    }
    
    # Update annotations
    rv$grid_annotations(annotations)
    
    # Refresh the map with the updated classifications
    output$ward_map <- renderLeaflet({
      process_and_view_shapefile_and_csv_enhanced(
        input$selected_ward, 
        rv$shp_data,
        rv$grid_annotations(),
        enable_grid = input$enable_grid,
        grid_cell_size = as.numeric(input$grid_cell_size %||% 500)
      )
    })
    
    # Show notification
    showNotification(paste("Grid", grid_id, "classified as", classification), 
                     type = "message", duration = 2)
  })
  
  # NEW - Download Map functionality 
  output$download_map <- downloadHandler(
    filename = function() {
      paste0("classified_map_", 
             gsub(" ", "_", input$selected_ward), "_", 
             format(Sys.time(), "%Y%m%d_%H%M%S"), 
             ".png")
    },
    content = function(file) {
      # Create a temporary HTML file to save the leaflet widget
      temp_html <- tempfile(fileext = ".html")
      
      # Create a clean version of the map for download
      download_map <- create_downloadable_map(
        input$selected_ward, 
        rv$shp_data,
        rv$grid_annotations(),
        enable_grid = input$enable_grid,
        grid_cell_size = as.numeric(input$grid_cell_size %||% 500)
      )
      
      # Save the map to the temporary file
      htmlwidgets::saveWidget(download_map, temp_html, selfcontained = TRUE)
      
      # Add a delay to ensure all map elements are loaded
      Sys.sleep(1)
      
      # Use webshot2 to capture the map as PNG
      webshot2::webshot(
        url = temp_html,
        file = file,
        delay = 2,  # Wait 2 seconds for rendering
        cliprect = "viewport",
        zoom = 2  # Higher quality
      )
      
      # Clean up
      unlink(temp_html)
      
      # Show success notification
      showNotification("Map downloaded successfully!", type = "message")
    }
  )
  
  #============================================================================
  # Net Distribution
  #============================================================================
  
  #' Process grid annotations to find overrides for prioritized wards
  observe({
    req(rv$grid_annotations(), rv$shp_data)
    
    # Get all annotations
    annotations <- rv$grid_annotations()
    
    if (nrow(annotations) == 0) {
      rv$grid_overrides(NULL)
      return()
    }
    
    # Apply urban extent threshold filtering with fixed 30% threshold
    threshold <- 30
    filtered_data <- filter_by_urban_extent(rv$shp_data, threshold)
    
    # Find prioritized wards (below threshold) - UPDATED TERMINOLOGY
    prioritized_wards <- filtered_data %>%
      filter(MeetsThreshold) %>%
      pull(WardName)
    
    # Find grids in prioritized wards that are classified as formal/informal
    # These areas will receive nets in the prioritized distribution
    override_areas <- annotations %>%
      filter(WardName %in% prioritized_wards,
             Classification %in% c("Formal", "Informal")) %>%
      group_by(WardName) %>%
      summarize(
        FormalGrids = sum(Classification == "Formal"),
        InformalGrids = sum(Classification == "Informal"),
        TotalValidGrids = FormalGrids + InformalGrids,
        .groups = 'drop'
      ) %>%
      filter(TotalValidGrids >= 3) # Only consider wards with at least 3 valid grid cells
    
    # Store these overrides
    rv$grid_overrides(override_areas)
  })
  
  #' Calculate net distribution
  observeEvent(input$calculate_distribution, {
    req(rv$shp_data, rv$ward_rankings, input$total_nets, input$avg_household_size)
    
    # Show a loading message
    withProgress(message = 'Calculating optimal net distribution...', value = 0, {
      
      # Get the input values - with validation
      total_nets <- as.numeric(input$total_nets)
      if (is.na(total_nets) || total_nets <= 0) {
        total_nets <- 10000 # Default if invalid
        showNotification("Invalid net count. Using 10,000 as default.", type = "warning")
      }
      
      avg_household_size <- as.numeric(input$avg_household_size)
      if (is.na(avg_household_size) || avg_household_size <= 0) {
        avg_household_size <- 5 # Default if invalid
        showNotification("Invalid household size. Using 5 as default.", type = "warning")
      }
      
      # Use the threshold from the Box and Whisker Plot tab
      threshold <- as.numeric(input$urban_threshold)
      if (is.na(threshold) || threshold < 0 || threshold > 100) {
        threshold <- 30 # Default if invalid
        showNotification("Invalid threshold. Using 30% as default.", type = "warning")
      }
      
      # Join shapefile data with ward rankings
      ward_data <- left_join(rv$shp_data, rv$ward_rankings, by = "WardName")
      
      # Add area calculation if not present
      if (!"area" %in% names(ward_data)) {
        incProgress(0.1, detail = "Calculating area...")
        ward_data$area <- st_area(ward_data) %>% 
          as.numeric() / 1000000  # Convert to sq km
      }
      
      # Get manual grid overrides
      grid_overrides <- rv$grid_overrides()
      
      incProgress(0.3, detail = "Applying manual classification overrides...")
      
      # Calculate the net distribution with improved strategy
      distribution_results <- calculate_prioritized_net_distribution(
        ward_data = ward_data, 
        total_nets = total_nets, 
        avg_household_size = avg_household_size, 
        urban_threshold = threshold, 
        strategy = "rank", # Use ranking strategy
        grid_overrides = grid_overrides
      )
      
      # Verify we have valid data in the summary before storing
      if (is.null(distribution_results$summary$TargetPopulation) || 
          distribution_results$summary$TargetPopulation <= 0) {
        # Set a reasonable default
        distribution_results$summary$TargetPopulation <- sum(ward_data$EstimatedPopulation, na.rm = TRUE)
      }
      
      # Store results in reactive value
      rv$net_distribution_results(distribution_results)
      
      incProgress(1, detail = "Complete!")
      
      # Show a notification with key stats
      showNotification(
        paste0(
          "Distribution plan generated: ", 
          format(distribution_results$summary$NetsDistributed, big.mark = ","), 
          " nets allocated (", 
          distribution_results$summary$CoveragePercent, 
          "% coverage)"
        ), 
        type = "message", 
        duration = 5
      )
    })
  })
  
  #' Population statistic
  output$stat_population <- renderUI({
    req(rv$net_distribution_results)
    results <- rv$net_distribution_results()
    
    if (is.null(results) || is.null(results$summary)) {
      # Provide default values instead of NULL
      return(div(class = "stat-value", "0"))
    }
    
    # Ensure we use a valid population value
    population_value <- results$summary$TargetPopulation
    if (is.null(population_value) || is.na(population_value) || population_value <= 0) {
      # Try to get the total population from a different source
      if (!is.null(results$summary$TotalPopulation) && !is.na(results$summary$TotalPopulation)) {
        population_value <- results$summary$TotalPopulation
      } else {
        # Default to calculating from wards data if available
        if (!is.null(results$wards)) {
          population_value <- sum(results$wards$EstimatedPopulation, na.rm = TRUE)
        } else {
          population_value <- 0
        }
      }
    }
    
    tagList(
      div(class = "stat-value", format(population_value, big.mark = ",")),
      div(class = "stat-label", "TARGET POPULATION")
    )
  })
  
  #' Households statistic
  output$stat_households <- renderUI({
    req(rv$net_distribution_results)
    results <- rv$net_distribution_results()
    
    if (is.null(results) || is.null(results$summary)) {
      return(div(class = "stat-value", "0"))
    }
    
    # Get the households count, either directly or calculated from population
    households_value <- 0
    households_covered <- 0
    
    if (!is.null(results$summary$TotalHouseholds) && !is.na(results$summary$TotalHouseholds)) {
      households_value <- results$summary$TotalHouseholds
    } else if (!is.null(results$summary$TargetPopulation) && !is.na(results$summary$TargetPopulation)) {
      # Calculate from population using average household size
      avg_household_size <- input$avg_household_size
      if (is.null(avg_household_size) || is.na(avg_household_size) || avg_household_size <= 0) {
        avg_household_size <- 5 # Default value
      }
      households_value <- ceiling(results$summary$TargetPopulation / avg_household_size)
    }
    
    # Calculate households covered
    if (!is.null(results$summary$HouseholdsCovered) && !is.na(results$summary$HouseholdsCovered)) {
      households_covered <- results$summary$HouseholdsCovered
    } else if (!is.null(results$summary$PopulationCovered) && !is.na(results$summary$PopulationCovered)) {
      # Calculate from covered population
      avg_household_size <- input$avg_household_size
      if (is.null(avg_household_size) || is.na(avg_household_size) || avg_household_size <= 0) {
        avg_household_size <- 5 # Default value
      }
      households_covered <- ceiling(results$summary$PopulationCovered / avg_household_size)
    }
    
    tagList(
      div(class = "stat-value", 
          HTML(paste0(
            format(households_covered, big.mark = ","), 
            " / ", 
            format(households_value, big.mark = ",")
          ))),
      div(class = "stat-label", "HOUSEHOLDS COVERED / TOTAL")
    )
  })
  
  #' Nets statistic
  output$stat_nets <- renderUI({
    req(rv$net_distribution_results)
    results <- rv$net_distribution_results()
    
    if (is.null(results) || is.null(results$summary)) {
      return(div(class = "stat-value", "0 / 0"))
    }
    
    # Get nets distributed and nets needed
    nets_distributed <- 0
    nets_needed <- 0
    
    if (!is.null(results$summary$NetsDistributed) && !is.na(results$summary$NetsDistributed)) {
      nets_distributed <- results$summary$NetsDistributed
    }
    
    if (!is.null(results$summary$NetsNeeded) && !is.na(results$summary$NetsNeeded)) {
      nets_needed <- results$summary$NetsNeeded
    }
    
    tagList(
      div(class = "stat-value", 
          HTML(paste0(
            format(nets_distributed, big.mark = ","), 
            " / ", 
            format(nets_needed, big.mark = ",")
          ))),
      div(class = "stat-label", "NETS DISTRIBUTED / NEEDED")
    )
  })
  
  #' Coverage statistic
  output$stat_coverage <- renderUI({
    req(rv$net_distribution_results)
    results <- rv$net_distribution_results()
    
    if (is.null(results) || is.null(results$summary)) {
      return(div(class = "stat-value", "0%"))
    }
    
    # Get coverage percentage
    coverage <- 0
    
    if (!is.null(results$summary$CoveragePercent) && !is.na(results$summary$CoveragePercent)) {
      coverage <- results$summary$CoveragePercent
    }
    
    tagList(
      div(class = "stat-value", paste0(coverage, "%")),
      div(class = "stat-label", "COVERAGE")
    )
  })
  
  #' Population coverage bar
  output$population_coverage_bar <- renderUI({
    req(rv$net_distribution_results)
    results <- rv$net_distribution_results()
    
    if (is.null(results) || is.null(results$summary)) {
      return(div(
        "No distribution data available. Please click 'Calculate Distribution' to generate results.",
        style = "text-align: center; color: #777; padding: 20px;"
      ))
    }
    
    coverage <- results$summary$CoveragePercent
    
    # Determine color based on coverage level
    bar_color <- if (coverage < 50) {
      "linear-gradient(to right, #FF5722, #FF9800)"  # Red-orange for low coverage
    } else if (coverage < 75) {
      "linear-gradient(to right, #FFC107, #CDDC39)"  # Yellow-lime for medium coverage
    } else {
      "linear-gradient(to right, #4CAF50, #8BC34A)"  # Green for good coverage
    }
    
    # Determine message based on coverage
    if (coverage < 50) {
      status_message <- paste0("Critical: ", 
                               format(results$summary$NetsNeeded - results$summary$NetsDistributed, big.mark = ","), 
                               " more nets needed for full coverage.")
      status_color <- "#FF5722"
    } else if (coverage < 80) {
      status_message <- paste0("Attention: ", 
                               format(results$summary$NetsNeeded - results$summary$NetsDistributed, big.mark = ","), 
                               " more nets needed for full coverage.")
      status_color <- "#FF9800"
    } else if (coverage < 100) {
      status_message <- "Good coverage. Additional nets would achieve full coverage."
      status_color <- "#8BC34A"
    } else {
      status_message <- "Excellent! Full population coverage achieved."
      status_color <- "#4CAF50"
    }
    
    # Household coverage information
    households_info <- ""
    if (!is.null(results$summary$TotalHouseholds) && !is.null(results$summary$HouseholdsCovered)) {
      households_info <- paste0(
        "<div style='margin-top: 10px; background-color: #f5f5f5; padding: 10px; border-radius: 5px;'>",
        "<strong>Household Coverage:</strong> ", 
        format(results$summary$HouseholdsCovered, big.mark = ","), 
        " of ", 
        format(results$summary$TotalHouseholds, big.mark = ","), 
        " households covered (", 
        round(results$summary$HouseholdsCovered / max(results$summary$TotalHouseholds, 1) * 100, 1), 
        "%)",
        "</div>"
      )
    }
    
    # Prioritized vs de-prioritized areas details
    reprioritized_coverage <- 0
    if (!is.null(results$summary$ReprioritizedCoverage)) {
      reprioritized_coverage <- results$summary$ReprioritizedCoverage
    }
    
    prioritized_message <- paste0(
      "Prioritized areas: ", 
      results$summary$PrioritizedCoverage, "% coverage (",
      format(results$summary$PrioritizedNets, big.mark = ","), " nets)"
    )
    
    deprioritized_message <- paste0(
      "De-prioritized areas: ", 
      reprioritized_coverage, "% coverage (",
      format(results$summary$ReprioritizedNets, big.mark = ","), " nets)"
    )
    
    tagList(
      div(class = "coverage-bar",
          div(class = "coverage-fill",
              paste0(coverage, "% Population Covered"),
              style = paste0("width: ", coverage, "%; background: ", bar_color, ";")
          )
      ),
      div(
        style = paste0("margin-top: 10px; color: ", status_color, "; font-weight: bold;"),
        status_message
      ),
      div(
        style = "margin-top: 5px; color: #555;",
        paste0("With ", format(results$summary$TotalNets, big.mark = ","), " nets, you can cover ", 
               format(results$summary$PopulationCovered, big.mark = ","), " out of ", 
               format(results$summary$TargetPopulation, big.mark = ","), " people in targeted areas.")
      ),
      div(
        style = "margin-top: 5px; display: flex; justify-content: space-between; color: #666;",
        div(prioritized_message),
        div(deprioritized_message)
      ),
      HTML(households_info)
    )
  })
  
  #' Distribution summary
  output$distribution_summary <- renderUI({
    req(rv$net_distribution_results)
    results <- rv$net_distribution_results()
    
    if (is.null(results) || is.null(results$summary)) {
      return(NULL)
    }
    
    # Count wards by priority type
    prioritized_count <- results$summary$PrioritizedWards
    grid_classified_count <- results$summary$GridClassifiedWards
    reprioritized_count <- results$summary$ReprioritizedWards
    
    # Create summary box
    div(
      style = "background-color: #f8f9fa; border-radius: 8px; padding: 15px; margin: 15px 0;",
      h4("Distribution Strategy Summary", style = "margin-top: 0; color: #5D4E6D;"),
      div(style = "display: flex; flex-wrap: wrap; gap: 15px; margin-bottom: 10px;",
          div(style = "flex: 1; min-width: 200px; background-color: #e8f5e9; padding: 10px; border-radius: 5px;",
              h5(style = "margin-top: 0;", paste0("Prioritized Wards (<", input$urban_threshold, "% urban)")),
              p(style = "margin-bottom: 5px;", 
                HTML(paste0("<strong>", prioritized_count, "</strong> wards below selected urban threshold"))),
              p(style = "margin-bottom: 0;", 
                HTML(paste0("<strong>", results$summary$PrioritizedNets, "</strong> nets allocated")))
          ),
          
          # Grid-classified wards info (only show if there are any)
          if (grid_classified_count > 0) {
            div(style = "flex: 1; min-width: 200px; background-color: #fff8e1; padding: 10px; border-radius: 5px;",
                h5(style = "margin-top: 0;", "Grid-Classified Areas"),
                p(style = "margin-bottom: 5px;", 
                  HTML(paste0("<strong>", grid_classified_count, "</strong> wards with manual grid classification"))),
                p(style = "margin-bottom: 0;", 
                  HTML(paste0("<strong>", results$summary$GridClassifiedNets, "</strong> nets allocated")))
            )
          },
          
          # Re-prioritized wards info
          div(style = "flex: 1; min-width: 200px; background-color: #ffebee; padding: 10px; border-radius: 5px;",
              h5(style = "margin-top: 0;", paste0("Re-prioritized Wards (>", input$urban_threshold, "% urban)")),
              p(style = "margin-bottom: 5px;", 
                HTML(paste0("<strong>", reprioritized_count, "</strong> wards above selected urban threshold"))),
              p(style = "margin-bottom: 0;", 
                HTML(paste0("<strong>", results$summary$ReprioritizedNets, "</strong> nets allocated")))
          )
      ),
      
      # Show remaining nets information
      if (results$summary$RemainingNets > 0) {
        div(style = "background-color: #e3f2fd; padding: 10px; border-radius: 5px; margin-top: 10px;",
            p(style = "margin: 0;", 
              HTML(paste0("<strong>", results$summary$RemainingNets, "</strong> nets remaining unallocated. Consider increasing the population estimates for target areas.")))
        )
      }
    )
  })
  
  #' Distribution map
  output$distribution_map <- renderLeaflet({
    req(rv$net_distribution_results)
    results <- rv$net_distribution_results()
    
    if (is.null(results) || is.null(results$wards)) {
      return(
        leaflet() %>%
          addTiles() %>%
          addControl(
            html = paste("<div style='padding: 15px; background: white; border-radius: 5px;'>",
                         "<h4>No Distribution Data</h4>",
                         "<p>Please calculate distribution first using the button above.</p>",
                         "</div>"),
            position = "topright"
          )
      )
    }
    
    # Prepare data for mapping
    map_data <- results$wards %>%
      st_transform(crs = 4326) # Transform to WGS84 for leaflet
    
    # Create a custom color palette for coverage with different colors by priority
    get_color <- function(coverage, priority) {
      if (priority == "Prioritized") {
        if (coverage <= 0) return("#CCCCCC")
        else if (coverage < 25) return("#FF5722")
        else if (coverage < 50) return("#FFC107")
        else if (coverage < 75) return("#8BC34A")
        else if (coverage < 100) return("#4CAF50")
        else return("#1B5E20")  # 100% coverage
      } else if (priority == "Grid-Classified") {
        if (coverage <= 0) return("#CCCCCC")
        else if (coverage < 25) return("#FF9800")
        else if (coverage < 50) return("#FFB74D")
        else if (coverage < 75) return("#FFD54F")
        else if (coverage < 100) return("#FFE082")
        else return("#FFD700")  # 100% coverage - gold
      } else {
        # For Re-prioritized wards
        if (coverage <= 0) return("#E0E0E0")  # Gray if no coverage
        else if (coverage < 25) return("#FFCDD2")  # Very light red
        else if (coverage < 50) return("#EF9A9A")  # Light red
        else if (coverage < 75) return("#E57373")  # Medium red
        else if (coverage < 100) return("#EF5350")  # Darker red
        else return("#C62828")  # Very dark red for 100% coverage
      }
    }
    
    # Apply colors
    map_data$DisplayColor <- mapply(get_color, map_data$CoveragePercent, map_data$Priority)
    
    # Create label text based on priority and coverage
    map_data$LabelText <- ifelse(
      map_data$Priority == "Re-prioritized" & map_data$CoveragePercent == 0,
      paste0(map_data$WardName, " (Re-prioritized)"),
      paste0(map_data$WardName, ": ", map_data$CoveragePercent, "% Coverage")
    )
    
    # Build leaflet map
    map <- leaflet() %>%
      addTiles() %>%
      addPolygons(
        data = map_data,
        fillColor = ~DisplayColor,
        fillOpacity = 0.7,
        color = "#333333",
        weight = 1,
        smoothFactor = 0.2,
        highlightOptions = highlightOptions(
          weight = 2,
          color = "#666",
          fillOpacity = 0.9,
          bringToFront = TRUE
        ),
        label = ~LabelText,
        popup = ~paste0(
          "<div style='min-width: 150px;'>",
          "<h4 style='margin-top: 0;'>", WardName, "</h4>",
          "<table style='width: 100%; border-collapse: collapse;'>",
          "<tr><td>Priority:</td><td>", Priority, "</td></tr>",
          "<tr><td>Urban %:</td><td>", round(UrbanPercent, 1), "%</td></tr>",
          "<tr><td>Coverage:</td><td>", CoveragePercent, "%</td></tr>",
          "<tr><td>Population:</td><td>", format(EstimatedPopulation, big.mark = ","), "</td></tr>",
          "<tr><td>Nets Allocated:</td><td>", format(NetsAllocated, big.mark = ","), "</td></tr>",
          "<tr><td>Nets Needed:</td><td>", format(NetsNeeded, big.mark = ","), "</td></tr>",
          "</table>",
          "</div>"
        )
      )
    
    # Add legends
    map <- map %>%
      addLegend(
        position = "bottomright",
        colors = c("#1B5E20", "#4CAF50", "#8BC34A", "#FFC107", "#FF5722", "#FFD700", "#E0E0E0"),
        labels = c("Full Coverage (100%)", "High (76-99%)", "Good (51-75%)", "Medium (26-50%)", 
                   "Low (1-25%)", "Grid-Classified", "Re-prioritized"),
        title = "Coverage Level",
        opacity = 0.7
      )
    
    return(map)
  })
  
  #' Ward coverage table
  output$ward_coverage_table <- renderDT({
    req(rv$net_distribution_results)
    results <- rv$net_distribution_results()
    
    if (is.null(results) || is.null(results$wards)) {
      return(NULL)
    }
    
    # Prepare data for table
    table_data <- results$wards %>%
      st_drop_geometry() %>%
      select(WardName, Priority, UrbanPercent, overall_rank, EstimatedPopulation, 
             NetsNeeded, NetsAllocated, CoveragePercent) %>%
      arrange(Priority, overall_rank)
    
    # Format for display
    display_data <- table_data %>%
      mutate(
        `Ward Name` = WardName,
        `Priority` = Priority,
        `Urban %` = round(UrbanPercent, 1),
        `Vulnerability Rank` = overall_rank,
        `Population` = EstimatedPopulation,
        `Nets Needed` = NetsNeeded,
        `Nets Allocated` = NetsAllocated,
        `Coverage %` = CoveragePercent
      ) %>%
      select(`Ward Name`, `Priority`, `Urban %`, `Vulnerability Rank`, 
             `Population`, `Nets Needed`, `Nets Allocated`, `Coverage %`)
    
    # Create the datatable
    datatable(display_data,
              options = list(
                pageLength = 10,
                autoWidth = TRUE,
                dom = 'Bfrtip',
                buttons = c('copy', 'csv', 'excel')
              ),
              rownames = FALSE) %>%
      formatStyle(
        'Priority',
        backgroundColor = styleEqual(
          c("Prioritized", "Re-prioritized", "Grid-Classified"),
          c("#E8F5E9", "#FFEBEE", "#FFF8E1")
        )
      ) %>%
      formatStyle(
        'Coverage %',
        backgroundColor = styleInterval(
          c(1, 25, 50, 75, 99),
          c('#CCCCCC', '#FF5722', '#FFC107', '#8BC34A', '#4CAF50', '#1B5E20')
        ),
        color = styleInterval(
          c(50),
          c('black', 'white')
        )
      ) %>%
      formatCurrency('Population', currency = "", interval = 3, mark = ",", digits = 0) %>%
      formatCurrency('Nets Needed', currency = "", interval = 3, mark = ",", digits = 0) %>%
      formatCurrency('Nets Allocated', currency = "", interval = 3, mark = ",", digits = 0)
  })
  
  #' Manual override table (grid classifications)
  output$manual_override_table <- renderDT({
    req(rv$net_distribution_results, rv$grid_overrides)
    results <- rv$net_distribution_results()
    overrides <- rv$grid_overrides()
    
    if (is.null(results) || is.null(results$wards) || is.null(overrides) || nrow(overrides) == 0) {
      return(datatable(
        data.frame(
          Message = "No grid classifications found. To create these, use the Manual Labelling tab to classify grid cells in priority wards."
        ),
        options = list(dom = 't'),
        rownames = FALSE
      ))
    }
    
    # Get data for manually prioritized wards
    grid_classified_wards <- results$wards %>%
      st_drop_geometry() %>%
      filter(Priority == "Grid-Classified") %>%
      select(WardName, UrbanPercent, overall_rank, EstimatedPopulation, 
             AdjustedPopulation, NetsNeeded, NetsAllocated, CoveragePercent)
    
    # Merge with grid override info
    override_data <- left_join(grid_classified_wards, overrides, by = "WardName") %>%
      mutate(
        `Ward Name` = WardName,
        `Urban %` = round(UrbanPercent, 1),
        `Vulnerability Rank` = overall_rank,
        `Total Population` = EstimatedPopulation,
        `Adjusted Population` = AdjustedPopulation,
        `Formal Grid Cells` = FormalGrids,
        `Informal Grid Cells` = InformalGrids,
        `Total Valid Cells` = TotalValidGrids,
        `Nets Allocated` = NetsAllocated,
        `Coverage %` = CoveragePercent
      ) %>%
      select(`Ward Name`, `Urban %`, `Vulnerability Rank`, `Total Population`, 
             `Adjusted Population`, `Formal Grid Cells`, `Informal Grid Cells`, 
             `Total Valid Cells`, `Nets Allocated`, `Coverage %`)
    
    # Create the datatable
    datatable(override_data,
              caption = "Wards with Grid Classifications",
              options = list(
                pageLength = 10,
                autoWidth = TRUE,
                dom = 'Bfrtip',
                buttons = c('copy', 'csv', 'excel')
              ),
              rownames = FALSE) %>%
      formatStyle(
        'Coverage %',
        backgroundColor = styleInterval(
          c(1, 25, 50, 75, 99),
          c('#CCCCCC', '#FF5722', '#FFC107', '#8BC34A', '#4CAF50', '#1B5E20')
        ),
        color = styleInterval(
          c(50),
          c('black', 'white')
        )
      ) %>%
      formatCurrency('Total Population', currency = "", interval = 3, mark = ",", digits = 0) %>%
      formatCurrency('Adjusted Population', currency = "", interval = 3, mark = ",", digits = 0) %>%
      formatCurrency('Nets Allocated', currency = "", interval = 3, mark = ",", digits = 0)
  })
  
  #' Download distribution plan
  output$download_distribution <- downloadHandler(
    filename = function() {
      paste0("net_distribution_plan_", format(Sys.Date(), "%Y%m%d"), ".csv")
    },
    content = function(file) {
      req(rv$net_distribution_results)
      results <- rv$net_distribution_results()
      
      if (is.null(results) || is.null(results$wards)) {
        # If no results, create a message file
        write.csv(data.frame(Message = "No distribution results available. Please calculate distribution first."), 
                  file, row.names = FALSE)
        return()
      }
      
      # Prepare export data
      export_data <- results$wards %>%
        st_drop_geometry() %>%
        mutate(
          Date = format(Sys.Date(), "%Y-%m-%d"),
          UrbanThreshold = 30,  # Fixed 30% threshold
          TotalNetsAvailable = input$total_nets,
          AverageHouseholdSize = input$avg_household_size
        )
      
      # Write to CSV
      write.csv(export_data, file, row.names = FALSE)
    }
  )
  
  #' Observer to link urban threshold between tabs
  observe({
    req(input$urban_threshold)
    if (input$urban_threshold != 0) {  # Only sync when a threshold is selected
      updateNumericInput(session, "dist_urban_threshold", value = input$urban_threshold)
    }
  })
  
  
  # In server function, add this to update information about population data source
  output$population_data_source <- renderUI({
    req(rv$net_distribution_results)
    results <- rv$net_distribution_results()
    
    if (is.null(results) || is.null(results$wards)) {
      return(NULL)
    }
    
    # Check if we found any ITN data matches
    matches_found <- results$wards$ITNDataMatches[1] %||% 0
    total_wards <- results$wards$TotalWards[1] %||% 0
    state_name <- results$summary$StateName %||% "this"
    
    if (matches_found > 0) {
      div(
        style = "padding: 15px; background-color: #dff0d8; border-left: 5px solid #5cb85c; margin: 15px 0;",
        h4("Population Data Source", style = "margin-top: 0; color: #3c763d;"),
        p(paste0("Using actual population data from ITN distribution records for ", 
                 state_name, " state."), 
          style = "font-weight: bold;"),
        p(paste0("Found ITN distribution data for ", matches_found, 
                 " out of ", total_wards, " wards (", 
                 round(matches_found/total_wards*100), "% coverage)."))
      )
    } else {
      div(
        style = "padding: 15px; background-color: #fcf8e3; border-left: 5px solid #f0ad4e; margin: 15px 0;",
        h4("Population Data Source", style = "margin-top: 0; color: #8a6d3b;"),
        p("Using estimated population based on settlement types and densities.", 
          style = "font-weight: bold;"),
        if (is.null(state_name)) {
          p("Could not determine which state this area belongs to.")
        } else {
          p(paste0("No matching ward names found in ITN distribution data for ", state_name, " state."))
        }
      )
    }
  })
  
  # Add these to the server function
  output$phase1_description <- renderUI({
    req(input$urban_threshold)
    threshold <- as.numeric(input$urban_threshold)
    
    div(class = "phase phase-1",
        h6("Phase 1: Prioritized Areas", style = "margin-top: 0;"),
        p(paste0("Nets are first distributed to the most vulnerable wards below the ", 
                 threshold, "% urban threshold, prioritized by vulnerability rank."))
    )
  })
  
  output$phase2_description <- renderUI({
    req(input$urban_threshold)
    threshold <- as.numeric(input$urban_threshold)
    
    div(class = "phase phase-2",
        h6("Phase 2: Re-prioritized Areas", style = "margin-top: 0;"),
        p(paste0("If nets remain after Phase 1, they are distributed to wards above ", 
                 threshold, "% threshold, with manually classified areas receiving special consideration."))
    )
  })
  
  # Then add this to the server:
  output$map_description <- renderUI({
    req(input$urban_threshold)
    threshold <- as.numeric(input$urban_threshold)
    
    p(paste0("This map shows where nets will be distributed, prioritizing wards below ", 
             threshold, "% urban threshold. Gray areas are re-prioritized and will receive nets if available."), 
      style = "font-style: italic; color: #666; margin-bottom: 10px;")
  })
  
  
  #============================================================================
  # Synchronization Between Tabs
  #============================================================================
  
  # Synchronize ward selection across tabs
  observeEvent(input$selected_ward, {
    updateSelectInput(session, "population_ward", selected = input$selected_ward)
  }, ignoreInit = TRUE)
  
  observeEvent(input$population_ward, {
    updateSelectInput(session, "selected_ward", selected = input$population_ward)
  }, ignoreInit = TRUE)
  
  # Synchronize urban threshold across tabs
  observe({
    req(input$urban_threshold)
    
    # Only sync when a valid threshold is selected
    if (!is.na(input$urban_threshold) && input$urban_threshold > 0) {
      updateNumericInput(session, "manual_urban_threshold", value = input$urban_threshold)
    }
  })
  
  observe({
    req(input$manual_urban_threshold)
    
    # Sync with urban threshold if different
    if (!is.na(input$manual_urban_threshold) && 
        input$manual_urban_threshold != input$urban_threshold) {
      updateNumericInput(session, "urban_threshold", value = input$manual_urban_threshold)
    }
  })
}

#==============================================================================
# Complete App Definition
#==============================================================================

shinyApp(ui = ui, server = server)