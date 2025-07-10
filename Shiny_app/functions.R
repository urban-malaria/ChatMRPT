# Load required libraries
library(sf)
library(dplyr)
library(tidyr)
library(reshape2)
library(ggplot2)
library(ggiraph)
library(RColorBrewer)
library(stringr)
library(viridis)
library(leaflet)
library(spdep)
library(plotly)
library(DiagrammeR)
library(DiagrammeRsvg)
library(rsvg)
library(ggtext)
library(DT)
library(htmlwidgets)
library(webshot2)
library(leaflet.extras)
library(data.table)

# Define NULL-default operator
`%||%` <- function(x, y) if (is.null(x)) y else x

# ==============================================================================
# DATA HANDLING AND MANIPULATION
# ==============================================================================

#' Process CSV file with duplicate checking and handling
#' 
#' @param file_path Path to CSV file
#' @return Dataframe with unique ward identifiers
process_csv_with_duplicate_handling <- function(file_path) {
  # Read file based on extension
  csv_data <- if (tolower(tools::file_ext(file_path)) %in% c("xlsx", "xls")) {
    readxl::read_excel(file_path)
  } else {
    read.csv(file_path)
  }
  
  # Process the data
  csv_data <- rename_columns(as.data.frame(csv_data))
  
  # Check for duplicate ward names
  if (any(duplicated(csv_data$WardName))) {
    # Create a unique ID column if WardCode is available
    if ("WardCode" %in% names(csv_data)) {
      csv_data$OriginalWardName <- csv_data$WardName  # Store original names
      csv_data$WardName <- paste(csv_data$WardCode, csv_data$WardName, sep = "_")
    } else {
      # If no WardCode, create numbered suffixes for duplicates
      dup_wards <- csv_data$WardName[duplicated(csv_data$WardName)]
      
      for (ward in unique(dup_wards)) {
        indices <- which(csv_data$WardName == ward)
        csv_data$OriginalWardName <- csv_data$WardName  # Store original names
        csv_data$WardName[indices] <- paste0(csv_data$WardName[indices], "_", seq_along(indices))
      }
    }
  }
  
  return(csv_data)
}

#' Process shapefile with duplicate checking and handling
#' 
#' @param file_path Path to shapefile
#' @param csv_data Processed CSV data with unique ward identifiers
#' @return SF object with unique ward identifiers
process_shapefile_with_duplicate_handling <- function(file_path, csv_data) {
  shp_data <- st_read(file_path, quiet = TRUE)
  
  # Check for "Ward" column and rename to "WardName" if needed
  if ("Ward" %in% names(shp_data) && !"WardName" %in% names(shp_data)) {
    shp_data <- shp_data %>% rename(WardName = Ward)
  }
  
  # Check for duplicate ward names
  if (any(duplicated(shp_data$WardName))) {
    # Create a unique ID column if WardCode is available
    if ("WardCode" %in% names(shp_data)) {
      shp_data$OriginalWardName <- shp_data$WardName  # Store original names
      shp_data$WardName <- paste(shp_data$WardCode, shp_data$WardName, sep = "_")
    } else {
      # If no WardCode, create numbered suffixes for duplicates
      dup_wards <- shp_data$WardName[duplicated(shp_data$WardName)]
      
      for (ward in unique(dup_wards)) {
        indices <- which(shp_data$WardName == ward)
        shp_data$OriginalWardName <- shp_data$WardName  # Store original names
        shp_data$WardName[indices] <- paste0(shp_data$WardName[indices], "_", seq_along(indices))
      }
    }
  } else if ("OriginalWardName" %in% names(csv_data)) {
    # If CSV data had duplicates but shapefile doesn't, align shapefile with CSV
    shp_data$OriginalWardName <- shp_data$WardName
    
    # Create a mapping table from original to new ward names
    ward_mapping <- csv_data %>%
      select(OriginalWardName, WardName) %>%
      distinct()
    
    # Match shapefile names with CSV processed names
    for (i in 1:nrow(ward_mapping)) {
      orig_name <- ward_mapping$OriginalWardName[i]
      new_name <- ward_mapping$WardName[i]
      
      # Update shapefile names to match CSV
      shp_data$WardName[shp_data$WardName == orig_name] <- new_name
    }
  }
  
  return(shp_data)
}



#' Rename columns in a dataframe for consistency
#'
#' @param df Dataframe to process
#' @return Dataframe with renamed columns
rename_columns <- function(df) {
  # First rename Ward to WardName if it exists
  if ("Ward" %in% names(df)) {
    df <- df %>% rename(WardName = Ward)
  }
  
  # Generate name patterns
  pattern_list <- sapply(names(df), function(col) {
    pattern <- tolower(gsub("\\s+", "_", col))
    c(col, pattern)
  }, simplify = FALSE, USE.NAMES = TRUE)
  
  # Apply rename with patterns
  for (pattern in names(pattern_list)) {
    df <- df %>% rename_with(~ gsub("\\.", " ", pattern), 
                             all_of(intersect(names(df), pattern_list[[pattern]])))
  }
  
  return(df)
}

#' Get columns after WardName column, optionally filtering for specific columns
#'
#' @param data Dataframe to process
#' @param specific_columns Optional vector of specific column names to filter for
#' @return Vector of column names
get_columns_after_wardname <- function(data, specific_columns = NULL) {
  # Check for either Ward or WardName
  ward_col <- intersect(c("Ward", "WardName"), names(data))
  
  if (length(ward_col) == 0) {
    warning("Neither Ward nor WardName column found. Returning all numeric columns.")
    return(names(data)[sapply(data, is.numeric)])
  }
  
  ward_name_index <- which(names(data) == ward_col[1])
  columns_after_wardname <- names(data)[(ward_name_index + 1):ncol(data)]
  
  # Filter for numeric columns
  columns_after_wardname <- columns_after_wardname[sapply(data[columns_after_wardname], is.numeric)]
  
  if (!is.null(specific_columns)) {
    columns_after_wardname <- intersect(columns_after_wardname, specific_columns)
  }
  
  return(columns_after_wardname)
}

#' Check for missing values in a dataframe
#'
#' @param data Dataframe to check
#' @return List with columns containing missing values and the original data
check_missing_values <- function(data) {
  missing_cols <- sapply(data, function(x) any(is.na(x)))
  cols_with_missing <- names(missing_cols[missing_cols])
  return(list(columns = cols_with_missing, data = data))
}

#' Check for ward name mismatches between CSV and shapefile data
#' 
#' @param csv_data CSV data containing ward names
#' @param shp_data Shapefile data containing ward names
#' @return Dataframe of mismatched ward names or NULL if no mismatches
check_wardname_mismatches <- function(csv_data, shp_data) {
  csv_wardnames <- csv_data$WardName
  shp_wardnames <- shp_data$WardName
  
  # Determine if we're using processed names with originals stored
  using_processed_names <- "OriginalWardName" %in% names(csv_data) && 
    "OriginalWardName" %in% names(shp_data)
  
  mismatched_wards <- setdiff(csv_wardnames, shp_wardnames)
  
  if (length(mismatched_wards) > 0) {
    if (using_processed_names) {
      # If using processed names, include both original and processed in options
      mismatches <- data.frame(
        CSV_WardName = mismatched_wards,
        CSV_OriginalName = sapply(mismatched_wards, function(w) {
          orig <- csv_data$OriginalWardName[csv_data$WardName == w]
          if (length(orig) > 0) orig[1] else w
        }),
        Shapefile_Options = I(replicate(length(mismatched_wards), list(shp_wardnames))),
        Original_Shapefile_Options = I(replicate(length(mismatched_wards), 
                                                 list(unique(shp_data$OriginalWardName)))),
        stringsAsFactors = FALSE
      )
    } else {
      # Standard behavior if not using processed names
      mismatches <- data.frame(
        CSV_WardName = mismatched_wards,
        Shapefile_Options = I(replicate(length(mismatched_wards), list(shp_wardnames))),
        stringsAsFactors = FALSE
      )
    }
    return(mismatches)
  } else {
    return(NULL)
  }
}

# ==============================================================================
# DATA CLEANING AND IMPUTATION
# ==============================================================================

#' Handle missing values using spatial neighbor mean
#'
#' @param data Dataframe containing the data
#' @param shp_data Shapefile data for spatial relationships
#' @param col Column name to process, or NULL to process all columns with missing values
#' @return Dataframe with imputed values
handle_na_neighbor_mean <- function(data, shp_data, col = NULL) {
  if (is.null(col)) {
    cols_to_process <- names(data)[sapply(data, function(x) any(is.na(x)))]
  } else {
    cols_to_process <- col
  }
  
  # Create neighbor structure using shapefile data
  w <- spdep::poly2nb(shp_data, queen = TRUE)
  
  for (current_col in cols_to_process) {
    col_data <- data[[current_col]]
    missing_indices <- which(is.na(col_data))
    
    print(paste("Processing column:", current_col))
    print(paste("Number of NAs:", length(missing_indices)))
    
    for (index in missing_indices) {
      neighbor_indices <- w[[index]]
      neighbor_values <- col_data[neighbor_indices]
      imputed_value <- mean(neighbor_values, na.rm = TRUE)
      
      if (is.na(imputed_value)) {
        imputed_value <- mean(col_data, na.rm = TRUE)
      }
      
      col_data[index] <- imputed_value
      print(paste("Imputed value for index", index, ":", imputed_value))
    }
    
    # Only update the NAs in the original data
    data[[current_col]][missing_indices] <- col_data[missing_indices]
  }
  
  return(data)
}

#' Handle missing values using mean of entire region
#'
#' @param data Dataframe containing the data
#' @param col Column name to process, or NULL to process all columns with missing values
#' @return Dataframe with imputed values
handle_na_region_mean <- function(data, col = NULL) {
  if (is.null(col)) {
    cols_to_process <- names(data)[sapply(data, function(x) any(is.na(x)))]
  } else {
    cols_to_process <- col
  }
  
  for (current_col in cols_to_process) {
    data[[current_col]][is.na(data[[current_col]])] <- mean(data[[current_col]], na.rm = TRUE)
  }
  
  return(data)
}

#' Handle missing values using mode of entire region
#'
#' @param data Dataframe containing the data
#' @param col Column name to process, or NULL to process all columns with missing values
#' @return Dataframe with imputed values
handle_na_region_mode <- function(data, col = NULL) {
  get_mode <- function(x) {
    ux <- unique(x)
    ux[which.max(tabulate(match(x, ux)))]
  }
  
  if (is.null(col)) {
    cols_to_process <- names(data)[sapply(data, function(x) any(is.na(x)))]
  } else {
    cols_to_process <- col
  }
  
  for (current_col in cols_to_process) {
    data[[current_col]][is.na(data[[current_col]])] <- get_mode(data[[current_col]][!is.na(data[[current_col]])])
  }
  
  return(data)
}

# ==============================================================================
# PLOTTING AND VISUALIZATION
# ==============================================================================

#' Plot map for visualizing variable distribution in wards
#'
#' @param variable_name Name of the variable to plot
#' @param shp_data_reactive Shapefile data
#' @param dataframe_reactive Dataframe with variables
#' @param title Plot title
#' @param na_handling_method NA handling method (for display purposes)
#' @return Girafe object with interactive map
plot_map_00 <- function(variable_name, 
                        shp_data_reactive, 
                        dataframe_reactive, 
                        title, 
                        na_handling_method = NULL) {
  # Ensure the dataframe has a geometry column
  if (!"geometry" %in% names(dataframe_reactive)) {
    dataframe_reactive <- left_join(shp_data_reactive, dataframe_reactive, by = "WardName")
  }
  
  na_method_text <- if (!is.null(na_handling_method)) {
    paste("\nNA Handling: ", na_handling_method)
  } else {
    ""
  }
  
  plot <- ggplot() +
    geom_sf_interactive(data = shp_data_reactive, color = "black", fill = "white") +
    geom_sf_interactive(data = dataframe_reactive,
                        aes(fill = !!sym(variable_name),  
                            tooltip = paste(WardName, "(", round(as.numeric(!!sym(variable_name)), 3), ")"))) +
    scale_fill_gradientn(colors = brewer.pal(9, "Blues"), name = "") +
    labs(title = paste(title, na_method_text), subtitle = variable_name, fill = "", x = NULL, y = NULL) +
    theme_minimal() +
    theme(legend.position = "right",
          legend.title = element_text(size = 14),
          legend.text = element_text(size = 12),
          plot.title = element_text(hjust = 0.5, size = 16, face = "bold"),
          plot.subtitle = element_text(hjust = 0.5, size = 14),
          axis.text = element_blank(),
          axis.ticks = element_blank(),
          panel.grid = element_blank())
  
  girafe(ggobj = plot, width_svg = 10, height_svg = 8)
}

#' Set custom theme for maps
#'
#' @return Theme object
map_theme <- function(){
  theme(axis.text.x = element_blank(),
        axis.text.y = element_blank(),
        axis.ticks = element_blank(),
        rect = element_blank(),
        plot.background = element_rect(fill = "white", colour = NA), 
        plot.title = element_text(hjust = 0.5),
        legend.title = element_text(hjust = 0.5, size = 8, colour = 'black'),
        legend.text = element_text(size = 8, colour = 'black'),
        legend.key.height = unit(0.65, "cm"))
}

#' Set manuscript theme for plots
#'
#' @return Theme object
theme_manuscript <- function(){
  theme_bw() + 
    theme(panel.border = element_rect(colour = "black", fill=NA, linewidth=0.5),
          plot.title = element_text(hjust = 0.5),
          axis.text.x = element_text(size = 22, color = "black"), 
          axis.text.y = element_text(size = 22, color = "black"),
          axis.title.x = element_text(size = 22),
          axis.title.y = element_text(size =22),
          legend.title=element_text(size=22, colour = 'black'),
          legend.text =element_text(size = 22, colour = 'black'),
          legend.key.height = unit(1, "cm"))
}

# ==============================================================================
# NORMALIZATION AND SCORING
# ==============================================================================

#' Normalize data based on variable relationships
#'
#' @param cleaned_data Dataframe with cleaned data
#' @param variable_relationships Named list with relationships (direct/inverse)
#' @return Dataframe with normalized variables
normalize_data <- function(cleaned_data, variable_relationships) { 
  tryCatch({
    print("Input data structure (cleaned data):")
    print(str(cleaned_data))
    print("Variable relationships:")
    print(variable_relationships) 
    
    # Identify numeric columns for normalization
    numeric_cols <- names(cleaned_data)[sapply(cleaned_data, is.numeric)]
    numeric_cols <- intersect(numeric_cols, names(variable_relationships))
    
    print("Numeric columns to be normalized:")
    print(numeric_cols)
    
    if (length(numeric_cols) == 0) {
      stop("No numeric columns found for normalization!")
    }
    
    # Apply normalization to numeric columns based on relationships
    scoring_data <- cleaned_data %>% 
      mutate(across(all_of(numeric_cols), 
                    ~{
                      col_name <- cur_column()
                      if (variable_relationships[col_name] == "inverse") {
                        inverted <- 1 / (. + 1e-10)  # Add small constant to avoid division by zero
                        ((inverted - min(inverted, na.rm = TRUE)) / 
                            (max(inverted, na.rm = TRUE) - min(inverted, na.rm = TRUE)))
                      } else {  
                        (. - min(., na.rm = TRUE)) / 
                          (max(., na.rm = TRUE) - min(., na.rm = TRUE)) 
                      }
                    },
                    .names = "normalization_{tolower(.col)}"))
    
    print("Normalized data summary:")
    print(summary(scoring_data))
    
    return(scoring_data)
    
  }, error = function(e) {
    print(paste("Error in normalize_data:", e$message))
    print(traceback()) 
    return(NULL)
  })
} 

#' Plot normalized map
#'
#' @param shp_data Shapefile data
#' @param processed_csv Processed CSV data with normalized variables
#' @param selected_vars Selected variables to plot
#' @return Girafe object with interactive map
plot_normalized_map <- function(shp_data, processed_csv, selected_vars) {
  palette_func <- brewer.pal(5, "YlOrRd")
  
  selected_cols <- c("WardName", selected_vars)
  filtered_data <- processed_csv %>%  
    select(all_of(selected_cols)) %>% 
    pivot_longer(cols = -WardName, names_to = "variable", values_to = "value") 
  
  combined_data <- left_join(filtered_data, shp_data, by = "WardName") 
  
  plot <- ggplot(data = shp_data) +
    geom_sf_interactive(color = "black", fill = "white") + 
    geom_sf_interactive(data = combined_data,
                        aes(geometry = geometry, fill = value, 
                            tooltip = paste(WardName, variable, 
                                            "\nValue:", round(value, 3)))) +
    scale_fill_gradientn(colours = palette_func, name = "Normalized Value") +
    labs(title = paste('Normalized Variable:', gsub("normalization_", "", selected_vars))) +
    theme_void() +
    theme(panel.background = element_blank(),
          plot.title = element_text(size = 16, face = "bold", hjust = 0.5),
          legend.position = "right",
          legend.key.size = unit(0.8, 'cm'),
          legend.text = element_text(size = 10),
          axis.text = element_blank(),
          axis.ticks = element_blank(),
          axis.line = element_blank())
  
  girafe(ggobj = plot, width_svg = 10, height_svg = 8)
}

#' Calculate composite scores for different models
#'
#' @param normalized_data Dataframe with normalized data
#' @param selected_vars Selected variables for composite scores
#' @param shp_data Shapefile data
#' @return List with model formulas and final data
composite_score_models <- function(normalized_data, selected_vars, shp_data) {
  print("Entering composite_score_models function")
  print("Normalized data structure:")
  print(str(normalized_data))
  print("Selected variables:")
  print(selected_vars)
  print("Shapefile data structure:")
  print(str(shp_data))
  
  # Get normalized column names for selected variables only
  norm_cols <- paste0("normalization_", tolower(selected_vars))
  norm_cols <- intersect(norm_cols, names(normalized_data))
  
  print("Normalized columns to be used:")
  print(norm_cols)
  
  if (length(norm_cols) < 2) {
    print("Error: At least two valid variables are required for composite score calculation.")
    return(NULL)
  }
  
  # Generate combinations
   # model_combinations <- list()
  if (length(norm_cols) == 2) {
    # If only two variables are selected, create just one model
    model_combinations <- list(norm_cols)
  } else {
    # For more than two variables, generate all combinations
    # for (i in 2:length(norm_cols)) {
    #   model_combinations <- c(model_combinations, combn(norm_cols, i, simplify = FALSE))
    # }
    
    # removed the for loop comptationally expensive 
    model_combinations <- do.call(c, lapply(2:length(norm_cols), function(i) combn(norm_cols, i, simplify = FALSE)))
    
    
  }
  
  # Calculate composite scores
  normalized_data <- as.data.table(normalized_data)
  shp_data <- as.data.table(shp_data)
  # edited by @laurette substituted to a data.table call 
  # more optimal 
  final_data <- merge(
    normalized_data[, .(WardName)],
    shp_data[, .(WardName, Urban)],
    by = "WardName",
    all.x = TRUE
  )
  
  
  
  # for (i in seq_along(model_combinations)) {
  #   model_name <- paste0("model_", i)
  #   vars <- model_combinations[[i]]
  #   
  #   # print(paste("Processing", model_name))
  #   # print("Variables used:")
  #   # print(vars)
  #   
  #   tryCatch({
  #     final_data <- final_data %>%
  #       mutate(!!sym(model_name) := {
  #         result <- rowSums(select(normalized_data, all_of(vars))) / length(vars)
  #         attributes(result) <- NULL # Strip attributes here
  #         print("Result summary:")
  #         print(summary(result))
  #         
  #         # if (model_name == "model_4") { # 
  #         #   print("Detailed result for model_4:")
  #         #   print(head(result, 10))
  #         # }
  #         
  #         # Flag if not urban and in top 5
  #         final_data[[paste0(model_name, "_flagged")]] <- 
  #           final_data$Urban == "No" & rank(result, na.last = "keep") <= 5 
  #         
  #         result
  #       })
  #   }, error = function(e) {
  #     print(paste("Error in composite_score_models for", model_name, ":", e$message))
  #     print("Data causing the error:")
  #     print(str(normalized_data))
  #     print("Variables causing the error:")
  #     print(vars)
  #   })
  # }
  

  for (i in seq_along(model_combinations)) {
    
    #commented out by @ laurette to replace with an optimized chunck
    
    vars <- model_combinations[[i]]
    result <- rowSums(normalized_data[, ..vars], na.rm = TRUE) / length(vars)
    set(final_data, j = paste0("model_", i), value = result)
    set(final_data, j = paste0("model_", i, "_flagged"),
        value = final_data$Urban == "No" & rank(result, na.last = "keep") <= 5)
  }
  
  
  
  # Prepare output
  if (ncol(final_data) <= 1) {
    print("Error: No valid models could be created.")
    return(NULL)
  }
  
  list(model_formula = model_combinations, 
       final_data = final_data)
}

#' Generate model formulas
#'
#' @param model_data Model data from composite_score_models
#' @return Dataframe with model formulas
models_formulas <- function(model_data) {
  model_formulas_data <- data.frame(model = character(), 
                                    variables = character(),
                                    stringsAsFactors = FALSE)
  
  for (index in seq_along(model_data)) {
    model_formula <- data.frame(model = paste0("model_", index), 
                                variables = paste(gsub("normalization_", "", model_data[[index]]), collapse = " + "),
                                stringsAsFactors = FALSE)
    
    model_formulas_data <- rbind(model_formulas_data, model_formula)
  }
  
  return(model_formulas_data)
}

#' Process model scores for plotting
#'
#' @param data_to_process Data with model scores
#' @return Processed data for plotting
# process_model_score <- function(data_to_process){
#   # Separate Urban column
#   urban_data <- data_to_process %>% select(WardName, Urban)
#   
#   # Melt the data without Urban column
#   melted_data <- data_to_process %>% 
#     select(WardName, starts_with("model_")) %>%  # Select model columns and WardName
#     reshape2::melt(id.vars = "WardName", variable.name = "variable", value.name = "value") 
#   
#   # Rejoin Urban data
#   plottingdata <- melted_data %>%
#     left_join(urban_data, by = "WardName") %>%
#     group_by(variable) %>% 
#     mutate(
#       new_value = (value - min(value)) / (max(value) - min(value)),
#       class = cut(new_value, seq(0, 1, 0.2), include.lowest = TRUE)
#     ) %>%
#     arrange(value) %>% 
#     mutate(
#       rank = row_number(),
#       wardname_rank = paste(WardName, "(",rank,")"),
#       flag_not_ideal = ifelse(Urban == "No" & rank <= 5, TRUE, FALSE)
#     )
#   
#   print("Plotting data summary:")
#   print(summary(plottingdata))
#   
#   plottingdata
# }

process_model_score <- function(data_to_process) {
  
  #commented out tidyverse by @ laurette to replace with an optimized chunck which uses data.table
  dt <- as.data.table(data_to_process)
  
  # Separate urban data
  urban_data <- dt[, .(WardName, Urban)]
  
  # Melt model columns only
  melted_data <- melt(
    dt,
    id.vars = "WardName",
    measure.vars = patterns("^model_"),
    variable.name = "variable",
    value.name = "value"
  )
  
  # Merge Urban info back
  melted_data <- merge(melted_data, urban_data, by = "WardName", all.x = TRUE)
  
  # Normalize, classify, rank
  melted_data[, `:=`(
    new_value = (value - min(value, na.rm = TRUE)) / (max(value, na.rm = TRUE) - min(value, na.rm = TRUE)),
    class = cut((value - min(value, na.rm = TRUE)) / (max(value, na.rm = TRUE) - min(value, na.rm = TRUE)),
                breaks = seq(0, 1, 0.2), include.lowest = TRUE)
  ), by = variable]
  
  # Rank + flag
  melted_data[, `:=`(
    rank = frank(value, ties.method = "first", na.last = "keep"),
    wardname_rank = paste0(WardName, " (", frank(value, ties.method = "first", na.last = "keep"), ")"),
    flag_not_ideal = Urban == "No" & frank(value, ties.method = "first", na.last = "keep") <= 5
  ), by = variable]
  
  message("Plotting data summary:")
  print(summary(melted_data))
  
  return(melted_data[])
}


#' Plot model score map
#'
#' @param shp_data Shapefile data
#' @param processed_csv Processed CSV data with model scores
#' @param model_formulas Model formulas from models_formulas function
#' @param maps_per_page Number of maps per page
#' @return List of Girafe objects with interactive maps
# plot_model_score_map <- function(shp_data, processed_csv, model_formulas, maps_per_page = 4) {
#   palette_func <- brewer.pal(5, "YlOrRd")
#   
#   # Create facet labels with line breaks and flag
#   facet_labels <- setNames(
#     sapply(seq_along(model_formulas$model), function(i) {
#       var_names <- strsplit(model_formulas$variables[i], " \\+ ")[[1]]
#       base_label <- paste(var_names, collapse = " +<br>")
#       
#       # Check if the model is flagged
#       if (any(processed_csv$flag_not_ideal[processed_csv$variable == model_formulas$model[i]])) {
#         base_label <- paste0(base_label, "<br><span style='color:red;'>(Not Ideal)</span>")
#       }
#       
#       base_label
#     }),
#     model_formulas$model
#   )
#   
#   # Calculate consistent plot height based on maps per page
#   plot_height <- 10 / ceiling(sqrt(maps_per_page)) 
#   
#   # Split the data into pages
#   total_models <- nrow(model_formulas)
#   pages <- ceiling(total_models / maps_per_page)
#   
#   plot_list <- list()
#   
#   for (page in 1:pages) {
#     start_index <- (page - 1) * maps_per_page + 1
#     end_index <- min(page * maps_per_page, total_models)
#     
#     current_models <- model_formulas$model[start_index:end_index]
#     current_data <- processed_csv %>% filter(variable %in% current_models)
#     
#     plot <- ggplot(data = shp_data) +
#       geom_sf_interactive(color = "black", fill = "white") +
#       geom_sf_interactive(data = current_data, 
#                           aes(geometry = geometry, fill = class, tooltip = wardname_rank)) +
#       # Add a new layer for flagged wards
#       geom_sf_interactive(data = current_data %>% filter(flag_not_ideal), 
#                           aes(geometry = geometry), 
#                           fill = NA, color = "blue", size = 1) + # Add blue border
#       facet_wrap(~variable, ncol = 2, labeller = labeller(variable = facet_labels)) +
#       scale_fill_discrete(drop=FALSE, name="Malaria Risk Score", type = palette_func,
#                           labels = c("Very Low", "Low", "Medium", "High", "Very High")) +
#       labs(subtitle=paste("Page", page, "of", pages), 
#            title = 'Composite Score Distribution by Model', 
#            fill = "Malaria Risk Score",
#            caption = "Blue outline indicates non-urban wards ranked in top 5 for reprioritization (not ideal)") +
#       theme_void() +
#       theme(
#         strip.text = element_markdown(size = 7, face = "bold", lineheight = 1.0),
#         strip.background = element_blank(), 
#         legend.position = "bottom",
#         legend.title = element_text(size = 6, face = "bold"),
#         legend.text = element_text(size = 6),
#         plot.title = element_text(size = 6, face = "bold", hjust = 0.5),
#         plot.subtitle = element_text(size = 10, hjust = 0.5),
#         # Control spacing to maintain consistent plot sizes
#         panel.spacing = unit(1.5, "lines"), 
#         plot.caption = element_text(size = 8, hjust = 0.5)
#       )
#     
#     # Use girafe for interactivity with fixed plot height
#     plot_list[[page]] <- girafe(ggobj = plot, height_svg = plot_height) 
#   }
#   
#   return(plot_list)
# }

plot_model_score_map <- function(shp_data, processed_csv, model_formulas, maps_per_page = 4) {
  palette_func <- brewer.pal(5, "YlOrRd")
  
  # Pre-calculate facet labels once
  facet_labels <- setNames(
    vapply(seq_len(nrow(model_formulas)), function(i) {
      var_names <- strsplit(model_formulas$variables[i], " \\+ ")[[1]]
      label <- paste(var_names, collapse = " +<br>")
      if (any(processed_csv$flag_not_ideal[processed_csv$variable == model_formulas$model[i]])) {
        paste0(label, "<br><span style='color:red;'>(Not Ideal)</span>")
      } else {
        label
      }
    }, character(1)),
    model_formulas$model
  )
  
  # Determine layout
  total_models <- nrow(model_formulas)
  pages <- ceiling(total_models / maps_per_page)
  plot_height <- 10 / ceiling(sqrt(maps_per_page)) 
  
  plot_list <- vector("list", pages)
  
  # Convert shp_data only once to avoid redundant calls
  base_plot <- ggplot(data = shp_data) +
    geom_sf_interactive(color = "black", fill = "white")
  
  for (page in seq_len(pages)) {
    idx <- ((page - 1) * maps_per_page + 1):min(page * maps_per_page, total_models)
    current_models <- model_formulas$model[idx]
    current_data <- processed_csv[processed_csv$variable %in% current_models, , drop = FALSE]
    
    plot <- base_plot +
      geom_sf_interactive(
        data = current_data,
        aes(geometry = geometry, fill = class, tooltip = wardname_rank)
      ) +
      geom_sf_interactive(
        data = current_data[current_data$flag_not_ideal, , drop = FALSE],
        aes(geometry = geometry),
        fill = NA, color = "blue", size = 1
      ) +
      facet_wrap(~variable, ncol = 2, labeller = labeller(variable = facet_labels)) +
      scale_fill_discrete(
        drop = FALSE, name = "Malaria Risk Score", type = palette_func,
        labels = c("Very Low", "Low", "Medium", "High", "Very High")
      ) +
      labs(
        subtitle = paste("Page", page, "of", pages),
        title = 'Composite Score Distribution by Model',
        fill = "Malaria Risk Score",
        caption = "Blue outline = non-urban wards in top 5 (not ideal)"
      ) +
      theme_void() +
      theme(
        strip.text = element_markdown(size = 7, face = "bold"),
        strip.background = element_blank(),
        legend.position = "bottom",
        legend.title = element_text(size = 6, face = "bold"),
        legend.text = element_text(size = 6),
        plot.title = element_text(size = 6, face = "bold", hjust = 0.5),
        plot.subtitle = element_text(size = 10, hjust = 0.5),
        panel.spacing = unit(1.5, "lines"),
        plot.caption = element_text(size = 8, hjust = 0.5)
      )
    
    plot_list[[page]] <- girafe(ggobj = plot, height_svg = plot_height)
  }
  
  plot_list
}



#' Create box plot of ward rankings with pagination support
#'
#' @param plottingdata Plotting data from process_model_score
#' @param wards_per_page Number of wards to display per page (default 20)
#' @return List with plotly object, ward rankings, and pagination info
box_plot_function <- function(plottingdata, wards_per_page = 20) {
  df_long <- plottingdata %>%
    select(WardName, variable, rank)
  
  # Calculate ward rankings
  ward_rankings <- df_long %>%
    group_by(WardName) %>%
    summarize(median_rank = median(rank)) %>%
    arrange(median_rank) %>%
    mutate(overall_rank = row_number())
  
  # Join with original data
  df_long <- df_long %>%
    left_join(ward_rankings, by = "WardName")
  
  # Reorder WardName by overall_rank
  df_long$WardName <- factor(df_long$WardName, levels = ward_rankings$WardName)
  
  # Calculate number of pages needed
  total_wards <- length(unique(df_long$WardName))
  total_pages <- ceiling(total_wards / wards_per_page)
  
  # Add a cleaner display name for each ward
  # This trims long prefixes like NISNAS01_ etc.
  ward_rankings <- ward_rankings %>%
    mutate(
      DisplayName = sapply(WardName, function(w) {
        # Remove prefix pattern (e.g., NISNAS01_)
        clean_name <- gsub("^([A-Z]+[0-9]+_)", "", w)
        # Truncate extremely long names
        if(nchar(clean_name) > 25) {
          return(paste0(substr(clean_name, 1, 22), "..."))
        } else {
          return(clean_name)
        }
      })
    )
  
  # Add display names to the plotting data
  display_map <- setNames(ward_rankings$DisplayName, as.character(ward_rankings$WardName))
  df_long$DisplayName <- display_map[as.character(df_long$WardName)]
  
  # Function to create a plot for a specific page
  create_page_plot <- function(page_num, df_long, ward_rankings, wards_per_page) {
    # Calculate which wards to include
    start_idx <- (page_num - 1) * wards_per_page + 1
    end_idx <- min(page_num * wards_per_page, nrow(ward_rankings))
    
    # Get the wards for this page
    page_wards <- ward_rankings$WardName[start_idx:end_idx]
    
    # Filter data for these wards
    page_data <- df_long %>%
      filter(WardName %in% page_wards)
    
    # For plotting, use the display names
    display_names <- ward_rankings$DisplayName[start_idx:end_idx]
    display_name_map <- setNames(display_names, as.character(page_wards))
    
    # Create the plot
    p <- ggplot(page_data, aes(x = factor(WardName, levels = page_wards), y = rank)) +
      geom_boxplot(fill = "#69b3a2", color = "#3c5e8b", alpha = 0.7) +
      coord_flip() +
      labs(
        title = paste0("Ward Rankings Distribution (Page ", page_num, " of ", total_pages, ")"),
        subtitle = paste("Showing wards ranked", start_idx, "to", end_idx, "of", total_wards),
        x = "", 
        y = "Rank"
      ) +
      # Replace ward names with display names for the axis
      scale_x_discrete(labels = function(x) display_name_map[x]) +
      theme_minimal() +
      theme(
        axis.text.y = element_text(size = 11),
        axis.text.x = element_text(size = 10, angle = 45, hjust = 1),
        plot.title = element_text(size = 14, hjust = 0.5, face = "bold"),
        plot.subtitle = element_text(size = 12, hjust = 0.5, color = "gray50")
      )
    
    plot <- ggplotly(p, height = min(750, 300 + 20 * length(page_wards))) %>%
      layout(
        yaxis = list(fixedrange = FALSE),
        xaxis = list(fixedrange = TRUE)
      ) %>%
      config(scrollZoom = TRUE)
    
    return(plot)
  }
  
  # Create the plot for the first page
  first_page_plot <- create_page_plot(1, df_long, ward_rankings, wards_per_page)
  
  # Return all necessary data
  return(list(
    plot = first_page_plot,
    ward_rankings = ward_rankings,
    pagination = list(
      total_pages = total_pages,
      wards_per_page = wards_per_page,
      total_wards = total_wards,
      create_page_plot = create_page_plot  # Function to create plots for other pages
    )
  ))
}


# ==============================================================================
# DECISION TREE VISUALIZATION
# ==============================================================================

#' Create decision tree visualization
#'
#' @param all_variables All variables in the dataset
#' @param selected_variables Selected variables for the model
#' @param excluded_variables Excluded variables
#' @param progress Progress information for tree steps
#' @param top_5_wards Top 5 wards by vulnerability ranking
#' @return grViz object with decision tree
decision_tree_function <- function(all_variables, selected_variables, excluded_variables, progress, top_5_wards = character(0)) {
  # Format variables lists with bullets
  format_var_list <- function(vars) {
    if (length(vars) == 0) return("None")
    paste(sapply(vars, function(x) paste0("• ", x)), collapse = "\\n")
  }
  
  # Format top 5 wards
  formatted_top_5 <- if (length(top_5_wards) > 0) {
    paste("1.", top_5_wards[1], "\\n2.", top_5_wards[2], "\\n3.", 
          top_5_wards[3], "\\n4.", top_5_wards[4], "\\n5.", top_5_wards[5])
  } else {
    "No wards available"
  }
  
  # Define precise professional color scheme
  colors <- list(
    navy = "#1B2631",       # Darker navy for headers
    orange = "#E67E22",     # Bright orange for diamond
    teal = "#16A596",       # Bright teal for included
    gray = "#7F8C8D",       # Medium gray for excluded
    green = "#27AE60",      # Bright green for process
    blue = "#2980B9",       # Bright blue for maps
    purple = "#8E44AD",     # Bright purple for final nodes
    arrow = "#34495E"       # Dark arrow color
  )
  
  graph_string <- sprintf('
  digraph G {
    graph [rankdir=LR,
           nodesep=0.6,
           ranksep=0.8,
           splines=ortho,
           pad=0.5,
           compound=true]
    
    node [shape=rect,
          style="filled,rounded",
          fontname="Arial",
          fontsize=11,
          margin="0.2,0.2",
          penwidth=1.2]
    
    edge [color="%s",
          penwidth=1.2,
          arrowsize=0.9,
          arrowhead=vee]
    // Initial nodes
    start [label="Malaria Risk Assessment\\nVariable Selection"
           fillcolor="%s"
           fontcolor="white"]
    vars [label="Variables:\\n%s"
          fillcolor="%s"
          fontcolor="white"]
    // Evaluation diamond
    eval [label="Variable\\nEvaluation"
          shape=diamond
          fillcolor="%s"
          fontcolor="white"
          width=1.5
          height=1.5]
    // Variable groups
    included [label="Included Variables:\\n%s"
             fillcolor="%s"
             fontcolor="white"]
    excluded [label="Excluded Variables:\\n%s"
             fillcolor="%s"
             fontcolor="white"]
    // Processing and results
    process [label="Data Normalization &\\nComposite Score Calculation"
            fillcolor="%s"
            fontcolor="white"]
    maps [label="Generated Risk Maps\\nfor All Combinations"
          fillcolor="%s"
          fontcolor="white"]
    recommended [label="Recommended Risk Map\\nby the Box and Whisker Plot"
                fillcolor="%s"
                fontcolor="white"]
    priority [label="Top 5 Wards\\nfor Reprioritization:\\n%s"
             fillcolor="%s"
             fontcolor="white"]
    // Edge definitions with improved spacing
    start -> vars [weight=2]
    vars -> eval [weight=2]
    eval -> included [label=" Include"]
    eval -> excluded [label=" Exclude"]
    included -> process [weight=2]
    process -> maps [weight=2]
    maps -> recommended [weight=2]
    recommended -> priority [weight=2]
    // Rank definitions for better alignment
    {rank=same; start; vars}
    {rank=same; included; excluded}
    {rank=same; process; maps}
    {rank=same; recommended; priority}
    // Add invisible edges for better spacing
    included -> excluded [style=invis]
    maps -> recommended [style=invis]
  }',
                          colors$arrow,
                          colors$navy, format_var_list(all_variables), colors$navy,
                          colors$orange,
                          format_var_list(selected_variables), colors$teal,
                          format_var_list(excluded_variables), colors$gray,
                          colors$green, colors$blue,
                          colors$purple, formatted_top_5, colors$purple
  )
  
  grViz(graph_string)
}

# ==============================================================================
# GRID AND SPATIAL ANALYSIS
# ==============================================================================

#' Create grid for a ward
#'
#' @param polygon Polygon to subdivide into a grid
#' @param cell_size Cell size in coordinate units
#' @return SF object with grid cells
subdivide_polygon <- function(polygon, cell_size = 500) {
  # Extract the geometry
  polygon_sf <- st_geometry(polygon)
  
  # Get bounding box
  bounds <- st_bbox(polygon_sf)
  
  # Calculate number of cells based on cell_size
  x_span <- bounds["xmax"] - bounds["xmin"]
  y_span <- bounds["ymax"] - bounds["ymin"]
  
  # Calculate number of columns and rows
  cols <- ceiling(x_span / cell_size)
  rows <- ceiling(y_span / cell_size)
  
  # Ensure we have at least 2 rows and columns
  cols <- max(2, cols)
  rows <- max(2, rows)
  
  # Create grid
  x_range <- seq(bounds["xmin"], bounds["xmax"], length.out = cols + 1)
  y_range <- seq(bounds["ymin"], bounds["ymax"], length.out = rows + 1)
  
  # Create sub-polygons
  sub_polygons <- list()
  for (i in 1:cols) {
    for (j in 1:rows) {
      sub_poly <- st_polygon(list(rbind(
        c(x_range[i], y_range[j]),
        c(x_range[i + 1], y_range[j]),
        c(x_range[i + 1], y_range[j + 1]),
        c(x_range[i], y_range[j + 1]),
        c(x_range[i], y_range[j])
      )))
      sub_polygons <- append(sub_polygons, list(sub_poly))
    }
  }
  
  # Convert to sf collection
  grid_sfc <- st_sfc(sub_polygons, crs = st_crs(polygon_sf))
  
  # Create a grid sf object
  grid_sf <- st_sf(geometry = grid_sfc)
  
  # Add cell IDs
  grid_sf$cell_id <- 1:nrow(grid_sf)
  
  # Handle case where polygon has multiple features
  if (length(polygon_sf) > 1) {
    # Convert multiple polygons to a single multipolygon
    combined_poly <- st_union(polygon_sf)
    # Create a single-row sf object
    single_poly_sf <- st_sf(geometry = st_sfc(combined_poly, crs = st_crs(polygon_sf)))
    
    # Intersect the grid with this single polygon
    grid_intersection <- st_intersection(grid_sf, single_poly_sf)
  } else {
    # For single polygon case
    single_poly_sf <- st_sf(geometry = polygon_sf)
    grid_intersection <- st_intersection(grid_sf, single_poly_sf)
  }
  
  # Create final grid sf with attributes
  # Ensure each grid cell gets a unique ID
  final_grid <- grid_intersection %>%
    mutate(
      WardName = polygon$WardName[1],  # Just take the first one if multiple
      GridID = row_number(),
      CellSize = cell_size,  # Store the cell size for reference
      Settlement = ifelse("Urban" %in% names(polygon), polygon$Urban[1], "Unknown")
    )
  
  return(final_grid)
}

#' Create ward grid from shapefile data
#'
#' @param ward_name Ward name
#' @param shapefile_data Shapefile data
#' @param cell_size Cell size in coordinate units
#' @return SF object with grid cells
create_ward_grid <- function(ward_name, shapefile_data, cell_size = 500) {
  # Filter the shapefile to get just the selected ward
  ward_shape <- shapefile_data %>% filter(WardName == ward_name)
  
  if(nrow(ward_shape) == 0) {
    return(NULL)
  }
  
  # Create the grid
  tryCatch({
    grid <- subdivide_polygon(ward_shape, cell_size)
    return(grid)
  }, error = function(e) {
    message("Error creating grid: ", e$message)
    return(NULL)
  })
}

#' Create HTML for grid cell classification popup
#'
#' @param ward_name Ward name
#' @param grid_id Grid ID
#' @param current_class Current classification
#' @return HTML string for classification popup
create_classification_popup <- function(ward_name, grid_id, current_class = "Unclassified") {
  # Create a unique ID for the popup form
  popup_id <- paste0("popup_", ward_name, "_", grid_id)
  
  # Create the simplified classification checklist HTML
  html <- paste0(
    "<div class='classification-checklist' style='min-width: 300px; max-width: 400px;'>",
    "<h4 style='margin-top: 0; border-bottom: 1px solid #ddd; padding-bottom: 8px;'>Grid Cell Classification</h4>",
    
    "<div id='checklist-", popup_id, "' style='margin-bottom: 15px;'>",
    "<p style='font-weight: bold; margin-bottom: 5px;'>Select the most appropriate classification for this area:</p>",
    
    "<div class='classification-options' style='margin-bottom: 15px;'>",
    "<div style='margin-bottom: 10px;'>",
    "<label style='display: block; font-weight: bold; cursor: pointer;'>",
    "<input type='radio' name='classification' value='Formal'> <span style='color: #0074D9;'>Formal Settlement</span>",
    "</label>",
    "<ul style='margin-top: 5px; margin-bottom: 10px; padding-left: 20px; color: #666;'>",
    "<li>Planned layout with good road access</li>",
    "<li>Regular building patterns</li>",
    "<li>Typically with infrastructure</li>",
    "</ul>",
    "</div>",
    
    "<div style='margin-bottom: 10px;'>",
    "<label style='display: block; font-weight: bold; cursor: pointer;'>",
    "<input type='radio' name='classification' value='Informal'> <span style='color: #FF4136;'>Informal Settlement</span>",
    "</label>",
    "<ul style='margin-top: 5px; margin-bottom: 10px; padding-left: 20px; color: #666;'>",
    "<li>Unplanned layout with limited road access</li>",
    "<li>Irregular building patterns</li>",
    "<li>High density or overcrowded housing</li>",
    "</ul>",
    "</div>",
    
    "<div style='margin-bottom: 10px;'>",
    "<label style='display: block; font-weight: bold; cursor: pointer;'>",
    "<input type='radio' name='classification' value='No Buildings/Avoid Area'> <span style='color: #2ECC40;'>No Buildings/Avoid Area</span>",
    "</label>",
    "<ul style='margin-top: 5px; margin-bottom: 10px; padding-left: 20px; color: #666;'>",
    "<li>Water bodies, forests, or vegetation</li>",
    "<li>Industrial or hazardous areas</li>",
    "<li>Areas without residential structures</li>",
    "</ul>",
    "</div>",
    "</div>",
    
    "<div id='classification-result-", popup_id, "' style='margin-bottom: 15px; padding: 10px; background-color: #f5f5f5; border-radius: 5px;'>",
    "<h5 style='margin-top: 0; margin-bottom: 5px;'>Current Classification:</h5>",
    "<p style='font-weight: bold; margin: 0;'>", current_class, "</p>",
    "</div>",
    
    "<div style='display: flex; justify-content: space-between;'>",
    "<button type='button' class='btn btn-sm btn-primary save-classification' onclick='window.saveClassification(\"", 
    ward_name, "\", ", grid_id, ", document.querySelector(\"input[name=classification]:checked\").value)' style='width: 100%;'>",
    "SAVE CLASSIFICATION</button>",
    "</div>",
    
    "<script>",
    "  // Set the current classification if it exists",
    "  if ('", current_class, "' !== 'Unclassified') {",
    "    document.querySelector('input[name=classification][value=\"", current_class, "\"]').checked = true;",
    "  }",
    "</script>",
    "</div>"
  )
  
  return(html)
}

#' Process and view shapefile and CSV data with grid enhancements
#'
#' @param ward_name Ward name
#' @param shp_data Shapefile data
#' @param grid_annotations Grid annotations
#' @param enable_grid Whether to enable grid
#' @param grid_cell_size Grid cell size
#' @return Leaflet map with grid
# Modified function in functions.R
process_and_view_shapefile_and_csv_enhanced <- function(ward_name, shp_data, grid_annotations = NULL, 
                                                        enable_grid = TRUE, grid_cell_size = 500) {
  # Filter the main shapefile to get just the selected ward
  ward_shape <- shp_data %>% filter(WardName == ward_name)
  
  if (nrow(ward_shape) == 0) {
    return(
      leaflet() %>%
        addTiles() %>%
        addControl(
          html = paste("<div style='padding: 15px; background: white; border-radius: 5px;'>",
                       "<h4>Ward not found</h4>",
                       "<p>The selected ward '", ward_name, "' was not found in the shapefile.</p>",
                       "</div>"),
          position = "topright"
        )
    )
  }
  
  # Transform shapefile to WGS84 for leaflet
  shapefile_wgs84 <- st_transform(ward_shape, crs = 4326)
  
  # Create a Leaflet map with satellite imagery
  map <- leaflet() %>%
    addProviderTiles(providers$Esri.WorldImagery, group = "Satellite") %>%
    addProviderTiles(providers$OpenStreetMap, group = "OpenStreetMap") %>%
    addProviderTiles(providers$CartoDB.Positron, group = "CartoDB Light") %>%
    fitBounds(lng1 = st_bbox(shapefile_wgs84)[[1]],
              lat1 = st_bbox(shapefile_wgs84)[[2]],
              lng2 = st_bbox(shapefile_wgs84)[[3]],
              lat2 = st_bbox(shapefile_wgs84)[[4]])
  
  # Add layer control
  map <- map %>%
    addLayersControl(
      baseGroups = c("Satellite", "OpenStreetMap", "CartoDB Light"),
      options = layersControlOptions(collapsed = FALSE)
    )
  
  # Add ward boundary (outline only)
  map <- map %>%
    addPolygons(data = shapefile_wgs84,
                color = "yellow",       # Boundary color
                weight = 3,             # Thicker line for visibility
                opacity = 1.0,          # Fully opaque line
                fillOpacity = 0.0,      # Completely transparent fill (no shading)
                label = ward_name)
  
  # Create grid if enabled
  if (enable_grid) {
    # Create grid for the ward
    grid_sf <- tryCatch({
      create_ward_grid(ward_name, shp_data, grid_cell_size)
    }, error = function(e) {
      message("Error creating grid: ", e$message)
      NULL
    })
    
    if (!is.null(grid_sf)) {
      # Transform to WGS84
      grid_wgs84 <- st_transform(grid_sf, crs = 4326)
      
      # Add Classification column with default value
      grid_wgs84$Classification <- "Unclassified"
      
      # Update classifications from annotations if they exist for this ward
      if (!is.null(grid_annotations) && nrow(grid_annotations) > 0) {
        ward_annotations <- grid_annotations[grid_annotations$WardName == ward_name, ]
        if (nrow(ward_annotations) > 0) {
          for (i in 1:nrow(ward_annotations)) {
            grid_idx <- which(grid_wgs84$GridID == ward_annotations$GridID[i])
            if (length(grid_idx) > 0) {
              grid_wgs84$Classification[grid_idx] <- ward_annotations$Classification[i]
            }
          }
        }
      }
      
      # Create color palette for classifications with appropriate transparency
      classification_colors <- c(
        "Formal" = "#0074D9",           # Blue
        "Informal" = "#FF4136",         # Red
        "No Buildings/Avoid Area" = "#2ECC40",  # Green
        "Unclassified" = "#AAAAAA"      # Gray
      )
      
      # UPDATED: Opacity settings for better visibility
      classified_opacity <- 0.65      # More transparent to see the satellite imagery
      unclassified_opacity <- 0.1     # Very transparent for unclassified cells
      
      # Split grid data for better visualization
      unclassified_cells <- grid_wgs84[grid_wgs84$Classification == "Unclassified", ]
      formal_cells <- grid_wgs84[grid_wgs84$Classification == "Formal", ]
      informal_cells <- grid_wgs84[grid_wgs84$Classification == "Informal", ]
      avoid_cells <- grid_wgs84[grid_wgs84$Classification == "No Buildings/Avoid Area", ]
      
      # Add unclassified grid cells first (with just borders, minimal fill)
      if (nrow(unclassified_cells) > 0) {
        map <- map %>%
          addPolygons(data = unclassified_cells,
                      color = "white",
                      weight = 1,
                      opacity = 0.8,
                      fillColor = classification_colors["Unclassified"],
                      fillOpacity = unclassified_opacity,
                      layerId = ~paste(WardName, GridID, "unclassified", sep = "_"),
                      label = ~paste("Grid ID:", GridID, "- Unclassified"),
                      popup = ~create_classification_popup(WardName, GridID, Classification),
                      group = "UnclassifiedGrid")
      }
      
      # Add formal cells with blue color
      if (nrow(formal_cells) > 0) {
        map <- map %>%
          addPolygons(data = formal_cells,
                      color = "white",
                      weight = 2, 
                      opacity = 0.9,
                      fillColor = classification_colors["Formal"],
                      fillOpacity = classified_opacity,
                      layerId = ~paste(WardName, GridID, "formal", sep = "_"),
                      label = ~paste("Grid ID:", GridID, "- Formal Settlement"),
                      popup = ~create_classification_popup(WardName, GridID, Classification),
                      group = "ClassifiedGrid_Formal")
      }
      
      # Add informal cells with red color
      if (nrow(informal_cells) > 0) {
        map <- map %>%
          addPolygons(data = informal_cells,
                      color = "white",
                      weight = 2,
                      opacity = 0.9,
                      fillColor = classification_colors["Informal"],
                      fillOpacity = classified_opacity,
                      layerId = ~paste(WardName, GridID, "informal", sep = "_"),
                      label = ~paste("Grid ID:", GridID, "- Informal Settlement"),
                      popup = ~create_classification_popup(WardName, GridID, Classification),
                      group = "ClassifiedGrid_Informal")
      }
      
      # Add avoid cells with green color
      if (nrow(avoid_cells) > 0) {
        map <- map %>%
          addPolygons(data = avoid_cells,
                      color = "white",
                      weight = 2,
                      opacity = 0.9,
                      fillColor = classification_colors["No Buildings/Avoid Area"],
                      fillOpacity = classified_opacity,
                      layerId = ~paste(WardName, GridID, "avoid", sep = "_"),
                      label = ~paste("Grid ID:", GridID, "- No Buildings/Avoid Area"),
                      popup = ~create_classification_popup(WardName, GridID, Classification),
                      group = "ClassifiedGrid_Avoid")
      }
      
      # Add classification legend with proper colors
      map <- map %>%
        addLegend(
          position = "bottomright",
          colors = unname(classification_colors),
          labels = names(classification_colors),
          title = "Grid Classification",
          opacity = 0.9
        )
    }
  }
  
  # Add JavaScript functions for handling classification from popup
  map <- map %>% 
    htmlwidgets::onRender(paste0("
      function(el, x) {
        // Define the classification functions globally so they can be called from popups
        window.saveClassification = function(wardName, gridId, classification) {
          console.log('Saving classification:', {
            wardName,
            gridId,
            classification
          });
          
          // Send data to Shiny
          Shiny.setInputValue('classify_grid', {
            wardName: wardName,
            gridId: gridId,
            classification: classification,
            timestamp: new Date().toISOString(),
            method: 'manual'
          });
          
          // Show confirmation message
          const popup = document.querySelector('.leaflet-popup-content');
          if (popup) {
            const confirmationMsg = document.createElement('div');
            confirmationMsg.style.marginTop = '10px';
            confirmationMsg.style.padding = '5px';
            confirmationMsg.style.backgroundColor = '#dff0d8';
            confirmationMsg.style.borderRadius = '3px';
            confirmationMsg.style.color = '#3c763d';
            confirmationMsg.style.textAlign = 'center';
            confirmationMsg.innerHTML = 'Classification saved successfully!';
            popup.appendChild(confirmationMsg);
            
            // Remove message after a short delay
            setTimeout(function() {
              try {
                popup.removeChild(confirmationMsg);
                // Close the popup after saving
                setTimeout(() => {
                  document.querySelector('.leaflet-popup-close-button').click();
                }, 500);
              } catch (e) {
                console.log('Error removing confirmation message', e);
              }
            }, 1500);
          }
        };
      }
    "))
  
  return(map)
}

# New function to create a map for downloading
create_downloadable_map <- function(ward_name, shp_data, grid_annotations, 
                                    enable_grid = TRUE, grid_cell_size = 500) {
  # Create a modified version of the map for downloading
  map <- process_and_view_shapefile_and_csv_enhanced(ward_name, shp_data, grid_annotations, 
                                                     enable_grid, grid_cell_size)
  
  # Modify for downloading - remove interactive elements
  map <- map %>%
    htmlwidgets::onRender("
      function(el, x) {
        // Remove controls for download
        $('.leaflet-control-container').hide();
        
        // Add title
        const title = document.createElement('div');
        title.style.position = 'absolute';
        title.style.top = '10px';
        title.style.left = '10px';
        title.style.zIndex = '1000';
        title.style.background = 'white';
        title.style.padding = '5px 10px';
        title.style.borderRadius = '4px';
        title.style.fontWeight = 'bold';
        title.innerHTML = 'Ward Classification Map: " + ward_name + "';
        el.appendChild(title);
        
        // Add legend explanation
        const legend = document.createElement('div');
        legend.style.position = 'absolute';
        legend.style.bottom = '30px';
        legend.style.right = '10px';
        legend.style.zIndex = '1000';
        legend.style.background = 'rgba(255,255,255,0.9)';
        legend.style.padding = '8px';
        legend.style.borderRadius = '4px';
        legend.style.maxWidth = '300px';
        legend.innerHTML = '<strong>Classification Legend</strong><br>' +
                          '<span style=\"color:#0074D9;\">■</span> Formal Settlement<br>' +
                          '<span style=\"color:#FF4136;\">■</span> Informal Settlement<br>' +
                          '<span style=\"color:#2ECC40;\">■</span> No Buildings/Avoid Area<br>' +
                          '<span style=\"color:#AAAAAA;\">■</span> Unclassified';
        el.appendChild(legend);
      }
    ")
  
  return(map)
}

# Add a helper function for retrieving classification colors
get_classification_color <- function(classification) {
  colors <- c(
    "Formal" = "#0074D9",           # Blue
    "Informal" = "#FF4136",         # Red
    "No Buildings/Avoid Area" = "#2ECC40",  # Green
    "Unclassified" = "#AAAAAA"      # Gray
  )
  
  if (classification %in% names(colors)) {
    return(colors[classification])
  } else {
    return("#AAAAAA")  # Default gray for unclassified or unknown
  }
}

# ==============================================================================
# URBAN EXTENT ANALYSIS
# ==============================================================================

#' Filter shapefile data based on urban extent threshold
#'
#' @param shp_data Shapefile data
#' @param threshold Urban extent threshold
#' @return Shapefile data with added urban extent information
filter_by_urban_extent <- function(shp_data, threshold = 30) {
  # Ensure Urban column exists and is numeric
  if (!"UrbanPercent" %in% names(shp_data)) {
    if ("Urban" %in% names(shp_data)) {
      # If Urban is binary (Yes/No), convert to percentage (100/0)
      shp_data$UrbanPercent <- ifelse(shp_data$Urban == "Yes", 100, 0)
    } else {
      # Create random data for demonstration if needed
      shp_data$UrbanPercent <- runif(nrow(shp_data), min = 0, max = 100)
      warning("No Urban or UrbanPercent column found in shapefile data, using random values for demonstration")
    }
  }
  
  # Convert to numeric if it's not already
  if (!is.numeric(shp_data$UrbanPercent)) {
    shp_data$UrbanPercent <- as.numeric(as.character(shp_data$UrbanPercent))
  }
  
  # Handle NA values by setting to 0
  shp_data$UrbanPercent[is.na(shp_data$UrbanPercent)] <- 0
  
  # Add a column to indicate if the ward meets the threshold
  shp_data$MeetsThreshold <- shp_data$UrbanPercent >= threshold
  
  return(shp_data)
}

# ==============================================================================
# POPULATION ESTIMATION
# ==============================================================================

#' Estimate ward population
#'
#' @param ward_name Ward name
#' @param grid_annotations Grid annotations
#' @param shp_data Shapefile data
#' @param gridded_wards Gridded wards data
#' @return List with population estimates
# Update the ward population estimation function to use ITN data when available
estimate_ward_population <- function(ward_name, grid_annotations, shp_data, gridded_wards = NULL) {
  # Extract the state code from the shapefile if available
  state_code <- NULL
  state_name <- NULL
  
  if ("StateCode" %in% names(shp_data)) {
    # Get the state code for this ward
    ward_data <- shp_data %>% filter(WardName == ward_name)
    if (nrow(ward_data) > 0) {
      state_code <- ward_data$StateCode[1]
      # Map the code to full state name
      state_name <- map_state_code(state_code)
    }
  }
  
  # Try to get ITN distribution data if state name is available
  actual_population <- NULL
  
  if (!is.null(state_name)) {
    itn_data <- load_itn_population(state_name)
    
    if (!is.null(itn_data)) {
      # Try exact match first
      ward_match <- itn_data %>% filter(Ward == ward_name)
      
      # If no exact match, try case-insensitive match
      if (nrow(ward_match) == 0) {
        ward_match <- itn_data %>% filter(tolower(Ward) == tolower(ward_name))
      }
      
      if (nrow(ward_match) > 0) {
        actual_population <- ward_match$Population[1]
      }
    }
  }
  
  # Get ward shape for area calculation
  ward_shape <- shp_data %>% filter(WardName == ward_name)
  
  if (nrow(ward_shape) == 0) {
    return(NULL)
  }
  
  # Calculate ward area in sq km
  ward_area <- st_area(ward_shape) %>% 
    units::set_units(km^2) %>% 
    as.numeric()
  
  # If we have actual population data from ITN distribution
  if (!is.null(actual_population) && !is.na(actual_population) && actual_population > 0) {
    return(list(
      summary = data.frame(
        WardName = ward_name,
        TotalArea_sqkm = ward_area,
        TotalPopulation = actual_population,
        PopulationSource = "ITN_Distribution_Data",
        PopulationDensity = actual_population / ward_area
      ),
      classification = data.frame(
        Classification = "Actual",
        Area_sqkm = ward_area,
        EstimatedPopulation = actual_population,
        DensityEstimate = actual_population / ward_area
      )
    ))
  }
  
  # If no actual data is available, use estimation based on ward attributes and grid data
  # Default density estimates by classification type (people per sq km)
  density_estimates <- list(
    "Formal" = 5000,
    "Informal" = 8000,
    "Slum" = 12000,
    "No Buildings/Avoid Area" = 0,
    "Unclassified" = 2500  # Default for unclassified areas
  )
  
  # Get ward urban status
  is_urban <- FALSE
  if ("Urban" %in% names(ward_shape)) {
    is_urban <- ward_shape$Urban[1] %in% c("Yes", "YES", "yes", "Y", "y")
  } else if ("UrbanPercent" %in% names(ward_shape)) {
    is_urban <- ward_shape$UrbanPercent[1] > 30  # Using 30% threshold
  }
  
  # Base density on urban status
  base_density <- if (is_urban) 5000 else 500
  
  # Get grid annotations for the ward
  if (!is.null(grid_annotations) && nrow(grid_annotations) > 0) {
    ward_annotations <- grid_annotations %>%
      filter(WardName == ward_name)
  } else {
    ward_annotations <- data.frame()
  }
  
  # Check for gridded wards
  if (is.null(gridded_wards)) {
    # If no grid, use simple ward-level estimate
    total_population <- ward_area * base_density
    
    return(list(
      summary = data.frame(
        WardName = ward_name,
        TotalArea_sqkm = ward_area,
        TotalPopulation = round(total_population),
        PopulationSource = "Density_Estimate",
        PopulationDensity = base_density
      ),
      classification = data.frame(
        Classification = if (is_urban) "Urban" else "Rural",
        Area_sqkm = ward_area,
        EstimatedPopulation = round(total_population),
        DensityEstimate = base_density
      )
    ))
  }
  
  # Filter gridded ward to current ward
  gridded_ward <- gridded_wards[gridded_wards$WardName == ward_name,]
  
  # Make sure gridded_ward is not empty
  if (nrow(gridded_ward) == 0) {
    # If filtered data is empty, use simple ward-level estimate
    total_population <- ward_area * base_density
    
    return(list(
      summary = data.frame(
        WardName = ward_name,
        TotalArea_sqkm = ward_area,
        TotalPopulation = round(total_population),
        PopulationSource = "Density_Estimate",
        PopulationDensity = base_density
      ),
      classification = data.frame(
        Classification = if (is_urban) "Urban" else "Rural",
        Area_sqkm = ward_area,
        EstimatedPopulation = round(total_population),
        DensityEstimate = base_density
      )
    ))
  }
  
  # Add classification to all grid cells
  gridded_ward$Classification <- "Unclassified"
  
  # Update classifications from annotations
  if (nrow(ward_annotations) > 0) {
    for (i in 1:nrow(ward_annotations)) {
      grid_idx <- which(gridded_ward$GridID == ward_annotations$GridID[i])
      
      if (length(grid_idx) > 0) {
        gridded_ward$Classification[grid_idx] <- ward_annotations$Classification[i]
      }
    }
  }
  
  # Calculate area and population for each classification type
  classification_summary <- gridded_ward %>%
    group_by(Classification) %>%
    summarize(
      Area_sqkm = sum(st_area(geometry)) %>% units::set_units(km^2) %>% as.numeric(),
      .groups = 'drop'
    ) %>%
    mutate(
      DensityEstimate = sapply(Classification, function(c) {
        if (c %in% names(density_estimates)) {
          return(density_estimates[[c]])
        } else {
          return(1000)  # Default density
        }
      }),
      EstimatedPopulation = round(Area_sqkm * DensityEstimate)
    )
  
  # Calculate total classified and unclassified areas
  classified_area <- sum(classification_summary$Area_sqkm[classification_summary$Classification != "Unclassified"])
  unclassified_area <- sum(classification_summary$Area_sqkm[classification_summary$Classification == "Unclassified"])
  
  # Calculate total population
  total_population <- sum(classification_summary$EstimatedPopulation)
  
  return(list(
    summary = data.frame(
      WardName = ward_name,
      TotalArea_sqkm = ward_area,
      TotalPopulation = total_population,
      PopulationSource = "Grid_Classification_Estimate",
      ClassifiedArea_sqkm = classified_area,
      UnclassifiedArea_sqkm = unclassified_area
    ),
    classification = classification_summary
  ))
}

# ==============================================================================
# NET DISTRIBUTION CALCULATION
# ==============================================================================

#' Calculate net distribution
#'
#' @param population_data Population data
#' @param total_nets Total number of nets available
#' @param hh_distribution Household distribution by size
#' @param nets_per_hh Nets per household by size
#' @return List with net distribution results
calculate_net_distribution <- function(population_data, total_nets, hh_distribution, nets_per_hh) {
  # Default household size distribution if not provided
  if (missing(hh_distribution)) {
    hh_distribution <- c(`1-2` = 0.3, `3-4` = 0.4, `5+` = 0.3)
  }
  
  # Default nets per household size if not provided
  if (missing(nets_per_hh)) {
    nets_per_hh <- c(`1-2` = 1, `3-4` = 2, `5+` = 3)
  }
  
  # Normalize household distribution to sum to 1
  hh_distribution <- hh_distribution / sum(hh_distribution)
  
  # Average people per household category
  avg_people_per_hh <- c(`1-2` = 1.5, `3-4` = 3.5, `5+` = 5.5)
  
  # Calculate average household size
  avg_hh_size <- sum(hh_distribution * avg_people_per_hh)
  
  # Process each classification
  results <- list()
  
  if (!is.null(population_data) && !is.null(population_data$classification)) {
    classifications <- population_data$classification
    
    # Only include populated areas (excluding No Buildings and Avoid Area)
    populated_areas <- classifications[classifications$EstimatedPopulation > 0, ]
    
    if (nrow(populated_areas) > 0) {
      # Calculate total households in each classification
      populated_areas$TotalHouseholds <- round(populated_areas$EstimatedPopulation / avg_hh_size)
      
      # Calculate households by size category
      populated_areas$Households_small <- round(populated_areas$TotalHouseholds * hh_distribution[["1-2"]])
      populated_areas$Households_medium <- round(populated_areas$TotalHouseholds * hh_distribution[["3-4"]])
      populated_areas$Households_large <- round(populated_areas$TotalHouseholds * hh_distribution[["5+"]])
      
      # Calculate nets needed
      populated_areas$NetsNeeded <- (populated_areas$Households_small * nets_per_hh[["1-2"]]) +
        (populated_areas$Households_medium * nets_per_hh[["3-4"]]) +
        (populated_areas$Households_large * nets_per_hh[["5+"]])
      
      # Calculate percentage of total nets
      total_nets_needed <- sum(populated_areas$NetsNeeded)
      populated_areas$NetPercentNeeded <- populated_areas$NetsNeeded / max(total_nets_needed, 1) * 100
      
      # Allocate available nets proportionally
      if (total_nets <= total_nets_needed) {
        # Not enough nets for full coverage - allocate proportionally
        populated_areas$NetsAllocated <- round(populated_areas$NetPercentNeeded / 100 * total_nets)
      } else {
        # More than enough nets - everyone gets what they need
        populated_areas$NetsAllocated <- populated_areas$NetsNeeded
      }
      
      # Calculate population covered
      populated_areas$AllocatedNetCoverage <- populated_areas$NetsAllocated / populated_areas$NetsNeeded
      populated_areas$PopulationCovered <- round(populated_areas$EstimatedPopulation * 
                                                   pmin(1, populated_areas$AllocatedNetCoverage))
      
      # Overall coverage metrics
      total_population <- sum(populated_areas$EstimatedPopulation)
      total_population_covered <- sum(populated_areas$PopulationCovered)
      percent_population_covered <- round(total_population_covered / max(total_population, 1) * 100, 1)
      
      # Prepare results
      results <- list(
        classification_results = populated_areas,
        summary = data.frame(
          TotalPopulation = total_population,
          TotalHouseholds = sum(populated_areas$TotalHouseholds),
          TotalNetsNeeded = total_nets_needed,
          TotalNetsAvailable = total_nets,
          TotalPopulationCovered = total_population_covered,
          PercentPopulationCovered = percent_population_covered,
          AverageHouseholdSize = avg_hh_size
        )
      )
    }
  }
  
  return(results)
}

#' Calculate prioritized net distribution
#'
#' @param ward_data Ward data
#' @param total_nets Total number of nets available
#' @param avg_household_size Average household size
#' @param urban_threshold Urban extent threshold
#' @param strategy Distribution strategy
#' @param grid_overrides Grid overrides
#' @return List with net distribution results
calculate_prioritized_net_distribution <- function(ward_data, total_nets, avg_household_size, 
                                                   urban_threshold = 30, strategy = "rank",
                                                   grid_overrides = NULL) {
  # Ensure the required columns exist in ward_data
  if (!"UrbanPercent" %in% names(ward_data)) {
    if ("Urban" %in% names(ward_data)) {
      # If Urban is binary (Yes/No), convert to percentage (100/0)
      ward_data$UrbanPercent <- ifelse(ward_data$Urban %in% c("Yes", "YES", "yes", "Y", "y"), 100, 0)
    } else {
      ward_data$UrbanPercent <- 0  # Default assumption
      warning("No Urban data found, assuming all areas are non-urban")
    }
  }
  
  # Extract all ward names from shapefile
  all_ward_names <- ward_data$WardName
  
  # Try to get state information
  state_name <- NULL
  if ("StateCode" %in% names(ward_data)) {
    # Get the first state code (assuming all wards are from the same state)
    state_code <- ward_data$StateCode[1]
    state_name <- map_state_code(state_code)
    cat("Detected state code:", state_code, "mapped to state name:", state_name, "\n")
  }
  
  # Initialize population source column
  ward_data$PopulationSource <- "Density_Estimate"
  
  # Get ITN population data matching our ward names
  if (!is.null(state_name)) {
    # Build file path to ITN data
    file_path <- file.path("www/data/population", paste0("pbi_distribution_", state_name, ".csv"))
    excel_path <- file.path("www/data/population", paste0("pbi_distribution_", state_name, ".xlsx"))
    
    # Check if file exists
    itn_file_exists <- file.exists(file_path) || file.exists(excel_path)
    cat("Looking for ITN data file:", file_path, "or", excel_path, "- Exists:", itn_file_exists, "\n")
    
    if (itn_file_exists) {
      # Read ITN data
      if (file.exists(file_path)) {
        itn_data <- read.csv(file_path)
      } else {
        itn_data <- readxl::read_excel(excel_path, sheet = 1)
      }
      
      # Process the ITN data for ward population
      ward_population_data <- itn_data %>%
        rename(population = N_FamilyMembers,
               Ward = AdminLevel3) %>%
        select(Ward, population) %>%
        # Group by ward and sum population
        group_by(Ward) %>%
        summarise(Population = sum(population, na.rm = TRUE)) %>%
        ungroup()
      
      cat("ITN data loaded with", nrow(ward_population_data), "wards\n")
      cat("Sample ITN wards:", paste(head(ward_population_data$Ward), collapse=", "), "\n")
      cat("Sample shapefile wards:", paste(head(all_ward_names), collapse=", "), "\n")
      
      # Create lowercase versions for matching
      ward_population_data$Ward_lower <- tolower(ward_population_data$Ward)
      
      # Result dataframe for matched wards
      ward_matches <- data.frame(
        WardName = character(),
        Population = numeric(),
        MatchFound = logical(),
        stringsAsFactors = FALSE
      )
      
      # Check each ward name for matches
      for (ward in all_ward_names) {
        ward_lower <- tolower(ward)
        match_found <- any(ward_population_data$Ward_lower == ward_lower)
        
        if (match_found) {
          # Get the population for this ward
          ward_pop <- ward_population_data$Population[ward_population_data$Ward_lower == ward_lower]
          population <- ward_pop[1]  # Take first match if multiple
        } else {
          population <- NA
        }
        
        # Add to result
        ward_matches <- rbind(ward_matches, data.frame(
          WardName = ward,
          Population = population,
          MatchFound = match_found,
          stringsAsFactors = FALSE
        ))
      }
      
      # Count how many matches we found
      matches_found <- sum(ward_matches$MatchFound, na.rm = TRUE)
      cat("Found matches for", matches_found, "out of", length(all_ward_names), "wards\n")
      
      # Only join if we found matches
      if (matches_found > 0) {
        # Join the population data with ward_data
        ward_data <- left_join(ward_data, 
                               ward_matches %>% select(WardName, Population),
                               by = "WardName")
        
        # Where we found a match, use ITN population data
        for (i in 1:nrow(ward_data)) {
          if (!is.na(ward_data$Population[i])) {
            ward_data$EstimatedPopulation[i] <- ward_data$Population[i]
            ward_data$PopulationSource[i] <- "ITN_Distribution_Data"
          }
        }
      }
      
      # Store match statistics for display
      ward_data$ITNDataMatches <- matches_found
      ward_data$TotalWards <- length(all_ward_names)
    }
  }
  
  # Initialize all the columns we'll use later to avoid the replacement error
  ward_data$HasGridClassifications <- FALSE
  ward_data$ValidGridCells <- NA_integer_
  ward_data$AdjustedPopulation <- NA_real_
  ward_data$TotalGridCells <- NA_integer_
  
  # Apply manual grid overrides if available
  if (!is.null(grid_overrides) && nrow(grid_overrides) > 0) {
    for (i in 1:nrow(grid_overrides)) {
      ward_name <- grid_overrides$WardName[i]
      idx <- which(ward_data$WardName == ward_name)
      
      if (length(idx) > 0) {
        # Mark the ward as having grid classifications
        ward_data$HasGridClassifications[idx] <- TRUE
        # Store the number of valid grid cells
        ward_data$ValidGridCells[idx] <- grid_overrides$TotalValidGrids[i]
      }
    }
  }
  
  # Ensure EstimatedPopulation column exists for density-based estimates
  if (!"EstimatedPopulation" %in% names(ward_data)) {
    ward_data$EstimatedPopulation <- NA_real_
  }
  
  # For wards without population data, estimate it
  for (i in 1:nrow(ward_data)) {
    if (is.na(ward_data$EstimatedPopulation[i])) {
      # Use area-based estimation
      if ("area" %in% names(ward_data) && !is.na(ward_data$area[i])) {
        # Urban status affects density
        is_urban <- !is.na(ward_data$UrbanPercent[i]) && ward_data$UrbanPercent[i] >= urban_threshold
        base_density <- if (is_urban) 5000 else 500
        ward_data$EstimatedPopulation[i] <- ward_data$area[i] * base_density
      } else {
        # Default population if no area data
        ward_data$EstimatedPopulation[i] <- 5000
      }
    }
  }
  
  # Filter based on urban threshold
  ward_data$MeetsThreshold <- !is.na(ward_data$UrbanPercent) & ward_data$UrbanPercent >= urban_threshold
  
  # For wards with grid classifications, adjust population estimate based on valid grid cells
  if (!is.null(grid_overrides) && nrow(grid_overrides) > 0) {
    for (i in 1:nrow(grid_overrides)) {
      ward_name <- grid_overrides$WardName[i]
      idx <- which(ward_data$WardName == ward_name)
      
      if (length(idx) > 0 && !is.na(ward_data$ValidGridCells[idx])) {
        # Get total grid cells for this ward
        if ("TotalGridCells" %in% names(ward_data) && !is.na(ward_data$TotalGridCells[idx])) {
          # If we know total cells, adjust population proportionally
          total_cells <- ward_data$TotalGridCells[idx]
          valid_cells <- ward_data$ValidGridCells[idx]
          
          # If total cells is available, adjust population by grid cell ratio
          if (total_cells > 0) {
            ward_data$AdjustedPopulation[idx] <- ward_data$EstimatedPopulation[idx] * 
              (valid_cells / total_cells)
          } else {
            # Otherwise use a fixed amount per cell
            ward_data$AdjustedPopulation[idx] <- valid_cells * 250 # Assume 250 people per grid cell
          }
        } else {
          # If no total grid cells, use fixed amount per cell
          ward_data$AdjustedPopulation[idx] <- grid_overrides$TotalValidGrids[i] * 250
        }
      }
    }
  }
  
  # Calculate total households
  ward_data$TotalHouseholds <- ceiling(ward_data$EstimatedPopulation / avg_household_size)
  
  # Calculate nets needed (1 net per 1.8 people, min 1 per household)
  ward_data$NetsNeeded <- pmax(ceiling(ward_data$EstimatedPopulation / 1.8), 
                               ward_data$TotalHouseholds)
  
  # For wards with grid classifications, calculate nets needed based on adjusted population
  for (i in 1:nrow(ward_data)) {
    if (ward_data$HasGridClassifications[i] && !is.na(ward_data$AdjustedPopulation[i])) {
      ward_data$NetsNeeded[i] <- max(
        ceiling(ward_data$AdjustedPopulation[i] / 1.8),
        ceiling(ward_data$AdjustedPopulation[i] / avg_household_size)
      )
    }
  }
  
  # Create prioritization categories:
  # 1. Prioritized: Below threshold (these get nets first)
  # 2. Grid-Classified: Has valid grid classifications indicating habitable areas
  # 3. Re-prioritized: Above threshold and no grid classifications (lowest priority)
  
  ward_data$Priority <- "Re-prioritized"  # Default
  # Update priority based on conditions
  ward_data$Priority[!ward_data$MeetsThreshold] <- "Prioritized"
  ward_data$Priority[ward_data$HasGridClassifications] <- "Grid-Classified"
  
  # First, get wards in order of allocation priority
  prioritized_wards <- ward_data %>%
    filter(Priority == "Prioritized") 
  
  # Sort by vulnerability rank if available
  if ("overall_rank" %in% names(prioritized_wards)) {
    prioritized_wards <- prioritized_wards %>% arrange(overall_rank)
  }
  
  grid_classified_wards <- ward_data %>%
    filter(Priority == "Grid-Classified")
  
  # Sort by vulnerability rank if available
  if ("overall_rank" %in% names(grid_classified_wards)) {
    grid_classified_wards <- grid_classified_wards %>% arrange(overall_rank)
  }
  
  reprioritized_wards <- ward_data %>%
    filter(Priority == "Re-prioritized")
  
  # Sort by vulnerability rank if available
  if ("overall_rank" %in% names(reprioritized_wards)) {
    reprioritized_wards <- reprioritized_wards %>% arrange(overall_rank)
  }
  
  # Initialize allocation columns
  ward_data$NetsAllocated <- 0
  ward_data$CoveragePercent <- 0
  
  # Function to allocate nets to a group of wards
  allocate_nets <- function(wards, remaining_nets) {
    if(nrow(wards) == 0) {
      return(list(wards = wards, remaining_nets = remaining_nets))
    }
    
    wards$NetsAllocated <- 0
    wards$CoveragePercent <- 0
    
    # First, allocate at least some nets to each ward if possible
    if (remaining_nets > 0 && nrow(wards) > 0) {
      # Calculate nets needed for each ward
      total_nets_needed <- sum(wards$NetsNeeded)
      
      if (remaining_nets >= total_nets_needed) {
        # If we have enough nets for full coverage
        wards$NetsAllocated <- wards$NetsNeeded
        wards$CoveragePercent <- 100
        remaining_nets <- remaining_nets - total_nets_needed
      } else {
        # Distribute nets proportionally based on need
        for (i in 1:nrow(wards)) {
          # Calculate fair share based on percentage of total need
          fair_share <- round(remaining_nets * (wards$NetsNeeded[i] / total_nets_needed))
          
          # Ensure we don't allocate more than needed or available
          allocated <- min(fair_share, wards$NetsNeeded[i], remaining_nets)
          
          wards$NetsAllocated[i] <- allocated
          wards$CoveragePercent[i] <- round(allocated / wards$NetsNeeded[i] * 100, 1)
          remaining_nets <- remaining_nets - allocated
        }
        
        # If we still have nets left, distribute them to maximize coverage
        if (remaining_nets > 0) {
          # Sort by how close they are to full coverage
          coverage_gap <- data.frame(
            index = 1:nrow(wards),
            gap = wards$NetsNeeded - wards$NetsAllocated
          )
          coverage_gap <- coverage_gap[coverage_gap$gap > 0, ]
          coverage_gap <- coverage_gap[order(coverage_gap$gap), ]
          
          for (i in 1:nrow(coverage_gap)) {
            if (remaining_nets <= 0) break
            
            idx <- coverage_gap$index[i]
            nets_to_add <- min(coverage_gap$gap[i], remaining_nets)
            
            wards$NetsAllocated[idx] <- wards$NetsAllocated[idx] + nets_to_add
            wards$CoveragePercent[idx] <- round(wards$NetsAllocated[idx] / wards$NetsNeeded[idx] * 100, 1)
            remaining_nets <- remaining_nets - nets_to_add
          }
        }
      }
    }
    
    return(list(wards = wards, remaining_nets = remaining_nets))
  }
  
  # Allocate nets to prioritized wards first (below threshold)
  remaining_nets <- total_nets
  
  if (nrow(prioritized_wards) > 0) {
    allocation <- allocate_nets(prioritized_wards, remaining_nets)
    prioritized_wards <- allocation$wards
    remaining_nets <- allocation$remaining_nets
    
    # Update main ward_data with allocation
    for (i in 1:nrow(prioritized_wards)) {
      idx <- which(ward_data$WardName == prioritized_wards$WardName[i])
      if (length(idx) > 0) {
        ward_data$NetsAllocated[idx] <- prioritized_wards$NetsAllocated[i]
        ward_data$CoveragePercent[idx] <- prioritized_wards$CoveragePercent[i]
      }
    }
  }
  
  # Next, allocate to grid-classified wards
  if (nrow(grid_classified_wards) > 0 && remaining_nets > 0) {
    allocation <- allocate_nets(grid_classified_wards, remaining_nets)
    grid_classified_wards <- allocation$wards
    remaining_nets <- allocation$remaining_nets
    
    # Update main ward_data with allocation
    for (i in 1:nrow(grid_classified_wards)) {
      idx <- which(ward_data$WardName == grid_classified_wards$WardName[i])
      if (length(idx) > 0) {
        ward_data$NetsAllocated[idx] <- grid_classified_wards$NetsAllocated[i]
        ward_data$CoveragePercent[idx] <- grid_classified_wards$CoveragePercent[i]
      }
    }
  }
  
  # Finally, if nets remain, allocate to re-prioritized wards (above threshold)
  if (nrow(reprioritized_wards) > 0 && remaining_nets > 0) {
    allocation <- allocate_nets(reprioritized_wards, remaining_nets)
    reprioritized_wards <- allocation$wards
    remaining_nets <- allocation$remaining_nets
    
    # Update main ward_data with allocation
    for (i in 1:nrow(reprioritized_wards)) {
      idx <- which(ward_data$WardName == reprioritized_wards$WardName[i])
      if (length(idx) > 0) {
        ward_data$NetsAllocated[idx] <- reprioritized_wards$NetsAllocated[i]
        ward_data$CoveragePercent[idx] <- reprioritized_wards$CoveragePercent[i]
      }
    }
  }
  
  # Calculate population covered based on coverage percentage
  ward_data$PopulationCovered <- 0  # Initialize to avoid errors
  
  for (i in 1:nrow(ward_data)) {
    if (ward_data$HasGridClassifications[i] && !is.na(ward_data$AdjustedPopulation[i])) {
      # Use adjusted population for grid-classified
      ward_data$PopulationCovered[i] <- round(ward_data$AdjustedPopulation[i] * ward_data$CoveragePercent[i] / 100)
    } else {
      # Use regular population otherwise
      ward_data$PopulationCovered[i] <- round(ward_data$EstimatedPopulation[i] * ward_data$CoveragePercent[i] / 100)
    }
  }
  
  # Calculate households covered based on coverage percentage
  ward_data$HouseholdsCovered <- ceiling(ward_data$TotalHouseholds * ward_data$CoveragePercent / 100)
  
  # Calculate summary statistics
  # Target population includes prioritized wards and grid-classified areas
  target_population <- sum(
    sum(ward_data$EstimatedPopulation[ward_data$Priority == "Prioritized"], na.rm = TRUE),
    sum(
      ifelse(
        ward_data$Priority == "Grid-Classified" & !is.na(ward_data$AdjustedPopulation),
        ward_data$AdjustedPopulation,
        0
      ),
      na.rm = TRUE
    )
  )
  
  # Ensure target population is never zero to avoid division issues
  if (target_population <= 0) {
    target_population <- sum(ward_data$EstimatedPopulation, na.rm = TRUE)
  }
  
  # Calculate total population that will be covered
  population_covered <- sum(ward_data$PopulationCovered, na.rm = TRUE)
  
  # Calculate total households 
  total_households <- sum(ward_data$TotalHouseholds, na.rm = TRUE)
  
  # Calculate total households covered
  households_covered <- sum(ward_data$HouseholdsCovered, na.rm = TRUE)
  
  # Calculate statistics by priority group
  prioritized_allocation <- sum(ward_data$NetsAllocated[ward_data$Priority == "Prioritized"], na.rm = TRUE)
  grid_classified_allocation <- sum(ward_data$NetsAllocated[ward_data$Priority == "Grid-Classified"], na.rm = TRUE)
  reprioritized_allocation <- sum(ward_data$NetsAllocated[ward_data$Priority == "Re-prioritized"], na.rm = TRUE)
  
  prioritized_needed <- sum(ward_data$NetsNeeded[ward_data$Priority == "Prioritized"], na.rm = TRUE)
  grid_classified_needed <- sum(ward_data$NetsNeeded[ward_data$Priority == "Grid-Classified"], na.rm = TRUE)
  
  prioritized_coverage <- ifelse(prioritized_needed > 0,
                                 round(prioritized_allocation / prioritized_needed * 100, 1),
                                 0)
  
  grid_classified_coverage <- ifelse(grid_classified_needed > 0,
                                     round(grid_classified_allocation / grid_classified_needed * 100, 1),
                                     0)
  
  # Calculate overall coverage percentage
  overall_coverage_percent <- round(population_covered / max(target_population, 1) * 100, 1)
  
  # Count ITN data matches
  itn_matches <- sum(ward_data$PopulationSource == "ITN_Distribution_Data", na.rm = TRUE)
  using_itn_data <- itn_matches > 0
  
  # Prepare summary statistics
  summary <- list(
    TotalPopulation = sum(ward_data$EstimatedPopulation, na.rm = TRUE),
    TargetPopulation = max(target_population, 1),  # Avoid division by zero
    PopulationCovered = population_covered,
    CoveragePercent = overall_coverage_percent,
    TotalNets = total_nets,
    NetsDistributed = sum(ward_data$NetsAllocated, na.rm = TRUE),
    NetsNeeded = prioritized_needed + grid_classified_needed,
    PrioritizedWards = sum(ward_data$Priority == "Prioritized", na.rm = TRUE),
    GridClassifiedWards = sum(ward_data$Priority == "Grid-Classified", na.rm = TRUE),
    ReprioritizedWards = sum(ward_data$Priority == "Re-prioritized", na.rm = TRUE),
    PrioritizedNets = prioritized_allocation,
    GridClassifiedNets = grid_classified_allocation,
    ReprioritizedNets = reprioritized_allocation,
    PrioritizedCoverage = prioritized_coverage,
    GridClassifiedCoverage = grid_classified_coverage,
    ReprioritizedCoverage = 0,  # Will calculate below if possible
    RemainingNets = remaining_nets,
    TotalHouseholds = total_households,
    HouseholdsCovered = households_covered,
    UsingITNData = using_itn_data,
    ITNDataMatches = itn_matches,
    TotalWards = nrow(ward_data),
    StateName = state_name
  )
  
  # Calculate reprioritized coverage if data available
  reprioritized_needed <- sum(ward_data$NetsNeeded[ward_data$Priority == "Re-prioritized"], na.rm = TRUE)
  if (reprioritized_needed > 0) {
    summary$ReprioritizedCoverage <- round(reprioritized_allocation / reprioritized_needed * 100, 1)
  }
  
  # Return both ward data and summary statistics
  return(list(
    wards = ward_data,
    summary = summary
  ))
}

# ==============================================================================
# UTILITY FUNCTIONS
# ==============================================================================

#' Get color for coverage level
#'
#' @param coverage Coverage percentage
#' @return Hex color code
get_coverage_color <- function(coverage) {
  if (is.na(coverage)) return("#CCCCCC")
  
  if (coverage <= 0) return("#CCCCCC")
  else if (coverage < 25) return("#FF5722")
  else if (coverage < 50) return("#FFC107")
  else if (coverage < 75) return("#8BC34A")
  else if (coverage < 100) return("#4CAF50")
  else return("#1B5E20")  # 100% coverage
}

#' Get color for classification
#'
#' @param classification Classification
#' @return Hex color code
get_classification_color <- function(classification) {
  colors <- c(
    "Formal" = "#0074D9",           # Blue
    "Informal" = "#FF4136",         # Red
    "No Buildings/Avoid Area" = "#2ECC40",  # Green
    "Unclassified" = "#AAAAAA"      # Gray
  )
  
  if (classification %in% names(colors)) {
    return(colors[classification])
  } else {
    return("#AAAAAA")  # Default gray for unclassified or unknown
  }
}

#' Create color palette for coverage levels
#'
#' @return Color palette function
get_coverage_color_palette <- function() {
  colorBin(
    palette = c("#CCCCCC", "#FF5722", "#FFC107", "#8BC34A", "#4CAF50", "#1B5E20"),
    domain = 0:100,
    bins = c(0, 1, 25, 50, 75, 100),
    na.color = "#CCCCCC"
  )
}


# Add these CSS styles to your create_custom_css function
create_custom_css <- function() {
  tags$head(
    tags$style(HTML("
      /* Existing CSS styles... */
      
      /* NEW styles for map visualization */
      .leaflet-container {
        background-color: #f8f8f8 !important;
      }
      
      /* Make the colors more vibrant */
      .formal-cell {
        stroke: white !important;
        stroke-width: 2px !important;
        fill: rgba(0, 116, 217, 0.7) !important; /* Blue */
        fill-opacity: 0.7 !important;
      }
      
      .informal-cell {
        stroke: white !important;
        stroke-width: 2px !important;
        fill: rgba(255, 65, 54, 0.7) !important; /* Red */
        fill-opacity: 0.7 !important;
      }
      
      .avoid-cell {
        stroke: white !important;
        stroke-width: 2px !important;
        fill: rgba(46, 204, 64, 0.7) !important; /* Green */
        fill-opacity: 0.7 !important;
      }
      
      .unclassified-cell {
        stroke: white !important;
        stroke-width: 1px !important;
        fill: rgba(170, 170, 170, 0.1) !important; /* Light gray */
        fill-opacity: 0.1 !important;
      }
      
      /* Improve map download button appearance */
      #download_map {
        background-color: #5D4E6D;
        color: white;
        transition: background-color 0.3s;
      }
      
      #download_map:hover {
        background-color: #4A3D57;
      }
      
      .download-tooltip {
        position: absolute;
        background: white;
        padding: 5px 10px;
        border-radius: 3px;
        box-shadow: 0 0 5px rgba(0,0,0,0.2);
        display: none;
        z-index: 1000;
      }
      
      /* Styles for map download modal */
      .map-download-modal .modal-content {
        background-color: #f7f7f7;
      }
      
      .map-download-modal .modal-header {
        background-color: #5D4E6D;
        color: white;
      }
      
      .map-download-options {
        padding: 15px;
        background-color: #f0f0f0;
        border-radius: 5px;
        margin-bottom: 15px;
      }
      
      .map-preview {
        border: 1px solid #ddd;
        border-radius: 5px;
        overflow: hidden;
      }
    "))
  )
}






###############################################################################
# Added Functions for population estimates
################################################################################

# Map state codes to full state names
map_state_code <- function(state_code) {
  state_mapping <- list(
    "DE" = "Delta",
    "KN" = "Kano",
    "KD" = "Kaduna",
    "KT" = "Katsina",
    "NG" = "Niger",
    "OS" = "Osun",
    "TB" = "Taraba",
    "YB" = "Yobe"
  )
  
  if (state_code %in% names(state_mapping)) {
    return(state_mapping[[state_code]])
  } else {
    return(NULL)  # Return NULL if code not found
  }
}



# Load population data from ITN distribution files
# Updated function to load and match ITN population data
load_itn_population <- function(state_name, ward_names, data_dir = "www/data/population") {
  # Build file path
  file_path <- file.path(data_dir, paste0("pbi_distribution_", state_name, ".csv"))
  
  # Check if file exists
  if (!file.exists(file_path)) {
    # Try Excel format if CSV not found
    excel_path <- file.path(data_dir, paste0("pbi_distribution_", state_name, ".xlsx"))
    if (!file.exists(excel_path)) {
      return(NULL)
    }
    
    # Read Excel file
    tryCatch({
      itn_data <- readxl::read_excel(excel_path, sheet = 1)
    }, error = function(e) {
      message("Error reading Excel file: ", e$message)
      return(NULL)
    })
  } else {
    # Read CSV file
    tryCatch({
      itn_data <- read.csv(file_path)
    }, error = function(e) {
      message("Error reading CSV file: ", e$message)
      return(NULL)
    })
  }
  
  # Process the ITN data and match with provided ward names
  tryCatch({
    # Extract population data for AdminLevel3 (ward level)
    ward_population_data <- itn_data %>%
      rename(population = N_FamilyMembers,
             Ward = AdminLevel3) %>%
      select(Ward, population) %>%
      # Group by ward and sum population
      group_by(Ward) %>%
      summarise(Population = sum(population, na.rm = TRUE)) %>%
      ungroup()
    
    # Create lowercase versions for matching
    ward_population_data$Ward_lower <- tolower(ward_population_data$Ward)
    
    # Create a result dataframe for the matched wards
    result <- data.frame(
      WardName = character(),
      Population = numeric(),
      MatchFound = logical(),
      stringsAsFactors = FALSE
    )
    
    # Check each ward name for matches
    for (ward in ward_names) {
      ward_lower <- tolower(ward)
      match_found <- ward_lower %in% ward_population_data$Ward_lower
      
      if (match_found) {
        # Get the population for this ward
        ward_pop <- ward_population_data$Population[ward_population_data$Ward_lower == ward_lower]
        population <- ward_pop[1]  # Take first match if multiple
      } else {
        population <- NA
      }
      
      # Add to result
      result <- rbind(result, data.frame(
        WardName = ward,
        Population = population,
        MatchFound = match_found,
        stringsAsFactors = FALSE
      ))
    }
    
    return(result)
  }, error = function(e) {
    message("Error processing ITN data: ", e$message)
    return(NULL)
  })
}