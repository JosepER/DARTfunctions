# compare_upload_files.R


read_upload_files_from_zip <- function(path_to_zipfile){
  
  list_of_files <- unzip(zipfile = path_to_zipfile, 
                         list = TRUE)[["Name"]]
  
  files_raw <- purrr::map(list_of_files, ~readr::read_lines(unz(path_to_zipfile, .x), skip = 1) )
  names(files_raw) <- stringr::str_sub(list_of_files, start = 1, end = 8)
  
  file <- files_raw %>%
    purrr::map(~stringr::str_split(.x, pattern = "\\|\\$\\|") %>%
                 purrr::map(~purrr::set_names(.x , c("indicator_id", "variable_id", "group_value_id", "value"))) %>%
                 dplyr::bind_rows()) %>%
    dplyr::bind_rows(.id = "file")
  
  return(file)
  
}


retrieve_path_to_dart <- function(){
  
  if(version$os == "linux-gnu"){
    
    path_to_dart <- "/media/Share/Projects/2018-DART/"
    
  }else if(version$os == "mingw32"){
    
    path_to_dart <- "S:/Projects/2018-DART/"
    
  }else{
    
    stop("OS version could not be identified")
    
  }
  
  
  return(path_to_dart)
  
}


compare_upload_files <- function(path_to_file_1, path_to_file_2, use_labels = TRUE){
  
  file_1 <- read_upload_files_from_zip(path_to_file_1)
  
  file_2 <- read_upload_files_from_zip(path_to_file_2)
  
  # check missing files in file_1 and file_2
  
  not_in_file_1 <- unique(file_1[["file"]])[unique(file_1[["file"]]) %notin% unique(file_2[["file"]])]
  not_in_file_2 <- unique(file_2[["file"]])[unique(file_2[["file"]]) %notin% unique(file_1[["file"]])]
  
  if(length(not_in_file_1) > 0){
    message(glue::glue("The following files are not in 'file_1': \n {not_in_file_1}"))
  } 
  
  if(length(not_in_file_1) > 0){
    message(glue::glue("The following files are not in 'file_2': \n {not_in_file_2}"))
  } 
  
  
  # merge files
  
  names(file_1) <- c("file", "indicator_id", "variable_id", "group_value_id", "value_file_1")
  names(file_2) <- c("file", "indicator_id", "variable_id", "group_value_id", "value_file_2")
  
  merged_files <- file_1 %>%
    dplyr::left_join(file_2, by = c("file", "indicator_id", "variable_id", "group_value_id"))
  
  merged_files %<>%
    dplyr::mutate(ratio_diff = as.numeric(value_file_1)/as.numeric(value_file_2),
                  change = (abs_ratio_diff < 0.99) | ( abs_ratio_diff > 1.01))
  
  
  if(use_labels){
    
    merged_files %<>%
      dplyr::mutate(variable_id = dplyr::recode(variable_id, 
                                                `1` = "[1] Disposable Household Income (eq.) - dhi_eq",
                                                `2` = "[2] Market Household Income (eq.) - mhi",
                                                `3` = "[3] Gross Household Income (eq.) - hitotal_eq",
                                                `4` = "[4] Disposable Net Worth - dnw",
                                                `9` = "[9] Total Assets - asset",
                                                `10` = "[10] Value of Principal Residence - hanrp",
                                                `11` = "[11] Financial Assets - haf",
                                                `12` = "[12] Financial Investments - hafi",
                                                `13` = "[13] Total Debt - hl",
                                                `14` = "[14] Principal Residence Loans - hlrp",
                                                `15` = "[15] Consumer Loans - hlnc"),
                    indicator_id = dplyr::recode(indicator_id,
                                                 `1` =   "[1] Gini Index",
                                                 `2` =   "[2] Atkinson Index (Aversion parameter = 1.5)",
                                                 `4` =   "[4] Percentile Ratio (90/10)",
                                                 `5` =   "[5] Percentile Ratio (90/50)",
                                                 `6` =   "[6] Percentile Ratio (50/10)",
                                                 `7` =   "[7] Average",
                                                 `8` =   "[8] Median",
                                                 `9` =   "[9] Share of Low Income Workers (< 50% of Median)",
                                                 `10` =   "[10] Relative Poverty Rate at %50 of the median",
                                                 `11` =   "[11] Relative Poverty Rate at %60 of the median",
                                                 `12` =   "[12] Participation rate (with agg 9-15)",
                                                 `13` =   "[13] Income and Asset Poor in %",
                                                 `14` =   "[14] Not Income Poor, Asset Poor in %",
                                                 `15` =   "[15] Income Poor, not Asset Poor in %",
                                                 `16` =   "[16] Median Debt-to-Asset Ratio",
                                                 `17` =   "[17] Median Debt-to-Income Ratio",
                                                 `18` =   "[18] Median Debt Payment-to-Income Ratio",
                                                 `19` =   "[19] Share of Category in Total Population")
      )
    
    
    path_to_dart <- retrieve_path_to_dart()
    
    group_value_categories <- readr::read_rds(paste0(path_to_dart, "Josep/interim_outputs/map_group_categories_numbers/map_decompositions_lws_with_categories.rds")) %>%
      dplyr::select(category_num, category) %>%
      dplyr::mutate(category_num = as.character(category_num))
    
    merged_files %<>%
      dplyr::left_join(group_value_categories, by = c("group_value_id" = "category_num")) %>%
      dplyr::select(file, indicator_id, variable_id, group_value_id, category, value_file_1, value_file_2, ratio_diff, abs_ratio_diff, change)
    
    
  }
  
  return(merged_files)
  
}
