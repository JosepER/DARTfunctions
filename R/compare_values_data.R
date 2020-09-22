

#' Compare values in data files
#' 
#' This function is a wrap around 'implement_comparison_of_values()'. It produces a comparison of the values
#'   in all the '.fst' files that can be found in two different directories. 
#' 
#' @param path_directory_files_1 Path to the directory of first batch of .fst files which should be compared.
#' @param path_directory_files_2 Path to the directory of second batch of .fst files which should be compared.
#' @param names_files_to_compare Specifies the names of the files which files should be compared. The files should appear 
#'   in both path directories specified in 'path_directory_files_1' and 'path_directory_files_2'. If NULL (defalut),
#'   all files in the path directories will be compared. The names should be specified without the full path and 
#'   without the extension. E.g. '/media/files/my_file.fst' should be passed as 'my_file' 
#' @param variables_to_compare Variables that should be compared. Should be in all files included in the comparison
compare_data_values <- function(path_directory_files_1, path_directory_files_2, names_files_to_compare = NULL, variables_to_compare){
  
  safe_compare_lis_files_diffdf <- purrr::safely(implement_comparison_of_values)
  
  fst_files_in_dir_1 <- fs::dir_ls(path_directory_files_1) %>%
    stringr::str_subset(pattern = "fst$")
  
  fst_files_in_dir_2 <- fs::dir_ls(path_directory_files_2) %>%
    stringr::str_subset(pattern = "fst$")
  
  if(is.null(names_files_to_compare)){
    
    fst_files_for_comparison <- fst_files_in_dir_1[fst_files_in_dir_1 %in% fst_files_in_dir_2]
    
    fst_files_for_comparison %<>%
      stringr::str_sub(-8, -5)
  
  }else{
      
    assertthat::assert_that(all(names_files_to_compare %in% stringr::str_sub(fst_files_in_dir_1, -8, -5)),
                            msg = "Not all files specified in 'names_files_to_compare' are in 'path_directory_files_1'.")
    
    assertthat::assert_that(all(names_files_to_compare %in% stringr::str_sub(fst_files_in_dir_2, -8, -5)),
                            msg = "Not all files specified in 'names_files_to_compare' are in 'path_directory_files_2'.")
    
    fst_files_for_comparison <- names_files_to_compare
    
  }
  
  vector_comparison_files_in_dir_1 <- stringr::str_c(path_directory_files_1, fst_files_for_comparison, ".fst")
  
  vector_comparison_files_in_dir_2 <- stringr::str_c(path_directory_files_2, fst_files_for_comparison, ".fst")
  
  glue::glue_collapse(glue::glue("The following files were added to the comparison: {fst_files_for_comparison}."))
  
  comparisons <- furrr::future_map2(.x = vector_comparison_files_in_dir_1, 
                                    .y = vector_comparison_files_in_dir_2,
                                    ~safe_compare_lis_files_diffdf(path_to_file_1 = .x, 
                                                                   path_to_file_2 = .y, 
                                                                   variables_to_compare = variables_to_compare))
  
  names(comparisons) <- fst_files_for_comparison
  
  assertthat::assert_that(purrr::every(comparisons, ~is.null(.x[["error"]])),
                       msg = "Not all comparison could be successfully performed.")
  
  all_comparisons <- dplyr::bind_rows(map(comparisons, "result"), .id = "file")
  
  return(all_comparisons)
  
}



#' Implements comparison of values across two files.
#' 
#' Compares the values across two files for the variables specified in 'variables_to_compare'. 
#' 
#' @param path_to_files_1 Path to first .fst file.
#' @param path_to_files_1 Path to second .fst file.
#' @param variables_to_compare Variables that should be compared. They need to appear in both files 
implement_comparison_of_values <- function(path_to_file_1, path_to_file_2, variables_to_compare){
  
  comparison_ <- diffdf::diffdf(base = fst::read_fst(path_to_file_1)  %>%
                                  dplyr::select(hid, pid, dplyr::all_of(variables_to_compare)),
                                compare = fst::read_fst(path_to_file_2)  %>%
                                  dplyr::select(hid, pid, dplyr::all_of(variables_to_compare)),
                                keys = c("hid", "pid"),
                                suppress_warnings = TRUE)
  
  var_diff_elements_ <- names(comparison_) %>%
    stringr::str_detect("^VarDiff_[a-z]+") 
  
  return(purrr::keep(comparison_, var_diff_elements_) %>%
           dplyr::bind_rows())
  
}