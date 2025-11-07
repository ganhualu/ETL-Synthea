#' @title Load All Vocabulary Tables From CSV Files in a Directory.
#'
#' @description A convenience function that finds and loads all standard vocabulary CSV files
#'              from a specified directory by calling \code{LoadSingleVocabFromCsv} for each file.
#'
#' @details This function is NOT recommended for systems with low RAM, as it iterates through
#'          all files sequentially in a single R session. If you have memory constraints,
#'          call \code{LoadSingleVocabFromCsv} for each file manually in separate R scripts or sessions.
#'
#' @inheritParams LoadSingleVocabFromCsv
#' @param vocabFileLoc     The path to the directory containing the vocabulary CSV files.
#'
#'@export
LoadVocabFromCsv_gl <- function(connectionDetails,
                             cdmSchema,
                             vocabFileLoc,
                             bulkLoad = FALSE,
                             delimiter = "\t") {

  # List of standard vocabulary CSV files
  csvList <- c(
    "concept.csv", "vocabulary.csv", "concept_ancestor.csv",
    "concept_relationship.csv", "relationship.csv", "concept_synonym.csv",
    "domain.csv", "concept_class.csv", "drug_strength.csv"
  )

  if (!dir.exists(vocabFileLoc)) {
    stop("Vocabulary file location is not a valid directory: ", vocabFileLoc)
  }

  # Find which of the standard vocabulary files exist in the directory
  filesToLoad <- list.files(
    path = vocabFileLoc,
    pattern = paste0("^(", paste(csvList, collapse = "|"), ")$"),
    full.names = TRUE,
    ignore.case = TRUE
  )

  if (length(filesToLoad) == 0) {
    warning("No standard vocabulary CSV files found in the specified location.")
    return()
  }

  # Call the single-file loader for each file found
  for (csvFile in filesToLoad) {
    LoadSingleVocabFromCsv(
      csvFilePath = csvFile,
      connectionDetails = connectionDetails,
      cdmSchema = cdmSchema,
      bulkLoad = bulkLoad,
      delimiter = delimiter
    )
    # Garbage collect to help release memory between file loads
    gc()
  }
}
