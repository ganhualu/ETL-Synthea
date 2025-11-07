#' @title Load All Vocabulary Tables From Parquet Files in a Directory.
#'
#' @description A convenience function that finds and loads all standard vocabulary Parquet files
#'              from a specified directory by calling \code{LoadSingleVocabFromParquet} for each file.
#'
#' @details This function is NOT recommended for systems with low RAM. If you have memory
#'          constraints, call \code{LoadSingleVocabFromParquet} for each file manually in
#'          separate R scripts or sessions. Requires the \code{arrow} package.
#'
#' @inheritParams LoadSingleVocabFromParquet
#' @param parquetFileLoc     The path to the directory containing the vocabulary Parquet files.
#'
#'@export
LoadVocabFromParquet <- function(connectionDetails,
                                 cdmSchema,
                                 parquetFileLoc,
                                 bulkLoad = FALSE) {

  # List of standard vocabulary tables. File extension will be .parquet
  vocabList <- c(
    "concept", "vocabulary", "concept_ancestor",
    "concept_relationship", "relationship", "concept_synonym",
    "domain", "concept_class", "drug_strength"
  )
  parquetList <- paste0(vocabList, ".parquet")

  if (!dir.exists(parquetFileLoc)) {
    stop("Vocabulary file location is not a valid directory: ", parquetFileLoc)
  }

  # Find which of the standard vocabulary files exist in the directory
  filesToLoad <- list.files(
    path = parquetFileLoc,
    pattern = "\\.parquet$", # Look for any parquet file
    full.names = TRUE,
    ignore.case = TRUE
  )

  # Filter down to only the ones that match our vocab list
  filesToLoad <- filesToLoad[tolower(basename(filesToLoad)) %in% tolower(parquetList)]

  if (length(filesToLoad) == 0) {
    warning("No standard vocabulary Parquet files found in the specified location.")
    return()
  }

  # Call the single-file loader for each file found
  for (parquetFile in filesToLoad) {
    LoadSingleVocabFromParquet(
      parquetFilePath = parquetFile,
      connectionDetails = connectionDetails,
      cdmSchema = cdmSchema,
      bulkLoad = bulkLoad
    )
    # Garbage collect to help release memory between file loads
    gc()
  }
}
