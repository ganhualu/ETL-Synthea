#' @title Load a Single Vocabulary Table From a Parquet File.
#'
#' @description This function connects to the database and populates a single Vocabulary table
#'              with data from a specified Parquet file.
#'
#' @details This function is memory-efficient by processing one file at a time. It requires the
#'          \code{arrow} package to be installed. It assumes \cr\code{createCDMTables()} has
#'          already been run.
#'
#' @param parquetFilePath    The full, absolute path to the single vocabulary Parquet file.
#' @param connectionDetails  An R object of type\cr\code{connectionDetails} created using the
#'                           function \code{createConnectionDetails} in the
#'                           \code{DatabaseConnector} package.
#' @param cdmSchema          The name of the database schema that will contain the Vocabulary tables.
#'                           On SQL Server, this should include both database and schema,
#'                           e.g., 'cdm_instance.dbo'.
#' @param bulkLoad           Boolean flag for using bulk loading (if possible). Default is FALSE.
#'
#' @export
LoadSingleVocabFromParquet <- function(parquetFilePath,
                                       connectionDetails,
                                       cdmSchema,
                                       bulkLoad = FALSE) {

  # Check for arrow package dependency
  if (!requireNamespace("arrow", quietly = TRUE)) {
    stop("The 'arrow' package is required to read Parquet files. Please install it with install.packages('arrow')")
  }

  if (!file.exists(parquetFilePath)) {
    stop("The file path specified does not exist: ", parquetFilePath)
  }

  # Derive table name from the Parquet filename (e.g., "CONCEPT.parquet" -> "concept")
  tableName <- tools::file_path_sans_ext(basename(parquetFilePath))

  # Establish connection
  conn <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(conn), add = TRUE)

  writeLines(paste0("Working on: ", basename(parquetFilePath)))

  # --- 1. Read File ---
  writeLines(" - Reading data from Parquet file...")
  vocabTable <- arrow::read_parquet(file = parquetFilePath) %>%
    dplyr::tibble() # Convert to tibble for consistency

  # --- 2. Process Data ---
  # Date handling for specific tables. Parquet often stores dates natively,
  # but this ensures they are in the correct R Date format if read as characters.
  if (tolower(basename(parquetFilePath)) %in% c("concept.parquet", "concept_relationship.parquet", "drug_strength.parquet")) {
    writeLines(" - Ensuring date columns are correctly formatted...")
    if ("valid_start_date" %in% names(vocabTable)) {
      vocabTable$valid_start_date <- as.Date(vocabTable$valid_start_date)
    }
    if ("valid_end_date" %in% names(vocabTable)) {
      vocabTable$valid_end_date   <- as.Date(vocabTable$valid_end_date)
    }
  }

  # Special NA handling for drug_strength.parquet
  if (tolower(basename(parquetFilePath)) == "drug_strength.parquet") {
    writeLines(" - Applying specific rules for drug_strength.parquet...")
    vocabTable <- vocabTable %>%
      dplyr::mutate_at(
        dplyr::vars(
          "amount_value", "amount_unit_concept_id", "numerator_value",
          "numerator_unit_concept_id", "denominator_value",
          "denominator_unit_concept_id", "box_size"
        ),
        ~ replace(., is.na(.), 0)
      )
  }

  # --- 3. Upload Data ---
  chunkSize <- 1e7 # Process 10 million rows at a time
  numberOfRowsInVocabTable <- nrow(vocabTable)
  numberOfChunks <- ceiling(x = numberOfRowsInVocabTable / chunkSize)

  writeLines(
    paste0(
      " - Preparing to upload ", numberOfRowsInVocabTable, " rows in ",
      numberOfChunks, " chunk(s)."
    )
  )

  # Clear the target table before inserting new data
  sql <- "DELETE FROM @cdm_schema.@table_name;"
  DatabaseConnector::renderTranslateExecuteSql(
    connection = conn,
    sql = sql,
    cdm_schema = cdmSchema,
    table_name = tableName
  )

  # Insert data in chunks
  for (j in 1:numberOfChunks) {
    startRow <- (j - 1) * chunkSize + 1
    endRow <- min(j * chunkSize, numberOfRowsInVocabTable)

    writeLines(
      paste0(
        " - Uploading chunk ", j, " of ", numberOfChunks, " (rows ",
        startRow, " to ", endRow, ")..."
      )
    )

    chunk <- vocabTable[startRow:endRow, ]

    DatabaseConnector::insertTable(
      connection = conn,
      tableName = paste(cdmSchema, tableName, sep = "."),
      data = chunk,
      dropTableIfExists = FALSE,
      createTable = FALSE,
      bulkLoad = bulkLoad,
      progressBar = TRUE
    )
  }

  writeLines(paste0("SUCCESS: Finished loading ", basename(parquetFilePath), "."))
}
