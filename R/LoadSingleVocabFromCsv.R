#' @title Load a Single Vocabulary Table From a CSV File.
#'
#' @description This function connects to the database and populates a single Vocabulary table
#'              with data from a specified CSV file.
#'
#' @details This function is designed to be memory-efficient by processing one file at a time.
#'          It establishes a connection, loads the data, inserts it into the database, and
#'          then disconnects. It assumes \cr\code{createCDMTables()} has already been run.
#'
#' @param csvFilePath        The full, absolute path to the single vocabulary CSV file you want to load.
#' @param connectionDetails  An R object of type\cr\code{connectionDetails} created using the
#'                           function \code{createConnectionDetails} in the
#'                           \code{DatabaseConnector} package.
#' @param cdmSchema          The name of the database schema that will contain the Vocabulary tables.
#'                           On SQL Server, this should include both database and schema,
#'                           e.g., 'cdm_instance.dbo'.
#' @param bulkLoad           Boolean flag for using bulk loading (if possible). Default is FALSE.
#' @param delimiter          The delimiter used in the CSV file. Defaults to "\\t".
#'
#' @export
LoadSingleVocabFromCsv <- function(csvFilePath,
                                   connectionDetails,
                                   cdmSchema,
                                   bulkLoad = FALSE,
                                   delimiter = "\t") {

  if (!file.exists(csvFilePath)) {
    stop("The file path specified does not exist: ", csvFilePath)
  }

  # Derive table name from the CSV filename (e.g., "concept.csv" -> "concept")
  tableName <- tools::file_path_sans_ext(basename(csvFilePath))

  # Establish connection
  conn <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(conn), add = TRUE)

  writeLines(paste0("Working on: ", basename(csvFilePath)))

  # --- 1. Read File ---
  writeLines(" - Reading data from file...")
  vocabTable <- data.table::fread(
    file = csvFilePath,
    stringsAsFactors = FALSE,
    header = TRUE,
    sep = delimiter,
    na.strings = ""
  )

  # --- 2. Process Data ---
  # Date handling for specific tables
  if (tolower(basename(csvFilePath)) %in% c("concept.csv", "concept_relationship.csv", "drug_strength.csv")) {
    writeLines(" - Handling date conversions...")
    vocabTable$valid_start_date <- as.Date(as.character(vocabTable$valid_start_date), "%Y%m%d")
    vocabTable$valid_end_date   <- as.Date(as.character(vocabTable$valid_end_date), "%Y%m%d")
  }

  writeLines(" - Converting column types...")
  vocabTable <- readr::type_convert(df = vocabTable, col_types = readr::cols(), na = c("")) %>%
    dplyr::tibble()

  # Special NA handling for drug_strength.csv
  if (tolower(basename(csvFilePath)) == "drug_strength.csv") {
    writeLines(" - Applying specific rules for drug_strength.csv...")
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

  writeLines(paste0("SUCCESS: Finished loading ", basename(csvFilePath), "."))
}



