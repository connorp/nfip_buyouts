## Connor P. Jackson second year paper
## cpjackson@berkeley.edu
## Query the OpenFEMA API for data in CSV format, and write it out to a file

library(data.table)
library(httr)
library(jsonlite)

policies_path <- "https://www.fema.gov/api/open/v1/FimaNfipPolicies"
claims_path <- "https://www.fema.gov/api/open/v1/FimaNfipClaims"


query_FEMA_API <- function(API_url, outfile, columns = NULL, rowfilter = NULL,
                           perQuery = 1000, N = NULL) {
  # For a given API path, query the API for CSV data, passing parameters

  if (is.null(N)) {
    # Query the API for the length of the data
    initial_resp <- httr::GET(url = API_url,
                              query = list(`$inlinecount` = 'allpages',
                                           `$top` = 1,
                                           `$filter` = rowfilter))
    parsed_result <- jsonlite::fromJSON(httr::content(initial_resp, as = "text"))
    N <- parsed_result$metadata$count
    print(N)
  }

  # Build the vector of URLs to query
  assemble_url <- function(skipN) {
    # Assemble a single URL with the appropriate number of rows skipped
    final_url <- httr::modify_url(url = API_url,
                                  query = list(`$skip` = skipN,
                                               `$filter` = rowfilter,
                                               `$select` = columns,
                                               `$top` = perQuery,
                                               `$format` = "csv"))
    return(final_url)
  }

  # Assemble a vector of URLs with the same criteria but per page
  page_indices <- seq(from = 0, to = N, by = perQuery)
  query_urls <- sapply(page_indices, assemble_url)

  topchunk <- httr::GET(query_urls[1])
  toptable <- fread(text=httr::content(topchunk, as = "text"), na.strings = "")
  col_names <- names(toptable)

  # Start a CSV file with column headers and nothing else
  write(paste(col_names, collapse = ","), outfile)

  query_and_read <- function(url) {
    # query a URL, get the results, and append it to the CSV
    query_resp <- httr::GET(url)
    if (http_error(query_resp)) {
      return(c("Failure: ", url))
    }
    # Sys.sleep(stats::runif(1, min = 1, max = min(2^i, 60)))
    res_table <- fread(text=httr::content(query_resp, as = "text"), na.strings = "")
    fwrite(res_table[, ..col_names], outfile, append=TRUE)
  }

  lapply(query_urls, query_and_read)
}

query_FEMA_API(policies_path, "../data_buyouts/policies.csv",
               rowfilter = "propertyState eq 'NC'", N = 1629168)

