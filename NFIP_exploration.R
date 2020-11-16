## Connor P. Jackson second year paper
## cpjackson@berkeley.edu
## Exploratory code: FEMA National Flood Insurance Program

library(data.table)
library(httr)
library(jsonlite)

policies_path <- "https://www.fema.gov/api/open/v1/FimaNfipPolicies"
claims_path <- "https://www.fema.gov/api/open/v1/FimaNfipClaims"


query_FEMA_API <- function(API_url, columns = NULL, rowfilter = NULL, perQuery = 1000,
                           queryCap = NULL, queryN = FALSE) {
  if (queryN == TRUE) {
    # First find out how long the data are
    initial_resp <- httr::GET(url = API_url,
                              query = list(`$inlinecount` = 'allpages',
                                           `$top` = 1,
                                           `$filter` = rowfilter))
    parsed_result <- jsonlite::fromJSON(httr::content(initial_resp, as = "text"))
    N <- parsed_result$metadata$count
    print(N)
  } else{
    N <- 1629168
  }

  # If we don't want to pull the full table
  if (!is.null(queryCap)) {
    N <- min(N, queryCap)
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
  write(paste(col_names, collapse = ","), "../data_buyouts/policies.csv")

  query_and_read <- function(url) {
    query_resp <- httr::GET(url)
    # Sys.sleep(stats::runif(1, min = 1, max = min(2^i, 60)))
    res_table <- fread(text=httr::content(query_resp, as = "text"), na.strings = "")
    fwrite(res_table[, ..col_names], "../data_buyouts/policies.csv", append=TRUE)
  }

  lapply(query_urls, query_and_read)
}

query_FEMA_API(policies_path, rowfilter = "propertyState eq 'NC'")

