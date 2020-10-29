# Import policies from JSON

library(jsonlite)
library(data.table)

# Fix the column names and their order by reading the first line of data
toprow <- fromJSON(readLines("../NFIP_data/policies_actual_ND.json", n=1))
col_names <- names(toprow)

# Start a CSV file with column headers and nothing else
write(paste(col_names, collapse = ","), "../NFIP_data/policies.csv")

stream_csv <- function(X) {
  fwrite(X[, col_names], "../NFIP_data/policies.csv", append=TRUE)
}

stream_in(file("../NFIP_data/policies_actual_ND.json"),
          handler = stream_csv, pagesize = 100000)
