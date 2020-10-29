# Import policies from JSON

library(jsonlite)
library(data.table)

stream_csv <- function(X) {
  Xtab <- fromJSON(as.character(X))
  fwrite(xtab, "policies.csv", append=TRUE)
}

colfile <- fromJSON(file("policies_first_record.json"))
names(colfile)

policies_json <- stream_in(file("fimanfippolicies.json"),
                           handler = stream_csv, pagesize = 100000)
