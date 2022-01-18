url <- "https://datos.ins.gob.pe/api/3/action/package_show?id=dataset-de-pruebas-moleculares-del-instituto-nacional-de-salud-ins"

context <- rjson::fromJSON(file = url)
last_modified <- lapply(context$result$resources, FUN = "[[", "last_modified") %>%
  unlist %>%
  ymd_hms %>%
  which.max
url <- context$result$resources[[last_modified]]$url

process_file <- function(url) {
  filename <- str_extract(url, "[^/]+\\.zip$")
  local_path <- sprintf("tmp/%s", filename)
  if (!file.exists(local_path)) {
    download.file(url = url, destfile = local_path)
  }
  csv_filename <- unzip(local_path, list = TRUE)$Name[1]
  unzip(local_path, exdir = "tmp")
  df <- fread(sprintf("tmp/%s", csv_filename), showProgress = FALSE, select = c("FECHA_MUESTRA", "RESULTADO"))
  setnames(df, c("Date", "Result"))
  df[, Date := as.character(Date)]
  return(df)
}

data <- process_file(url)

data[, Date := ymd(Date)]
data <- data[Date <= today() & Date >= "2020-01-01" & !is.na(Date)]

df <- data[, .(
    `Daily change in cumulative total` = .N,
    `Positive` = sum(Result == "POSITIVO")
), Date]

setorder(df, Date)
df[, `Positive rate` := round(frollsum(Positive, 7) / frollsum(`Daily change in cumulative total`, 7), 3)]
df[, Positive := NULL]

df[, Country := "Peru"]
df[, Units := "tests performed"]
df[, `Source URL` := "https://datos.ins.gob.pe/dataset/dataset-de-pruebas-moleculares-del-instituto-nacional-de-salud-ins"]
df[, `Source label` := "National Institute of Health"]
df[, Notes := NA]

fwrite(df, "automated_sheets/Peru.csv")

stopifnot(max(df$Date) > today() - days(14))
