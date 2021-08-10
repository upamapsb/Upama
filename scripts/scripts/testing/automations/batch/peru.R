url <- "https://datos.ins.gob.pe/dataset/c00ea8f5-89e0-4c55-bd78-a4472605c168/resource/0eec1a5e-a3ce-4c06-80f6-123f8c3b5461/download/pm31julio2021.zip"

process_file <- function(url) {
    filename <- str_extract(url, "[^/]+\\.zip$")
    local_path <- sprintf("tmp/%s", filename)
    if (!file.exists(local_path)) {
        download.file(url = url, destfile = local_path)
    }
    csv_filename <- unzip(local_path, list = TRUE)$Name[1]
    unzip(local_path, exdir = "tmp")
    df <- fread(sprintf("tmp/%s", csv_filename), showProgress = FALSE, select = c("FECHATOMAMUESTRA", "RESULTADO"))
    setnames(df, c("Date", "Result"))
    df[, Date := as.character(Date)]
    return(df)
}

data <- process_file(url)

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
df[, `Source URL` := "https://datos.ins.gob.pe/dataset?q=%22pruebas+moleculares%22&organization=covid-19"]
df[, `Source label` := "National Institute of Health"]
df[, `Testing type` := "PCR only"]
df[, Notes := NA]

fwrite(df, "automated_sheets/Peru.csv")

stopifnot(max(df$Date) > today() - days(14))
