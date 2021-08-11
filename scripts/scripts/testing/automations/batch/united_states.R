url <- "https://healthdata.gov/api/views/j8mb-icvb/rows.csv"
df <- fread(url, showProgress = FALSE, select = c("date", "new_results_reported"))
setnames(df, "date", "Date")

df <- df[, .(`Daily change in cumulative total` = sum(new_results_reported)), Date]
df[, Date := ymd(Date)]

df[, Country := "United States"]
df[, Units := "tests performed"]
df[, `Source label` := "Department of Health & Human Services"]
df[, `Source URL` := "https://healthdata.gov/dataset/COVID-19-Diagnostic-Laboratory-Testing-PCR-Testing/j8mb-icvb"]
df[, Notes := NA_character_]

fwrite(df, "automated_sheets/United States.csv")
