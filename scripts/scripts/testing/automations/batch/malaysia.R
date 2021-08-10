df <- fread("https://raw.githubusercontent.com/MoH-Malaysia/covid19-public/main/epidemic/tests_malaysia.csv")
df[, `Daily change in cumulative total` := `rtk-ag` + pcr]
df[, c("rtk-ag", "pcr") := NULL]
setorder(df, date)
df[, `Cumulative total` := cumsum(`Daily change in cumulative total`)]

setnames(df, "date", "Date")

df[, Country := "Malaysia"]
df[, `Source URL` := "https://github.com/MoH-Malaysia/covid19-public"]
df[, `Source label` := "Malaysia Ministry of Health"]
df[, Notes := "Made available by the Malaysia Ministry of Health on GitHub"]
df[, Units := "people tested"]

fwrite(df, "automated_sheets/Malaysia.csv")
