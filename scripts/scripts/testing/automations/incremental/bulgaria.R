url <- "https://coronavirus.bg/bg/statistika"

page <- read_html(url)

tables <- page %>% html_nodes("table") %>% html_table()

for (table in tables) {
    setDT(table)
    if ("RT PCR" %in% unlist(table[, 1])) {
        count <- table[Тип == "Общо", Общо]
    }
}

add_snapshot(
    count = count,
    sheet_name = "Bulgaria",
    country = "Bulgaria",
    units = "tests performed",
    source_url = url,
    source_label = "Bulgaria COVID-10 Information Portal"
)
