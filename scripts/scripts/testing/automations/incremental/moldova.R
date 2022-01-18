url <- "https://noi.md/md/societate/coronavirus-in-moldova-statistica-infectarilor-pe-teritoriul-tarii-428097"

df <- read_html(url) %>%
    html_node(".news-text table") %>%
    html_table() %>%
    data.table()

count <- df[X1 == "Testați total", X2] %>%
    str_extract("[\\d ]+$") %>%
    str_replace_all("[^\\d]", "") %>%
    as.integer()

add_snapshot(
    count = count,
    sheet_name = "Moldova",
    country = "Moldova",
    units = "tests performed",
    source_url = url,
    source_label = "Moldova Ministry of Health, Labour and Social Protection",
    testing_type = "includes non-PCR"
)
