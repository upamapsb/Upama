url <- "https://www.malawipublichealth.org/"

page <- read_html(url)

idx <- page %>%
    html_nodes('.elementor-counter-title') %>%
    html_text() %>%
    str_detect("Total Tests Conducted") %>%
    which()

count <- page %>%
    html_nodes('.elementor-counter-number') %>%
    html_attr("data-to-value") %>%
    magrittr::extract(idx) %>%
    as.integer()

add_snapshot(
    count = count,
    sheet_name = "Malawi",
    country = "Malawi",
    units = "samples tested",
    source_url = url,
    source_label = "Public Health Institute of Malawi",
    testing_type = "unclear"
)
