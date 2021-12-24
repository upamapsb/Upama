# Data on COVID-19 (coronavirus) hospitalizations and intensive care by _Our World in Data_

For more general information on our COVID-19 data, see our main README file in [`/public/data`](https://github.com/owid/covid-19-data/tree/master/public/data).

The data is collected from official sources and collated by Our World in Data. We make it available as a [CSV 💾](covid-hospitalizations.csv).


### Fields

| Column field        | Description                                                                  |
|---------------------|------------------------------------------------------------------------------|
| `entity`            | Name of the country (or region within a country).                            |
| `date`                | Date of the observation.                                                     |
| `iso_code`             | ISO corresponding to `entity` value. |
| `indicator`       | Indicator name. See below list of indicators and descriptions. |
| `value`      | Value of the `indicator`. |

#### Indicators
| Indicator name | Description |
|----------------|-------------|
| `Daily hospital occupancy`                      | Beds occupied due to COVID-19 cases. |
| `Daily hospital occupancy per million`          | `Daily hospital occupancy` per million people. |
| `Daily ICU occupancy`                           | _ICU_ Beds occupied due to COVID-19 cases. |
| `Daily ICU occupancy per million`               | `Daily ICU occupancy` per million people. |
| `Weekly new hospital admissions`                 | Weekly new hospital admissions for COVID-19 cases. |
| `Weekly new hospital admissions per million`     | `Weekly new hospital admissions` per million people. |
| `Weekly new ICU admissions`                      | Weekly new _ICU_ hospital admissions for COVID-19 cases. |
| `Weekly new ICU admissions per million`          | `Weekly new ICU admissions` per million people. |


## License

All visualizations, data, and code produced by _Our World in Data_ are completely open access under the [Creative Commons BY license](https://creativecommons.org/licenses/by/4.0/). You have the permission to use, distribute, and reproduce these in any medium, provided the source and authors are credited.
