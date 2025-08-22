# HKJC-Project-Special

Tools for scraping dynamic statistics from the Hong Kong Jockey Club.

## Usage

1. Create a CSV file named `horse_ids_to_update.csv` with a column `HorseID`.
   Each row should contain either a local horse number (e.g. `H123`) or an
   overseas identifier with a year prefix (e.g. `HK_2016_MAGIC` or `2016_XYZ`).
2. Run the scraper:

   ```bash
   python _scrape_horses_dynamic_data_special2.py
   ```

   The script automatically chooses the correct HKJC page:

   - Local horses → `Horse.aspx` (e.g. `https://racing.hkjc.com/racing/information/English/Horse/Horse.aspx?HorseNo=H123`)
   - Visiting horses → `OtherHorse.aspx` (e.g. `https://racing.hkjc.com/racing/information/English/Horse/OtherHorse.aspx?HorseId=HK_2016_MAGIC`)

   All results are written to `hkjc_horses_dynamic_special.db`.
