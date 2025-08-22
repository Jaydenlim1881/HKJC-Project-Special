# HKJC-Project-Special

Tools for scraping dynamic statistics from the Hong Kong Jockey Club.

## Prerequisites

- Python 3.8+
- Google Chrome and a matching [ChromeDriver](https://chromedriver.chromium.org/)
  placed at `./chromedriver`
- Python packages: `selenium`, `pandas`, `beautifulsoup4`, and `requests`
  (install with `pip install selenium pandas beautifulsoup4 requests`)
  
## Usage

1. Create a CSV file named `horse_ids_to_update.csv` with a column `HorseID`.
   Each row should contain either a local horse number (e.g. `H123`) or an
   overseas identifier with a year prefix (e.g. `HK_2016_MAGIC` or `2016_XYZ`).
2. Run the scraper:

   ```bash
   python _scrape_horses_dynamic_data_special2.py
   ```

   The script reads each horse ID and chooses the correct HKJC page:

   - Local horses → `Horse.aspx`
     (e.g. `https://racing.hkjc.com/racing/information/English/Horse/Horse.aspx?HorseNo=H123`)
   - Visiting horses → `OtherHorse.aspx`
     (e.g. `https://racing.hkjc.com/racing/information/English/Horse/OtherHorse.aspx?HorseId=HK_2016_MAGIC`)

   ## Output database)

All results are written to `hkjc_horses_dynamic_special.db`, an SQLite
database that stores the scraped dynamic statistics.  It acts as a cache and
can be queried or reused by other scripts for further analysis.
