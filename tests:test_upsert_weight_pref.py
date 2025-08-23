import sqlite3
import sys
import types


def test_duplicate_insert_after_update_skipped(capsys, tmp_path):
    # Stub external dependencies required for module import
    sys.modules.setdefault("pandas", types.ModuleType("pandas"))
    bs4 = types.ModuleType("bs4")
    bs4.BeautifulSoup = object
    bs4.UnicodeDammit = object
    sys.modules["bs4"] = bs4

    selenium = types.ModuleType("selenium")
    webdriver = types.ModuleType("selenium.webdriver")
    chrome = types.ModuleType("selenium.webdriver.chrome")
    service = types.ModuleType("selenium.webdriver.chrome.service")
    webdriver.Chrome = object
    service.Service = object
    webdriver.chrome = chrome
    chrome.service = service
    selenium.webdriver = webdriver
    sys.modules["selenium"] = selenium
    sys.modules["selenium.webdriver"] = webdriver
    sys.modules["selenium.webdriver.chrome"] = chrome
    sys.modules["selenium.webdriver.chrome.service"] = service

    sys.modules.setdefault("ftfy", types.ModuleType("ftfy"))

    import _horse_dynamic_stats_special as hw

    # Setup temporary database with additional unique constraint
    db_file = tmp_path / "test.db"
    hw.DB_PATH = str(db_file)

    conn = sqlite3.connect(db_file)
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE horse_weight_pref (
            HorseID TEXT,
            Season TEXT,
            DistanceGroup TEXT,
            WeightGroup TEXT,
            CarriedWeight REAL,
            Top3Rate REAL,
            Top3Count INTEGER,
            TotalRuns INTEGER,
            LastUpdate TEXT,
            PRIMARY KEY (HorseID, Season, DistanceGroup, WeightGroup),
            UNIQUE (HorseID, Season, DistanceGroup)
        )
        """
    )
    cur.execute(
        """
        INSERT INTO horse_weight_pref VALUES (
            'H1','2024','D1','W1',120.0,0.5,1,2,'2024/01/01 00:00'
        )
        """
    )
    conn.commit()
    conn.close()

    # Attempt to upsert a record that triggers duplicate constraint after update
    row = {
        'Season': '2024',
        'DistanceGroup': 'D1',
        'WeightGroup': 'W2',
        'CarriedWeight': 130,
        'Top3Count': 1,
        'TotalRuns': 1,
    }
    hw.upsert_weight_pref('H1', [row])

    captured = capsys.readouterr()
    assert 'WARNING' in captured.out
    assert 'Duplicate weight_pref record skipped' in captured.out

    conn = sqlite3.connect(db_file)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM horse_weight_pref")
    assert cur.fetchone()[0] == 1
    conn.close()