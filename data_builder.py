import sys
from data.crawler import crawl_latest_trading_day
from database.mysql_adapter import upsert_kr_base, upsert_kr_sector, upsert_kr_code, upsert_kr_price, upsert_kr_fs, upsert_kr_value

def main():
    try:        
        mkt_day = crawl_latest_trading_day()
        upsert_kr_base(mkt_day)
        upsert_kr_sector(mkt_day)
        upsert_kr_code()
        upsert_kr_price()
        upsert_kr_fs()
        upsert_kr_value()
        print("Database update complete.")
    except Exception as e:
        print(f"An error occurred: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()