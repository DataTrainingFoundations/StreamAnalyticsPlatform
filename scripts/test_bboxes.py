from datetime import datetime
from scripts.airnow_raw_producers import fetch_historic_data
from util import constants

start = "2026-01-01T00"
end = "2026-01-14T23"

output_file = "bbox_2_weeks_test_results.txt"

with open(output_file, "w", encoding="utf-8") as f:
    f.write(f"2 week pull test {start} -> {end}\n")
    f.write("=" * 80 + "\n\n")

    for i, bbox in enumerate(constants.BBOXES, start=1):
        try:
            print(f"Testing {i}/{len(constants.BBOXES)} : {bbox}")

            records = fetch_historic_data(start, end, bbox)    

            if isinstance(records, str):
                f.write(f"FAILED {bbox} -> returned string: {records[:500]}\n")
                continue

            if not isinstance(records, list):
                f.write(
                    f"FAILED {bbox} -> unexpected type {type(records).__name__}: {str(records)[:500]}\n"
                )
                continue

            unique_sites = {
                r["IntlAQSCode"]
                for r in records
                if isinstance(r, dict) and "IntlAQSCode" in r
            }

            f.write(
                f"SUCCESS {bbox} -> {len(unique_sites)} sites, {len(records)} rows\n"
            )

        except Exception as e:
            f.write(f"FAILED {bbox} -> {type(e).__name__}: {e}\n")

    print("Month test complete.")
    print(f"Results written to: {output_file}")
