#!/usr/bin/python

import csv
import argparse
import os

def bind_csv_files(main_file, loc_file, output_file):
    # Read the main file
    with open(main_file, "r") as main_csv:
        main_reader = csv.DictReader(main_csv)
        main_data = list(main_reader)

    # Read the loc file
    with open(loc_file, "r") as loc_csv:
        loc_reader = csv.reader(loc_csv)
        loc_data = list(loc_reader)

    # Bind the data based on matching substrings
    matched_data = []
    unmatched_subjects = set()
    for row in main_data:
        subject = row["Subject"].strip()
        matching_location = None
        for loc_row in loc_data:
            location = loc_row[0].strip()
            if subject in location:
                matching_location = location
                break
        if matching_location:
            row["location"] = matching_location
            matched_data.append(row)
        else:
            unmatched_subjects.add(subject)

    # Write the matched data to the output file
    with open(output_file, "w", newline="") as output_csv:
        fieldnames = main_reader.fieldnames + ["location"]
        writer = csv.DictWriter(output_csv, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(matched_data)

    # Write the unmatched subjects to a separate CSV file
    unmatched_file = os.path.splitext(output_file)[0] + "_not_found.csv"
    with open(unmatched_file, "w", newline="") as unmatched_csv:
        writer = csv.writer(unmatched_csv)
        writer.writerow(["Subject"])
        writer.writerows([[subject] for subject in unmatched_subjects])

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Bind Niagads sample info file with location file")
    parser.add_argument("--main_file", dest="main_file", help="Path to the main CSV file")
    parser.add_argument("--loc_file", dest="loc_file", help="Path to the location CSV file")
    parser.add_argument("--output_file", dest="output_file", help="Path to the output CSV file")
    args = parser.parse_args()
    bind_csv_files(args.main_file, args.loc_file, args.output_file)
