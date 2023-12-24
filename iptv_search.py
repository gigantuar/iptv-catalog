#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script Name: IPTV Stream Cataloger - Search Companion Script
Author: Gigantuar
Contact: me [at] gigantuar [dot] com
Repository URL: https://github.com/gigantuar/iptv-catalog
License: MIT License

Copyright (c) 2023 by Gigantuar

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""

import argparse
import glob
import os
import prettytable
import sys
from iptv_catalog import get_filename
from iptv_catalog import load_config
from iptv_catalog import merge_settings
from iptv_catalog import retrieve_live_ffprobe
from iptv_catalog import setup_database
from iptv_catalog import Stream


def fetch_data(args, db_filenames, search_table):
    # Split the channel_name into a list of names
    search_terms = args.channel_name.split()
    for db_file in db_filenames:
        conn = setup_database(db_file)

        sort = """ORDER BY f.height DESC,
                          s.name;"""

        results = retrieve_live_ffprobe(conn, names=search_terms, order=sort)

        streams = [Stream(data) for data in results]

        base_filename = os.path.basename(db_file)
        base_filename = os.path.splitext(base_filename)[0]
        if base_filename.startswith("portal_"):
            base_filename = base_filename.replace("portal_", "", 1)

        for stream in streams:
            stream_data = stream.get_display_data(search=True)
            if stream_data[4] != "" or args.include_failed:
                row_data = [base_filename, *stream_data]
                search_table.add_row(row_data)

        conn.close()

    return search_table


def perform_search(args):
    config = load_config(args.portal) if args.portal else None
    portal_name = args.portal if args.portal else None
    settings = merge_settings(args, config, portal_name)

    db_filenames = []
    if args.portal:
        db_filename = get_filename(settings["portal_url"], "db")
        if db_filename:
            db_filenames.append(db_filename)
        else:
            print(f"No configuration found for portal '{args.portal}'.")
            return
    elif args.db:
        db_filenames.append(args.db)
    elif args.all:
        db_filenames = glob.glob("*.db")

    if not db_filenames:
        print("No database files found.")
        return

    search_table = prettytable.PrettyTable(align="l")
    search_table.field_names = [
        "Database",
        "Category",
        "Ch Num",
        "Channel",
        "Catchup",
        "Standard",
        "FPS",
    ]

    full_table = fetch_data(args, db_filenames, search_table)

    if len(full_table._rows) > 0:
        print(full_table)
    else:
        print("No matches found.")


def main():
    parser = argparse.ArgumentParser(
        description="Search IPTV Cataloger databases for channel details."
    )
    parser.add_argument(
        "-c",
        "--channel-name",
        default=None,
        help="Text to search for in the channel names, split by spaces.",
    )
    parser.add_argument(
        "-p",
        "--portal",
        help="Specify a portal name to search from the portals.ini config.",
    )
    parser.add_argument(
        "-d", "--db", help="Specify the local SQLite database filename directly."
    )
    parser.add_argument(
        "-a",
        "--all",
        action="store_true",
        help="Search through all *.db files in the script folder.",
    )
    parser.add_argument(
        "-if",
        "--include-failed",
        action="store_true",
        help="Include channels which failed ffprobe processing in results.",
    )

    args = parser.parse_args()

    # Check if any database selection argument is provided
    if not any([args.portal, args.db, args.all]):
        print(
            "Error: No database specified.\n"
            "Please provide a database using --portal, --db, or --all."
        )
        sys.exit(1)

    try:
        if args.channel_name:
            perform_search(args)
        else:
            # Loop for continuous search
            while True:
                args.channel_name = input(
                    "Enter terms to search for in the channel name (split by spaces): "
                )
                perform_search(args)

                cont = input("Search again? (y/n): ").lower()
                if cont != "y":
                    break
                args.channel_name = None  # Reset channel name for new search
    except KeyboardInterrupt:
        print("\nExiting program.")
        sys.exit(0)


if __name__ == "__main__":
    main()
