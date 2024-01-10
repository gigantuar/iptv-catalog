#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script Name: IPTV Stream Cataloger
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
import configparser
import csv
import json
import logging
import os
import re
import signal
import sqlite3
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from threading import Event
from urllib.parse import urlparse

# third-party imports
import requests
import prettytable
from tqdm import tqdm

__version__ = "1.0.2"

# Global variables
db_filename = None

# Default refresh of data 24-hours
REFRESH_TIME = 86400
PORTAL_CONFIG = "portals.ini"


class Stream:
    """Stream data processing for CLI and CSV output."""

    def __init__(self, stream_data):
        self.category_id = stream_data[0]
        self.category_name = stream_data[1]
        self.stream_id = stream_data[2]
        self.channel_number = stream_data[3]
        self.channel_name = stream_data[4]
        self.tv_archive = stream_data[5]
        self.codec_name = stream_data[6]
        self.width = stream_data[7]
        self.height = stream_data[8]
        self.avg_frame_rate = stream_data[9]
        self.profile = stream_data[10]
        self.field_order = stream_data[11]
        self.last_updated = stream_data[12]

    def get_display_data(self, simple=False, failed=False, csv=False, search=False):
        category_name = self._clean_and_truncate(self.category_name, 30)
        channel_name = self._clean_and_truncate(self.channel_name, 50)
        catchup = "true" if self.tv_archive == 1 else "false"
        resolution_full = (
            f"{self.width}x{self.height}" if self.width and self.height else ""
        )
        resolution = self._classify_resolution(self.height) if self.height else ""
        fps = (
            round(self._convert_fraction_to_decimal(self.avg_frame_rate))
            if self.avg_frame_rate
            else ""
        )
        field_order = self._get_field_order()
        formatted_timestamp = (
            datetime.fromisoformat(self.last_updated).strftime("%Y-%m-%d %H:%M:%S")
            if self.last_updated
            else ""
        )

        if csv:
            return [
                self.category_id,
                category_name,
                self.stream_id,
                self.channel_number,
                channel_name,
                catchup,
                self.codec_name,
                self.profile,
                self.width,
                self.height,
                resolution_full,
                resolution,
                fps,
                field_order,
                formatted_timestamp,
            ]
        elif search:
            return [
                category_name,
                self.channel_number,
                channel_name,
                catchup,
                resolution,
                fps,
            ]
        elif failed:
            return [category_name, self.stream_id, self.channel_number, channel_name]
        elif simple:
            return [category_name, channel_name, resolution, fps]
        else:
            return [
                self.category_id,
                category_name,
                self.stream_id,
                self.channel_number,
                channel_name,
                catchup,
                self.codec_name,
                resolution_full,
                resolution,
                fps,
            ]

    def _classify_resolution(self, height):
        """Map stream height to standard resolutions."""
        resolution_mapping = {4320: "UHD-8K", 2160: "UHD-4K", 1080: "FHD", 720: "HD"}
        return next(
            (
                label
                for res, label in sorted(resolution_mapping.items(), reverse=True)
                if height >= res
            ),
            "SD",
        )

    def _clean_and_truncate(self, text, max_length):
        if text is None:
            return ""

        # Remove non-printable characters
        text = re.sub(r"[\x00-\x1F\x7F]+", "", text)

        # Trim whitespace and truncate
        text = text.strip()
        return text[:max_length] + ".." if len(text) > max_length else text

    def _convert_fraction_to_decimal(self, fraction):
        """Convert fractional frames per second into decimal."""
        try:
            numerator, denominator = map(int, fraction.split("/"))
            return numerator / denominator if denominator else 0
        except ValueError:
            return 0

    def _get_field_order(self):
        """Return interlaced from ffprobe field_order"""
        if self.field_order in ["tt", "bb", "tb", "bt"]:
            field_order = "interlaced"
        else:
            field_order = self.field_order
        return field_order


def signal_handler(shutdown_event):
    """Handle Ctrl+C for graceful shutdown."""

    def handler(signum, frame):
        print("\nTermination signal received. Exiting script after current task...\n")
        sys.exit()

    return handler


def configure_logging(verbose, debug):
    """Configure logging level"""
    if debug:
        level = logging.DEBUG
    elif verbose:
        level = logging.INFO
    else:
        level = logging.WARNING
    logging.basicConfig(level=level, format="%(levelname)s: %(message)s")


def check_ffprobe():
    """Ensure ffprobe is able to execute"""
    try:
        subprocess.run(
            ["ffprobe", "-version"],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        return True
    except (subprocess.CalledProcessError, FileNotFoundError):
        return False


def get_filename(portal_url, file_type):
    """Return the database or CSV filename derived from the portal name."""
    if not portal_url:
        logging.critical("Invalid Portal URL.")
        sys.exit(1)
    parsed_url = urlparse(portal_url)
    portal_name = (
        f"{parsed_url.hostname}-{parsed_url.port}"
        if parsed_url.port
        else parsed_url.hostname
    )

    # Get the directory of the script
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # Construct the full path for the file
    if file_type == "db":
        filename = f"portal_{portal_name}.db"
    elif file_type == "csv":
        filename = f"portal_{portal_name}.csv"
    else:
        logging.critical("Invalid filename type specified.")
    return os.path.join(script_dir, filename)


def load_config(portal_name, config_file=PORTAL_CONFIG):
    config = configparser.ConfigParser()
    config.read(config_file)
    return config if config.has_section(portal_name) else None


def merge_settings(cli_args, config, portal_name):
    """Merge settings from the config file with CLI arguments."""
    settings = {}

    boolean_settings = [
        "force_refresh",
        "probe_failed",
        "force_probe",
        "no_probe",
        "offline",
        "quiet",
        "simple",
    ]
    integer_settings = ["delay", "connections"]
    string_settings = [
        "portal_url",
        "username",
        "password",
        "categories",
        "categories_exclude",
        "user_agent",
    ]
    all_settings = string_settings + boolean_settings + integer_settings

    for setting in all_settings:
        cli_value = getattr(cli_args, setting, None)

        if setting in boolean_settings:
            # For 'store_true' arguments, first check CLI, then config
            if cli_value is not False:
                settings[setting] = cli_value
            elif config and config.has_option(portal_name, setting):
                settings[setting] = config.getboolean(
                    portal_name, setting, fallback=False
                )
            else:
                settings[setting] = False
        elif setting in integer_settings:
            # Handle integer arguments
            if cli_value is not None:
                settings[setting] = cli_value
            elif config and config.has_option(portal_name, setting):
                try:
                    settings[setting] = config.getint(portal_name, setting)
                except ValueError:
                    raise ValueError(
                        f"Invalid integer value for '{setting}' in config file."
                    )
            else:
                settings[setting] = 0
        else:
            # Handle other non-boolean, non-integer arguments
            if cli_value is not None:
                settings[setting] = cli_value
            elif config and config.has_option(portal_name, setting):
                settings[setting] = config.get(portal_name, setting)
            else:
                settings[setting] = None

    settings["portal_name"] = portal_name

    return settings


def get_user_agent(user_agent_choice):
    """
    Return the appropriate User-Agent string based on user selection.
    Default would be similar to: python-requests/X.X.X, Lavf/X.X.X
    """
    user_agents = {
        "vlc": "VLC/3.0.8 LibVLC/3.0.8",
        "mx": "MXPlayer/1.10.47",
        "smarters": "IPTVSmartersPlayer/3.0",
        "tivimax": "TiviMaxMobilePremium/30809.2.1 CFNetwork/1485 Darwin/23.1.0",
    }
    return user_agents.get(user_agent_choice, None)


def test_api(settings):
    """Test the Xtream compatible API."""
    headers = {"User-Agent": settings["user_agent"]} if settings["user_agent"] else {}
    params = {
        "username": settings["username"],
        "password": settings["password"],
    }
    try:
        response = requests.get(
            f"{settings['portal_url']}/player_api.php",
            params=params,
            headers=headers,
        )
        if response.ok:
            return True
        else:
            return response
    except requests.RequestException as e:
        logging.critical(f"Request failed: {e}")
        sys.exit(1)


def query_api(settings, action, portal_info=False):
    """Query the Xtream compatible API."""
    headers = {"User-Agent": settings["user_agent"]} if settings["user_agent"] else {}
    params = {
        "username": settings["username"],
        "password": settings["password"],
        "action": action,
    }
    try:
        if portal_info:
            response = requests.get(
                f"{settings['portal_url']}/player_api.php",
                params={
                    "username": settings["username"],
                    "password": settings["password"],
                },
                headers=headers,
            )
        else:
            response = requests.get(
                f"{settings['portal_url']}/player_api.php",
                params=params,
                headers=headers,
            )
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logging.critical(f"Request failed: {e}")
        return None


def fetch_categories(settings):
    """Query channel category groups from portal."""
    categories = query_api(settings, "get_live_categories")
    if categories:
        logging.info(f"Retrieved {len(categories)} categories.")
        return categories
    else:
        logging.info("No categories retrieved.")
        return None


def fetch_streams(settings):
    """Query channel streams from portal."""
    streams = query_api(settings, "get_live_streams")
    if streams:
        logging.info(f"Retrieved {len(streams)} streams.")
        return streams
    else:
        logging.info("No streams retrieved.")
        return None


def adapt_datetime(dt):
    """
    Convert a datetime object to a string.
    """
    return dt.isoformat()


def convert_datetime(s):
    """
    Convert a string to a datetime object.
    """
    return datetime.fromisoformat(s)


# Register the new adapter and converter
sqlite3.register_adapter(datetime, adapt_datetime)
sqlite3.register_converter("datetime", convert_datetime)


def execute_sql(conn, sql, data=None, fetch=False):
    """
    Execute a SQL query using the provided SQL statement and data.
    Optionally fetch and return the results of the query.
    """
    try:
        cursor = conn.cursor()
        if data:
            cursor.execute(sql, data)
        else:
            cursor.execute(sql)

        if fetch:
            result = cursor.fetchall()
        else:
            result = None

        conn.commit()
        return result

    except sqlite3.Error as e:
        logging.critical(f"An error occurred: {e}")
        conn.rollback()
        raise
    finally:
        cursor.close()


def setup_database(db_filename):
    """Setup shared SQLite database connection and handle table creation."""
    with sqlite3.connect(db_filename) as conn:
        table_sql = {
            "refresh": """CREATE TABLE IF NOT EXISTS refresh (
                                        synctable TEXT PRIMARY KEY,
                                        last_updated TIMESTAMP
                                    );""",
            "live_streams": """CREATE TABLE IF NOT EXISTS live_streams (
                                        stream_id INTEGER PRIMARY KEY,
                                        num INTEGER,
                                        name TEXT,
                                        stream_type TEXT,
                                        stream_icon TEXT,
                                        epg_channel_id TEXT,
                                        is_adult INTEGER,
                                        added TIMESTAMP,
                                        custom_sid TEXT,
                                        tv_archive INTEGER,
                                        direct_source TEXT,
                                        tv_archive_duration INTEGER,
                                        category_id TEXT,
                                        thumbnail TEXT,
                                        category_ids TEXT,
                                        last_updated TIMESTAMP,
                                        FOREIGN KEY(category_id)
                                        REFERENCES live_categories(category_id)
                                    );""",
            "live_categories:": """CREATE TABLE IF NOT EXISTS live_categories (
                                        category_id TEXT PRIMARY KEY,
                                        category_name TEXT,
                                        parent_id INTEGER,
                                        last_updated TIMESTAMP
                                    );""",
            "live_ffprobe": """CREATE TABLE IF NOT EXISTS live_ffprobe (
                                        stream_id INTEGER PRIMARY KEY,
                                        codec_name TEXT,
                                        codec_long_name TEXT,
                                        profile TEXT,
                                        codec_type TEXT,
                                        width INTEGER,
                                        height INTEGER,
                                        coded_width INTEGER,
                                        coded_height INTEGER,
                                        closed_captions INTEGER,
                                        film_grain INTEGER,
                                        has_b_frames INTEGER,
                                        sample_aspect_ratio TEXT,
                                        display_aspect_ratio TEXT,
                                        pix_fmt TEXT,
                                        level INTEGER,
                                        color_range TEXT,
                                        color_space TEXT,
                                        color_transfer TEXT,
                                        color_primaries TEXT,
                                        chroma_location TEXT,
                                        field_order TEXT,
                                        refs INTEGER,
                                        is_avc TEXT,
                                        nal_length_size TEXT,
                                        id TEXT,
                                        r_frame_rate TEXT,
                                        avg_frame_rate TEXT,
                                        time_base TEXT,
                                        bits_per_raw_sample TEXT,
                                        extradata_size INTEGER,
                                        raw_json TEXT,
                                        last_updated TIMESTAMP,
                                        FOREIGN KEY(stream_id)
                                        REFERENCES live_streams(stream_id)
                                    );""",
        }
        for _table_name, sql in table_sql.items():
            execute_sql(conn, sql)

    return conn


def get_value_or_none(data, key):
    """Helper function to get a value or None for a given key."""
    value = data.get(key)
    if isinstance(value, list):
        return ",".join(map(str, value)) if value else None
    return value if value or value == 0 else None


def store_refresh(synctable, conn):
    """Store last_updated time in the database refresh table."""
    data_tuple = (synctable, datetime.now())
    sql = "INSERT OR REPLACE INTO refresh (synctable, last_updated) VALUES (?, ?)"
    execute_sql(conn, sql, data_tuple)


def retrieve_refresh(synctable, conn):
    """Retrieve the last_updated time for a given synctable from the database."""
    sql = "SELECT last_updated FROM refresh WHERE synctable = ?"
    result = execute_sql(conn, sql, (synctable,), fetch=True)
    return result[0][0] if result else None


def retrieve_live_ffprobe(conn, categories=None, failed=False, names=None, order=False):
    """Retrieve live_streams ffprobe data from the database."""
    sql = """
        SELECT c.category_id, c.category_name,
               s.stream_id, s.num, s.name, s.tv_archive,
               f.codec_name, f.width, f.height, f.avg_frame_rate,
               f.profile, f.field_order, f.last_updated
        FROM live_streams s
        JOIN live_categories c ON s.category_id = c.category_id
        JOIN live_ffprobe f ON s.stream_id = f.stream_id
    """

    if failed:
        sql += " WHERE f.raw_json is NULL"
    elif categories:
        sql += f" WHERE s.category_id IN ({categories})"
    elif names:
        name_search = [f"s.name LIKE '%{name}%'" for name in names]
        sql += " WHERE " + " AND ".join(name_search)

    if order:
        sql += f" {order}"
    else:
        sql += " ORDER BY s.num"

    return execute_sql(conn, sql, fetch=True)


def store_categories(categories, conn, quiet):
    """Sync categories with the database"""

    def _compare_fields_category(old_data, new_data):
        """Helper function for comparing category data and formatting output."""
        fields = ["category_name", "parent_id"]
        diff_output = []

        for i, field in enumerate(fields):
            if old_data[i] != new_data[i]:
                diff_output.append(
                    f"{field}:\n\t- old: {old_data[i]}\n\t- new: {new_data[i]}"
                )

        return "\n".join(diff_output)

    sql_select = (
        "SELECT category_name, parent_id FROM live_categories WHERE category_id = ?"
    )
    sql_insert = """REPLACE INTO live_categories
                     (category_id, category_name, parent_id, last_updated)
                     VALUES (?, ?, ?, ?)"""
    sql_delete = "DELETE FROM live_categories WHERE category_id = ?"

    # Fetch existing category IDs from the database
    existing_category_ids = set(
        id[0]
        for id in execute_sql(
            conn, "SELECT category_id FROM live_categories", fetch=True
        )
    )

    for category in categories:
        category_id = category.get("category_id")
        if category_id is None:
            logging.info("category_id is missing, record will be skipped")
            continue

        existing_category_ids.discard(
            category_id
        )  # Remove existing IDs as we encounter them

        existing_category = execute_sql(conn, sql_select, (category_id,), fetch=True)
        new_data = (category.get("category_name"), category.get("parent_id"))

        if existing_category:
            if existing_category[0] != new_data:
                changes = _compare_fields_category(existing_category[0], new_data)
                if changes:
                    print(f"Updating category_id {category_id}:\n{changes}\n")
                execute_sql(
                    conn,
                    sql_insert,
                    (category_id,) + new_data + (datetime.now(),),
                )
            else:
                continue
        else:
            category_name = new_data[0]
            if not quiet:
                print(
                    "Inserting new category:\n"
                    f"\t- category_id: {category_id}\n"
                    f"\t- category_name: {category_name}\n"
                )
            execute_sql(
                conn,
                sql_insert,
                (category_id,) + new_data + (datetime.now(),),
            )

    # Delete categories that are no longer in the source
    for category_id in existing_category_ids:
        # Fetch the category name before deleting
        existing_category = execute_sql(conn, sql_select, (category_id,), fetch=True)
        category_name = existing_category[0][0] if existing_category else "Unknown"

        print(
            "Deleting category:\n"
            f"\t- category_id: {category_id}\n"
            f"\t- category_name: {category_name}\n"
        )
        execute_sql(conn, sql_delete, (category_id,))


def store_streams(streams_list, conn, quiet):
    """Sync streams with the database"""

    def _compare_fields_stream(old_data, new_data, field_names):
        """Helper function for comparing stream data and formatting output."""
        diff_output = []
        for i, field in enumerate(field_names):
            if old_data[i] != new_data[i]:
                diff_output.append(
                    f"{field}:\n\t- old: {old_data[i]}\n\t- new: {new_data[i]}"
                )
        return "\n".join(diff_output)

    keys = [
        "stream_id",
        "num",
        "name",
        "stream_type",
        "stream_icon",
        "epg_channel_id",
        "is_adult",
        "added",
        "custom_sid",
        "tv_archive",
        "direct_source",
        "tv_archive_duration",
        "category_id",
        "thumbnail",
        "category_ids",
    ]

    # Exclude 'added' from comparison keys
    comparison_keys = [key for key in keys if key != "added"]

    sql_insert = f"""REPLACE INTO live_streams
                     VALUES ({", ".join(["?"] * (len(keys) + 1))})"""
    sql_select = "SELECT " + ", ".join(keys) + " FROM live_streams WHERE stream_id = ?"
    sql_delete_ffprobe = "DELETE FROM live_ffprobe WHERE stream_id = ?"
    sql_delete_stream = "DELETE FROM live_streams WHERE stream_id = ?"

    existing_stream_ids = set(
        id[0]
        for id in execute_sql(conn, "SELECT stream_id FROM live_streams", fetch=True)
    )

    for stream in streams_list:
        stream_id = stream["stream_id"]
        existing_stream_ids.discard(stream_id)

        existing_data = execute_sql(conn, sql_select, (stream_id,), fetch=True)

        if existing_data:
            new_data = tuple(get_value_or_none(stream, key) for key in comparison_keys)
            existing_data_comparison = tuple(
                existing_data[0][i] for i, key in enumerate(keys) if key != "added"
            )

            if new_data != existing_data_comparison:
                changes = _compare_fields_stream(
                    existing_data_comparison, new_data, comparison_keys
                )

                data = [get_value_or_none(stream, key) for key in keys]
                data.append(datetime.now())
                data = tuple(data)

                # Delete ffprobe data if channel name changes
                if "name" in changes:
                    print(f"Updating stream_id {stream_id}:\n{changes}\n")
                    execute_sql(conn, sql_delete_ffprobe, (stream_id,))

                execute_sql(conn, sql_insert, data)
            else:
                continue
        else:
            data = [get_value_or_none(stream, key) for key in keys]
            data.append(datetime.now())
            data = tuple(data)

            if not quiet:
                print(
                    "Inserting new stream:\n"
                    f"\t- stream_id: {stream_id}\n"
                    f"\t- name: {stream.get('name')}\n"
                )
            execute_sql(conn, sql_delete_ffprobe, (stream_id,))
            execute_sql(conn, sql_insert, data)

    for stream_id in existing_stream_ids:
        existing_stream = execute_sql(
            conn,
            "SELECT name FROM live_streams WHERE stream_id = ?",
            (stream_id,),
            fetch=True,
        )
        stream_name = existing_stream[0][0] if existing_stream else "Unknown"
        print(
            f"Deleting stream:\n\t- stream_id: {stream_id}\n\t- name: {stream_name}\n"
        )
        execute_sql(conn, sql_delete_stream, (stream_id,))
        execute_sql(conn, sql_delete_ffprobe, (stream_id,))


def store_ffprobe_results(stream_id, ffprobe_data, conn, no_probe=False):
    """Store ffprobe data in the database, including handling ffprobe failures."""
    # Check if ffprobe_data is valid JSON; if not, set to a compact format or None
    compact_ffprobe_data = None
    if ffprobe_data:
        try:
            # Parse and re-serialize into a compact JSON string
            parsed_data = json.loads(ffprobe_data)
            compact_ffprobe_data = json.dumps(parsed_data, separators=(",", ":"))
        except json.JSONDecodeError:
            compact_ffprobe_data = None

    # Define stream_info with default values if ffprobe failed
    stream_info = {}
    if compact_ffprobe_data:
        try:
            stream_info = json.loads(compact_ffprobe_data).get("streams", [{}])[0]
        except (KeyError, IndexError, json.JSONDecodeError):
            stream_info = {}

    # Populate raw_json with "no_probe" if probing is disabled
    if no_probe is True:
        compact_ffprobe_data = "no_probe"

    keys = (
        "codec_name",
        "codec_long_name",
        "profile",
        "codec_type",
        "width",
        "height",
        "coded_width",
        "coded_height",
        "closed_captions",
        "film_grain",
        "has_b_frames",
        "sample_aspect_ratio",
        "display_aspect_ratio",
        "pix_fmt",
        "level",
        "color_range",
        "color_space",
        "color_transfer",
        "color_primaries",
        "chroma_location",
        "field_order",
        "refs",
        "is_avc",
        "nal_length_size",
        "id",
        "r_frame_rate",
        "avg_frame_rate",
        "time_base",
        "bits_per_raw_sample",
        "extradata_size",
    )

    # Prepare data tuple
    data_tuple = (
        (stream_id,)
        + tuple(stream_info.get(key, None) for key in keys)
        + (compact_ffprobe_data, datetime.now())
    )

    # Generate placeholders
    placeholders = ", ".join(["?"] * len(data_tuple))

    # Insert or replace the ffprobe results in the database
    sql = f"INSERT OR REPLACE INTO live_ffprobe VALUES ({placeholders})"
    execute_sql(conn, sql, data_tuple)


def run_ffprobe(settings, stream_id):
    """Execute ffprobe and return the JSON data."""
    ffprobe_command = [
        "ffprobe",
        "-hide_banner",
        "-show_streams",
        "-select_streams",
        "v",
        "-of",
        "json=c=1",
        "-v",
        "quiet",
        f"{settings['portal_url']}/{settings['username']}/{settings['password']}/{stream_id}",
    ]
    if settings["user_agent"]:
        ffprobe_command.extend(["-user_agent", settings["user_agent"]])
    try:
        ffprobe_output = subprocess.run(
            ffprobe_command, capture_output=True, text=True, check=True
        )
        time.sleep(settings["delay"])
        return ffprobe_output.stdout
    except subprocess.CalledProcessError as e:
        logging.info(f"ffprobe command failed for stream_id {stream_id}: {e}")
        time.sleep(settings["delay"])
        return None
    except Exception as e:
        logging.info(f"Unexpected error during ffprobe for stream_id {stream_id}: {e}")
        return None


def process_categories(settings, conn):
    """
    Initiate category fetch if data is older than 24 hours, or forced by the user.
    Initiate storage of categories into the database, and update the refresh time.
    """
    if settings["force_refresh"]:
        logging.info("Force refresh enabled for live stream categories.")
        time_since = float("inf")
    else:
        last_updated = retrieve_refresh("live_categories", conn)
        if last_updated is None:
            time_since = float("inf")
        else:
            if isinstance(last_updated, str):
                last_updated = datetime.fromisoformat(last_updated)
            time_since = (datetime.now() - last_updated).total_seconds()

    if time_since < REFRESH_TIME:
        print(
            "Live stream categories were last refreshed less than 24 hours "
            f"ago ({round(time_since / 60 / 60)} hours). Sync skipped."
        )
        return

    logging.info("Fetching live stream categories...")
    categories = fetch_categories(settings)
    if categories:
        logging.info("Storing live stream categories in database...")
        store_categories(categories, conn, settings["quiet"])
        store_refresh("live_categories", conn)


def process_streams(settings, conn):
    """
    Initiate streams fetch if data is older than 24 hours, or forced by the user.
    Initiate storage of streams into the database, and update the refresh time.
    """
    if settings["force_refresh"]:
        logging.info("Force refresh enabled for live streams.")
        time_since = float("inf")
    else:
        last_updated = retrieve_refresh("live_streams", conn)
        if last_updated is None:
            time_since = float("inf")
        else:
            if isinstance(last_updated, str):
                last_updated = datetime.fromisoformat(last_updated)
            time_since = (datetime.now() - last_updated).total_seconds()

    if time_since < REFRESH_TIME:
        print(
            "Live streams were last refreshed less than 24 hours "
            f"ago ({round(time_since / 60 / 60)} hours). Sync skipped."
        )
        return

    logging.info("Fetching live streams...")
    streams = fetch_streams(settings)
    if streams:
        logging.info("Storing live streams in database...")
        store_streams(streams, conn, settings["quiet"])
        store_refresh("live_streams", conn)


def process_ffprobe(settings, conn):
    """
    Initiate ffprobe threads based on user defined connections (default 1).
    If defined by the user, streams will be filtered on category_id.
    Allows for user override to re-check failed streams, or all streams.
    """
    # Building the SQL query based on categories included/excluded
    if settings["categories"]:
        # Split the categories string on commas and strip whitespace
        category_list = [cat.strip() for cat in settings["categories"].split(",")]
        placeholders = ",".join("?" for _ in category_list)
        sql_query = (
            f"SELECT stream_id FROM live_streams WHERE category_id IN ({placeholders})"
        )
        stream_ids = execute_sql(conn, sql_query, category_list, fetch=True)
    elif settings["categories_exclude"]:
        category_exclude_list = [
            cat.strip() for cat in settings["categories_exclude"].split(",")
        ]
        placeholders = ",".join("?" for _ in category_exclude_list)
        sql_query = f"""SELECT stream_id FROM live_streams
                        WHERE NOT category_id IN ({placeholders})"""
        stream_ids = execute_sql(conn, sql_query, category_exclude_list, fetch=True)
    else:
        sql_query = "SELECT stream_id FROM live_streams"
        stream_ids = execute_sql(conn, sql_query, fetch=True)

    # Filter out stream_ids that already have ffprobe data
    stream_ids_to_process = [
        stream_id[0]
        for stream_id in stream_ids
        if not execute_sql(
            conn,
            "SELECT stream_id FROM live_ffprobe WHERE stream_id = ?",
            (stream_id[0],),
            fetch=True,
        )
    ]

    # Append streams which previously had probing skipped
    skipped_stream_ids = execute_sql(
        conn,
        "SELECT stream_id FROM live_ffprobe WHERE raw_json LIKE 'no_probe'",
        fetch=True,
    )
    # Convert tuple to list
    skipped_stream_ids = [stream_id[0] for stream_id in skipped_stream_ids]
    # Add skipped stream_ids existing list
    stream_ids_to_process.extend(skipped_stream_ids)

    # If probe_failed is True, append failed stream_ids for processing
    if settings["probe_failed"]:
        # Fetch stream_ids missing JSON data
        failed_stream_ids = execute_sql(
            conn,
            "SELECT stream_id FROM live_ffprobe WHERE raw_json IS NULL",
            fetch=True,
        )
        # Convert tuple to list
        failed_stream_ids = [stream_id[0] for stream_id in failed_stream_ids]
        # Add failed stream_ids existing list
        stream_ids_to_process.extend(failed_stream_ids)

    # Remove duplicates from stream_ids_to_process
    stream_ids_to_process = list(set(stream_ids_to_process))

    # If -fp/--force-probe set all stream_ids for processing
    if settings["force_probe"]:
        stream_ids_to_process = [stream_id[0] for stream_id in stream_ids]

    # Check if there are stream_ids to process
    if not stream_ids_to_process:
        logging.info("No streams require ffprobe processing.")
        return

    # Run ffprobe with a dedicated database connection for threading
    def run_ffprobe_thread(stream_id):
        global db_filename
        conn = sqlite3.connect(db_filename)
        try:
            logging.info(f"Running ffprobe for stream_id: {stream_id}")
            if not settings["no_probe"]:
                ffprobe_data = run_ffprobe(settings, stream_id)
            else:
                ffprobe_data = None
            store_ffprobe_results(stream_id, ffprobe_data, conn, settings["no_probe"])
        finally:
            conn.close()

    # Initiate ffprobe threads based on the number of connections defined by the user
    if not settings["no_probe"]:
        print(f"Initiating ffprobe with {settings['connections']} connections.")
        logging.info(f"Using User-Agent: {settings['user_agent']}")
    else:
        print("Probing with ffprobe is disabled. Populating table with empty data.")
    with ThreadPoolExecutor(max_workers=settings["connections"]) as executor:
        # Use tdqm for progress bar tracking.
        # Fixed width to 80 columns to allow resizing the console.
        list(
            tqdm(
                executor.map(run_ffprobe_thread, stream_ids_to_process),
                total=len(stream_ids_to_process),
                ncols=80,
                desc="Running ffprobe",
                unit="stream",
            )
        )


def process_portal(settings, conn):
    """
    Process actions specific to the portal based on the
    provided arguments and portal details.
    """
    # Perform API queries and probes if not explicitly offline
    if not settings["offline"]:
        process_categories(settings, conn)
        process_streams(settings, conn)
        process_ffprobe(settings, conn)
    else:
        logging.warning(
            f"Portal '{settings['portal_name']}' configured for offline processing"
            " only. Processing from local database."
        )


def print_config(portal_name, settings):
    """Print the configuration based on the portal specified, and CLI args."""
    print(f"Portal: {portal_name}")
    for setting, value in settings.items():
        if setting != "portal_name":
            print(f"  {setting}: {value}")


def print_portal_info(portal_info):
    """Print the portal user and server information."""
    user_info_table = prettytable.PrettyTable(align="l")
    server_info_table = prettytable.PrettyTable(align="l")

    user_info_table.field_names = ["User Info", "Value"]
    server_info_table.field_names = ["Server Info", "Value"]

    user_info = portal_info.get("user_info", {})
    for key, value in user_info.items():
        if key in ["exp_date", "created_at"]:
            value = datetime.fromtimestamp(int(value), timezone.utc).strftime(
                "%Y-%m-%d %H:%M:%S"
            )
        user_info_table.add_row([key, value if value not in (None, "") else ""])

    server_info = portal_info.get("server_info", {})
    for key, value in server_info.items():
        server_info_table.add_row([key, value if value not in (None, "") else ""])

    print(user_info_table)
    print(server_info_table)


def print_portal_list(config_file):
    """Print the names and URLs of portals using prettytable."""
    config = configparser.ConfigParser()
    config.read(config_file)

    table = prettytable.PrettyTable(align="l")
    table.field_names = ["Portal Name", "URL", "Username"]

    for section in config.sections():
        url = config.get(section, "portal_url", fallback="Not specified")
        username = config.get(section, "username", fallback="Not specified")
        table.add_row([section, url, username])

    print(table)


def print_categories(conn):
    """Print categories in a CLI table with 'category_id' and 'category_name'."""
    sql = "SELECT category_id, category_name FROM live_categories"

    categories = execute_sql(conn, sql, fetch=True)

    table = prettytable.PrettyTable(align="l")
    table.field_names = ["Category ID", "Category Name"]

    for category in categories:
        table.add_row(category)

    print(table)


def print_streams(settings, conn, failed):
    """Print streams in a CLI table, sorted by channel number."""
    streams_data = retrieve_live_ffprobe(conn, settings["categories"], failed)
    streams = [Stream(data) for data in streams_data]

    table = prettytable.PrettyTable(align="l")
    # Print streams which failed ffprobe
    if failed:
        table.field_names = ["Category Name", "Stream ID", "Ch Num", "Channel Name"]
    # Print simple format for smaller screens
    elif settings["simple"]:
        table.field_names = ["Category Name", "Channel Name", "Standard", "FPS"]
    # Print default format with extra data
    else:
        table.field_names = [
            "Cat ID",
            "Category Name",
            "Stream ID",
            "Ch Num",
            "Channel Name",
            "Catchup",
            "Codec",
            "Resolution",
            "Standard",
            "FPS",
        ]

    for stream in streams:
        table.add_row(stream.get_display_data(settings["simple"], failed))

    print(table)


def print_stats(conn):
    """Print category/channel statistics from the database."""

    # Query for total number of distinct category_ids
    sql_category_count = "SELECT COUNT(DISTINCT category_id) FROM live_streams"
    total_categories = execute_sql(conn, sql_category_count, fetch=True)[0][0]

    # Query for total number of stream_ids
    sql_stream_count = "SELECT COUNT(stream_id) FROM live_streams"
    total_streams = execute_sql(conn, sql_stream_count, fetch=True)[0][0]

    # Query for number of stream_ids with raw_json populated
    sql_json_count = """
                     SELECT COUNT(stream_id)
                     FROM live_ffprobe
                     WHERE raw_json IS NOT NULL AND raw_json != ''
                     """
    streams_with_json = execute_sql(conn, sql_json_count, fetch=True)[0][0]
    streams_failed = total_streams - streams_with_json

    # Print the results
    print(f"Total Categories: {total_categories}")
    print(f"Total Channels: {total_streams}")
    print(f"Channels Successfully Probed: {streams_with_json}")
    print(f"Channels Missing Data: {streams_failed}")


def export_csv(conn, filename, categories):
    """
    Export streams to CSV sorted by channel number.
    Check for existing file and ask before overwriting.
    """
    if os.path.isfile(filename):
        overwrite = input(
            f"The specified CSV file '{filename}' already exists.\n\n"
            "Would you like to proceed and overwrite the existing file? (y/n): "
        ).lower()
        if overwrite != "y":
            print("Exiting without overwriting existing file.")
            sys.exit(1)

    field_names = [
        "Category ID",
        "Category Name",
        "Stream ID",
        "Channel Number",
        "Channel Name",
        "Catchup",
        "Codec",
        "Profile",
        "Width",
        "Height",
        "Resolution",
        "Standard",
        "FPS",
        "Scan Type",
        "Updated Timestamp",
    ]
    streams_data = retrieve_live_ffprobe(conn, categories)
    streams = [Stream(data) for data in streams_data]

    with open(filename, mode="w", newline="") as csv_file:
        writer = csv.writer(csv_file, quoting=csv.QUOTE_NONNUMERIC)

        # Write the header only if file is new/empty.
        if os.path.getsize(filename) == 0:
            writer.writerow(field_names)

        # Write each dictionary in the list as a row
        for stream in streams:
            writer.writerow(stream.get_display_data(csv=True))


def handle_cli_arguments(args, settings, conn):
    """Process CLI args. Return True to exit."""

    if args.portal_info:
        portal_info = query_api(settings, None, args.portal_info)
        print_portal_info(portal_info)
        return True

    if args.print_config:
        portal_name = args.portal if args.portal else "command_line"
        print_config(portal_name, settings)
        return True

    if args.categories_print:
        print_categories(conn)
        return True

    if args.export_cli or args.export_cli_failed:
        print_streams(settings, conn, args.export_cli_failed)
        return True

    if args.export_csv:
        if args.export_csv == "portal":
            filename = get_filename(settings["portal_url"], "csv")
        else:
            script_dir = os.path.dirname(os.path.abspath(__file__))
            filename = os.path.join(script_dir, args.export_csv)

        export_csv(conn, filename, settings["categories"])
        return True

    if args.stats:
        print_stats(conn)
        return True

    return False


def setup_arg_parser():
    parser = argparse.ArgumentParser(
        description="""Queries Xtream API enabled servers and catalogs
                       streams using ffprobe for metadata."""
    )
    parser.add_argument(
        "portal_url",
        nargs="?",
        help="""Xtream Portal URL. Include the protocol and
                optionally the port. (http://example.com:1234)""",
    )
    parser.add_argument("username", nargs="?", help="Xtream Portal Username.")
    parser.add_argument("password", nargs="?", help="Xtream Portal Password.")
    config = parser.add_argument_group("Configuration Options")
    config.add_argument(
        "-p",
        "--portal",
        help=f"Specify a portal name to process from the {PORTAL_CONFIG} config.",
    )
    config.add_argument(
        "-pl",
        "--portal-list",
        action="store_true",
        help=f"Print the list of portals defined in {PORTAL_CONFIG}.",
    )
    config.add_argument(
        "-pc",
        "--print-config",
        action="store_true",
        help="""Print the configuration as it is interpreted from
                the config file and/or cli arguments.""",
    )
    portal = parser.add_argument_group("Portal Options")
    portal.add_argument(
        "-fr",
        "--force-refresh",
        action="store_true",
        help="Force the refresh of categories and streams, ignoring the age check.",
    )
    portal.add_argument(
        "-o",
        "--offline",
        action="store_true",
        help="""Run in offline mode, utilizing only the local
                database if there is one populated.""",
    )
    portal.add_argument(
        "-ua",
        "--user-agent",
        choices=["vlc", "mx", "smarters", "tivimax"],
        help="""Override the User-Agent for requests and ffprobe.
                Options: vlc (VLC), mx (MX Player), smarters (IPTV Smarters),
                tivimax (Tivimax macOS).""",
    )
    info = parser.add_argument_group("Information Display/Export")
    info.add_argument(
        "-pi",
        "--portal-info",
        action="store_true",
        help="Print the user/server info for the portal.",
    )
    info.add_argument(
        "-cap",
        "--categories-print",
        action="store_true",
        help="Print all stream categories.",
    )
    info.add_argument(
        "-ecli",
        "--export-cli",
        action="store_true",
        help="Export/Print stream data to the CLI. May be used with -s/--simple.",
    )
    info.add_argument(
        "-eclif",
        "--export-cli-failed",
        action="store_true",
        help="""Export/Print stream data to the CLI which failed ffprobe processing.
                May be used with -s/--simple.""",
    )
    info.add_argument(
        "-ecsv",
        "--export-csv",
        nargs="?",
        const="portal",
        help="""Export stream data to CSV. Specify desired filename (-ecsv portal.csv)
                or it will be derived from the portal URL for you.""",
        metavar="FILENAME.csv",
    )
    info.add_argument(
        "-q",
        "--quiet",
        action="store_true",
        help="""Suppresses CLI output of categories/streams being added/removed/modified
                during sync. Significantly speeds up the initial sync.""",
    )
    info.add_argument(
        "-s",
        "--simple",
        action="store_true",
        help="""Used with -ecli/--export-cli.
                Simpler output format useful for smaller screens.""",
    )
    info.add_argument(
        "-st",
        "--stats",
        action="store_true",
        help="Print portal stats (total categories, streams success/fail).",
    )
    probe = parser.add_argument_group("FFprobe Options")
    probe.add_argument(
        "-con",
        "--connections",
        type=int,
        default=1,
        help="""Number of connections (default: 1) to use for probing in parallel.
                Do not use more than max_connections on --portal-info.""",
    )
    probe.add_argument(
        "-ca",
        "--categories",
        help="""Specify one or more (comma separated) category IDs
                to process only those streams with ffprobe.""",
    )
    probe.add_argument(
        "-cae",
        "--categories-exclude",
        help="""Specify one or more (comma separated) category IDs to
                exclude from processing with ffprobe.""",
    )
    probe.add_argument(
        "-d",
        "--delay",
        type=int,
        default=1,
        help="""Delay in seconds between probing streams (default: 1 second).
                Use 0 for no delay. Prevents abusing the server.""",
    )
    probe.add_argument(
        "-fp",
        "--force-probe",
        action="store_true",
        help="""Force the refresh of ffprobe data.
                May be filtered with -ca/--categories.""",
    )
    probe.add_argument(
        "-np",
        "--no-probe",
        action="store_true",
        help="""Disable ffprobe quality/metadata probing
                which uses a service connection.""",
    )
    probe.add_argument(
        "-pf",
        "--probe-failed",
        action="store_true",
        help="Force the refresh of ffprobe data on streams which previously failed.",
    )
    parser.add_argument("--debug", action="store_true", help="Debug output.")
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="""Verbose error and script processing output.
                Not recommended for regular usage.""",
    )
    parser.add_argument(
        "-V",
        "--version",
        action="version",
        version=f"%(prog)s {__version__}",
        help="Print the script version.",
    )
    return parser.parse_args()


def main():
    if not check_ffprobe():
        logging.critical(
            "ffprobe is not installed or not found in PATH."
            " Please install ffprobe to continue."
        )
        sys.exit(1)

    # Handle graceful shutdown
    shutdown_event = Event()
    signal.signal(signal.SIGINT, signal_handler(shutdown_event))

    # Setup CLI arguments
    args = setup_arg_parser()

    # Configure Logging
    configure_logging(args.verbose, args.debug)

    if args.portal_list:
        print_portal_list(PORTAL_CONFIG)
        sys.exit(0)

    # Check if portal, username, and password are provided
    if not ((args.portal_url or args.username or args.password) or args.portal):
        logging.error(
            "Portal, username, and password are required,"
            f" or use -p/--portal to load from {PORTAL_CONFIG}"
        )
        print("iptv-catalog.py [portal_url] [username] [password]")
        print("iptv-catalog.py --portal [portal_name]")
        print("iptv-catalog.py --help for more.")
        sys.exit(1)

    logging.info("Starting script execution...")

    # Load the configuration file if needed
    config = load_config(args.portal) if args.portal else None
    portal_name = args.portal if args.portal else None

    # Merge details from the config and args
    settings = merge_settings(args, config, portal_name)
    if not settings and config:
        logging.error("Portal configuration not found or missing required arguments.")
        sys.exit(1)

    # Test connection first
    if settings["offline"] is False:
        portal_test = test_api(settings)
        if portal_test is not True:
            logging.critical(
                f"Portal connection test failed. Error code: {portal_test.status_code}"
            )
            sys.exit(1)
    else:
        logging.info("Portal in offline mode, skipping connection test.")

    # Setup database
    global db_filename
    db_filename = get_filename(settings["portal_url"], "db")
    conn = setup_database(db_filename)

    if handle_cli_arguments(args, settings, conn):
        conn.close()
        sys.exit(0)

    # Execute all processing functions for the portal
    process_portal(settings, conn)

    conn.close()
    print("Script execution completed.")


if __name__ == "__main__":
    main()
