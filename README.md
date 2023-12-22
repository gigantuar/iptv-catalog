# IPTV Stream Cataloger

This Python script catalogs streams from Xtream API enabled servers using `ffprobe` for metadata. Channels, categories, and stream details are stored in a local database. Streams are processed with support for parallel operations. The script supports various options like filtering by categories, exporting data to CSV, and customizing the User-Agent if needed by the provider.

There is a companion script `iptv_search.py` which allows searching the local database by channel name. Additional details below.

## Goals

The primary motiviation behind this script was to store stream data in an easy to access format while also discovering metadata such as the resolution. The script works well for the purpose it was designed for. The script is intentionally not split into several modules as I prefered a single file for my use case, this could change in the future.

This is my first comprehensive Python script and there is room for improvement.

## Features

- Query channel categories and streams from Xtream API enabled servers.
- Portal data is refreshed every 24 hours unless overridden by the user.
- Stores all data in a SQLite database per portal.
- Display stream data on the CLI in various formats.
- Export detailed stream data to CSV.
- Customizable probing delay to avoid hammering the server.
- Customizable User-Agent which may be required for some providers.
- Execute ffprobe to gather detailed metadata about streams.
- Automatically refreshes ffprobe metadata if a stream name changes.
- Processes streams in parallel with a user defined connection quantity.
- Filters to include/exclude categories for filtering streams.
- Configurable via configuration file and/or command-line arguments.

## Requirements

- Python 3
- `requests`, `prettytable`, `tqdm` libraries.
- `ffprobe` installed and accessible in PATH.

## Installation

### Prerequisites

- Ensure FFmpeg (for ffprobe) is installed on your system.
- Ensure Python 3 is installed on your system unless you are using the binary releases. You can download it from [python.org](https://www.python.org/downloads/).

### Installing FFmpeg

- **Windows:** Download and install FFmpeg from [ffmpeg.org](https://ffmpeg.org/download.html). Add the path to `ffprobe.exe` to your system's PATH variable. Extracting the 'bin' folder contents of `ffmpeg-release-essentials.zip` into the script directory will suffice.
- **macOS:** Install using Homebrew with `brew install ffmpeg`.
- **Linux:** Use your distribution's package manager, for example, `sudo apt-get install ffmpeg` for Debian.

### Setting up a Python Virtual Environment (Recommended)

Using a virtual environment isolates the script dependencies from your system's Python environment.

1. **Create a Virtual Environment:**
   - **Windows:** `python -m venv venv`
   - **macOS/Linux:** `python3 -m venv venv`

2. **Activate the Virtual Environment:**
   - **Windows:** `venv\Scripts\activate`
   - **macOS/Linux:** `source venv/bin/activate`

### Installing Python Dependencies

With the virtual environment activated, install the required libraries:

```
pip install -r requirements.txt
```
or
```
pip install requests prettytable tqdm
```

### Binary Distribution

Binaries are available for download under Releases for Windows, Linux and macOS. These are generated using GitHub Actions and PyInstaller. The binaries will allow you to use the script without installing Python and Python dependencies. Antivirus software is likely to false detect the binaries as malware. Installing Python and using the previously mentioned Virtual Environment is preferred and recommended.

## Usage

The script can be executed using a combination of command-line arguments and an optional configuration file.

### Basic Command

```
python iptv-catalog.py [portal_url] [username] [password]
```

### Using a Configuration File

1. Create a `portals.ini` file with portal details. Reference `portals.ini.sample` for details.
2. Run the script specifying the portal name:
   ```
   python iptv-catalog.py --portal [portal_name]
   ```

### Common Command-Line Arguments
- `-h, --help`                  Print a complete list of all options.
- `-p, --portal`                Specify a portal name to process from `portals.ini`
- `-pl, --portal-list`          Print the list of portals defined in `portals.ini`
- `-pi, --portal-info`          Print the user/server info for the portal.
- `-pc, --print-config`         Print the configuration as interpreted from `portals.ini` and/or CLI overrides
- `-cap, --categories-print`    Print all categories with IDs and exit
- `-ca, --categories`           Filter specific categories by IDs
- `-con, --connections`         Number of connections for parallel processing (default: 1)
- `-ecli, --export-cli`         Display stream data on the CLI
- `-ecsv, --export-csv`         Export stream data to CSV
- `-fr, --force-refresh`        Force category and streams refresh from the portal
- `-o, --offline`               Run in offline mode, utilizing only the local database
- `-ua, --user-agent`           Override the User-Agent for requests and ffprobe. Options: vlc, mx, smarters, tivimax

### Example Usage

Export streams of a specific portal to a CSV file:

```
python iptv-catalog.py --portal myportal --export-csv portal_data.csv
python iptv-catalog.py https://example.com username password --export-csv portal_data.csv
```

Print configuration for a portal to verify settings or CLI arguments:

```
python iptv-catalog.py --portal myportal --print-config
python iptv-catalog.py https://example.com username password --print-config
```

## Configuration File Example (`portals.ini`)
Complete example available in `portals.ini.sample`
```
[myportal]
portal_url = http://example.com:1234
username = user
password = pass
```

## Output Examples

- CLI output of stream categories or streams.
```
+--------+---------------+-----------+--------+-------------------------+---------+-------+------------+----------+-----+
| Cat ID | Category Name | Stream ID | Ch Num | Channel Name            | Catchup | Codec | Resolution | Standard | FPS |
+--------+---------------+-----------+--------+-------------------------+---------+-------+------------+----------+-----+
| 123    | Test Group    | 123456    | 1      | Test Stream 1           | false   | hevc  | 3840x2160  | UHD-4K   | 50  |
| 123    | Test Group    | 567890    | 2      | Test Stream 2           | false   | h264  | 1920x1080  | FHD      | 50  |
| 123    | Test Group    | 765432    | 3      | Test Stream 3           | false   | h264  | 1280×720   | HD       | 25  |
+--------+---------------+-----------+--------+-------------------------+---------+-------+------------+----------+-----+
```
- CSV file headers with detailed stream information.
```
"Category ID","Category Name","Stream ID","Channel Number","Channel Name","Catchup","Codec","Profile","Width","Height","Resolution","Standard","FPS","Scan Type","Updated Timestamp"
```

## Additional Notes

- Ensure the `portals.ini` file is correctly formatted and accessible. Reference the example `portals.ini.sample` file. Section (portal) names are case sensitive.
- The script requires network access to the specified portal. Once data is cached locally in the database, offline mode may be used to print or export data.
- Use the `--help` option for more detailed usage instructions.

# Companion Scripts

## Database Search

`iptv_search.py` is available to quickly search the local database created by `iptv_catalog.py`. `iptv_search.py` imports functions from `iptv_catalog.py` so you need both scripts in order to search.

### Usage
`iptv_search.py [--portal PORTAL] [--db DB] [--all] channel_name`

- `-p, --portal PORTAL`   Specify a portal name from portals.ini to select the database.
- `-d, --db DB`           Specify the local SQLite database filename directly.
- `-a, --all`             Search through all *.db files in the script folder.
- `channel_name`          Text to search for in the channel names
