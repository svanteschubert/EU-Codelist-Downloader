# EU-Codelist-Downloader

A Java application for automatically downloading and organizing European e-Invoice code lists from the European Commission's Digital Building Blocks registry. This application helps members of the European e-Invoice Technical Advisory Group maintain up-to-date local copies of all official code lists and supporting artefacts.

## Purpose

This repository provides an automated solution to download and organize European EN16931 code lists and supporting artefacts including:

- **EAS** (Electronic Address Scheme) code lists
- **VATEX** (VAT Exemption Reason) code lists  
- **EN16931** code lists values
- **Validation artefacts** for UBL and CII syntaxes
- **Genericodes** for cross-border transactions
- **Technical guidance** documents

## Quick Start

### Prerequisites

- Java 11 or higher
- Maven 3.6 or higher

### Installation

```bash
# Clone the repository
git clone https://github.com/your-org/CodeListEU.git
cd CodeListEU

# Build the project
mvn clean package
```

### Running the Application

**Option 1: Run all phases (recommended for first run)**

```bash
# Windows
.\run-all.bat

# Linux/Mac
./run-all.sh
```

**Option 2: Run individual phases**

```bash
# Phase 1: Analyze and inventory files
.\run-phase1.bat     # or ./run-phase1.sh

# Phase 2: Compare with existing files
.\run-phase2.bat     # or ./run-phase2.sh

# Phase 3: Download new/changed files
.\run-phase3.bat     # or ./run-phase3.sh
```

**Option 3: Using Maven**

```bash
# Run complete workflow (runs once and exits by default - no idling)
mvn clean compile exec:java -Dexec.mainClass="org.standict.codelist.shared.CodeListDownloader"

# For continuous scheduled mode (runs periodically and idles between checks)
mvn clean compile exec:java -Dexec.mainClass="org.standict.codelist.shared.CodeListDownloader" -Dexec.args="--schedule"

# Or use the executable JAR (after mvn package)
# Default: runs once and exits
java -jar target/eu-codelist-downloader-1.0.0-jar-with-dependencies.jar

# Continuous scheduled mode
java -jar target/eu-codelist-downloader-1.0.0-jar-with-dependencies.jar --schedule
```

## How It Works

The application uses a **3-phase approach** to efficiently download and track code lists:

### Phase 1: Analysis

- Fetches the registry page and parses HTML
- Extracts metadata from HTML paragraphs and list items: effective date, publishing date, version, latest release flag
- Uses HTTP HEAD requests to get file metadata without downloading
- Collects file size, last modified date, content type, ETag
- Categorizes files automatically (EAS, VATEX, EN16931, validation artefacts)
- Writes inventory CSV to `src/main/resources/phase1/inventory-latest.csv`

### Phase 2: Comparison

- Compares current registry state with existing downloads from Phase 3
- Only shows files that do not exist in Phase 3 downloads CSV
- Uses same metadata structure as Phase 1 for consistency
- Writes diff CSV to `src/main/resources/phase2/diff-latest.csv`

### Phase 3: Download

- Downloads only files listed in Phase 2 (files not already downloaded)
- Organizes files into category-specific subdirectories
- Decodes URL-encoded filenames (%20 → spaces)
- Calculates SHA-256 hash for integrity verification
- Updates registry JSON (`downloaded-files.json`) with download metadata
- Includes configurable delays between downloads (1 second default)
- Appends to download log at `src/main/resources/phase3/downloads-latest.csv`

## Directory Structure

```bash
EU-Codelist-Downloader/
├── src/main/resources/downloaded-files/
│   ├── EAS code list/                      # Electronic Address Scheme (15 files)
│   ├── EN 16931 code list - GeneriCode/    # Genericode ZIP files (8 files)
│   ├── EN 16931 code list - XLSX/          # EN16931 spreadsheet files (17 files)
│   ├── guidance/                           # Technical guidance (1 file)
│   ├── validation-artefacts-CII/          # CII validation artefacts (18 files)
│   ├── validation-artefacts-UBL/          # UBL validation artefacts (17 files)
│   └── VATEX code list/                   # VAT Exemption Reason codes (7 files)
│
├── src/main/resources/
│   ├── phase1/
│   │   └── inventory-latest.csv           # Phase 1: File inventory (86 files)
│   ├── phase2/
│   │   └── diff-latest.csv                # Phase 2: Files to download
│   ├── phase3/
│   │   └── downloads-latest.csv           # Phase 3: Download results
│   ├── downloaded-files.json              # Registry of all downloaded files (sorted)
│   └── registry-links-analysis.txt        # Registry link analysis
│
└── config.json                             # Application configuration
```

**Note**: CSV files are prefixed (`inventory-`, `diff-`, `downloads-`) and timestamped snapshots are created for backup purposes. Only the `*-latest.csv` files are tracked in Git.

## Features

- **Smart downloading**: Only downloads files not already in `downloaded-files.csv` (cumulative history)
- **Rich metadata extraction**: Extracts effective date, publishing date, version, and latest release flag from HTML
- **Hash verification**: SHA-256 to detect corruptions and changes
- **Organized storage**: Automatic categorization into category-based directories
- **URL decoding**: Converts %20 to spaces in filenames and JSON output
- **Consistent sorting**: All outputs (CSV and JSON) sorted by effective date (oldest first), category, filename
- **Git integration**: `*-latest.csv` files tracked via Git for easy history
- **Configurable delays**: Human-like download behavior
- **Registry tracking**: JSON registry (`downloaded-files.json`) and cumulative CSV (`downloaded-files.csv`) maintain complete download history
- **Metadata propagation**: EN16931 XLSX metadata automatically propagated to paired GeneriCode ZIP files
- **Apache 2.0 licensed**: Open source

## Script Files

### Batch Scripts (Windows)

- `run-all.bat` - Run all phases sequentially
- `run-phase1.bat` - Run Phase 1 (Analysis) only
- `run-phase2.bat` - Run Phase 2 (Compare) only
- `run-phase3.bat` - Run Phase 3 (Download) only
- `run-link-extractor.bat` - Extract and categorize all registry links

### Shell Scripts (Linux/Mac)

- `run-all.sh` - Run all phases sequentially
- `run-phase1.sh` - Run Phase 1 (Analysis) only
- `run-phase2.sh` - Run Phase 2 (Compare) only
- `run-phase3.sh` - Run Phase 3 (Download) only

## Configuration

Create or edit `config.json` to customize:

```json
{
  "registryUrl": "https://ec.europa.eu/digital-building-blocks/sites/spaces/DIGITAL/pages/467108974/Registry+of+supporting+artefacts+to+implement+EN16931",
  "downloadBasePath": "src/main/resources/downloaded-files",
  "checkIntervalSeconds": 86400,
  "connectTimeoutMs": 30000,
  "readTimeoutMs": 60000,
  "downloadDelaySeconds": 1,
  "csvOutputBasePath": "src/main/resources",
  "writeLatestCopy": true
}
```

### Key Parameters

- `downloadBasePath`: Where to save downloaded files (default: `src/main/resources/downloaded-files`)
- `csvOutputBasePath`: Where to write CSV files (default: `src/main/resources`)
- `downloadDelaySeconds`: Delay between downloads in seconds (default: 1)
- `writeLatestCopy`: Write `*-latest.csv` files for Git tracking (default: true)
- `registryFilePath`: Path to registry JSON file (default: `downloaded-files.json` beside download directory)
- `autoConfirmDownloads`: Skip interactive confirmation prompts (default: false)

## Output Files

Each phase generates CSV files that are tracked in Git:

### Phase 1 CSV (`phase1/inventory-latest.csv`)

Inventory of all files detected on the registry page with extracted metadata.

**Columns**: `effective_date`, `publishing_date`, `version`, `category`, `is_latest_release`, `modification_date`, `url`, `filename`, `filetype`, `content_length`, `content_type`, `last_modified`

- All files sorted by: effective_date (oldest first), category, filename
- Effective date falls back to modification date from URL if not provided
- Publishing date omitted if not present in HTML

### Phase 2 CSV (`phase2/diff-latest.csv`)

Files that need to be downloaded (excluding files already in Phase 3).

**Columns**: Same structure as Phase 1 (12 columns)

- Shows only files not present in cumulative `downloaded-files.csv`
- Same sorting as Phase 1

### Phase 3 CSV Files

#### `phase3/downloads-latest.csv` (Current Run Only)
Per-run snapshot containing only files downloaded in the current execution.

**Columns**: Phase 1 columns + `actual_length`, `hash` (14 columns total)

- Contains only files downloaded in the current run
- Replaced (not appended) on each run
- Sorted by effective_date (oldest first), category, filename

#### `downloaded-files.csv` (Cumulative History)
Complete cumulative history of all files ever downloaded.

**Columns**: Same as `downloads-latest.csv` (14 columns total)

- Located beside `downloaded-files.json` (same directory)
- Contains ALL files ever successfully downloaded
- New downloads appended while maintaining sort order
- Used by Phase 2 to filter out already-downloaded files
- Used to seed `downloaded-files.json` registry when empty

### Registry JSON (`downloaded-files.json`)

Complete registry of all downloaded files with full metadata.

- Located beside `downloaded-files` directory (not inside it)
- Contains: URL, content length, content type, download timestamp, local path, effective date, publishing date, version, category, hash, file size
- Sorted identically to CSV files for consistency
- All fields quoted, URL-decoded filenames

## Link Analysis

Use `run-link-extractor.bat` to analyze all links on the registry page:

- Categorizes 88+ downloadable resource links
- Detects 129+ non-downloadable navigation links
- Output saved to `src/main/resources/registry-links-analysis.txt`

## Development

### Building

```bash
# Compile
mvn clean compile

# Run tests
mvn test

# Create executable JAR
mvn package
```

### Project Structure

```bash
src/
├── main/
│   ├── java/
│   │   └── org/standict/codelist/
│   │       ├── phase1/analyze/
│   │       │   └── RegistryAnalyzer.java
│   │       ├── phase2/compare/
│   │       │   └── FileComparator.java
│   │       ├── phase3/download/
│   │       │   └── FileDownloader.java
│   │       └── shared/
│   │           ├── CodeListDownloader.java
│   │           ├── Configuration.java
│   │           ├── FileMetadata.java
│   │           └── FileRegistry.java
│   └── resources/
│       ├── downloaded-files/  # Downloaded code lists
│       ├── phase1/           # Phase 1 CSV outputs
│       ├── phase2/           # Phase 2 CSV outputs
│       └── phase3/           # Phase 3 CSV outputs
└── test/
    └── java/
        └── org/standict/codelist/
            └── (test files)
```

## Troubleshooting

### No files downloaded

- Check internet connection
- Verify registry URL is accessible
- Review logs in `logs/code-list-downloader.log`
- Check CSV files in `src/main/resources/phase*/` for detected files

### Permission issues

- Ensure download directory is writable
- Check file system permissions
- Verify `src/main/resources` is writable for CSV outputs

## Technologies Used

- **Java 11+**: Programming language
- **Apache HttpClient 5.2**: HTTP client for fetching files and metadata
- **JSoup 1.16**: HTML parsing and DOM manipulation
- **Jackson**: JSON configuration management
- **Apache Commons CSV 1.10**: CSV file writing
- **SLF4J + Logback**: Logging framework
- **JUnit 5**: Testing framework
- **Maven**: Build and dependency management

## License

This project is licensed under Apache License 2.0. See [LICENSE](LICENSE) for details.

## Contributing

This project is maintained for the European e-Invoice Technical Advisory Group. Contributions are welcome!

## Acknowledgment

This project has received funding from the **[StandICT.eu 2026](https://standict.eu/)** initiative under the **9th Open Call**, which is funded by the European Union’s Horizon Europe research and innovation programme.

## Registry

Monitors: [Registry of supporting artefacts to implement EN16931](https://ec.europa.eu/digital-building-blocks/sites/spaces/DIGITAL/pages/467108974/Registry+of+supporting+artefacts+to+implement+EN16931)

## Release Packaging

The application can package downloaded files into release ZIP files, organized by effective date, for distribution via GitHub releases.

### Overview

- **Multiple Releases**: Creates one ZIP file per unique effective date
- **Deterministic ZIPs**: Uses fixed timestamps and sorted entries for consistent checksums
- **Checksum Verification**: Tracks SHA-256 checksums to avoid re-uploading unchanged files
- **Manual Trigger**: GitHub Actions workflow can be manually triggered by repository owner only

### Prerequisites

- Ensure repository Actions have write permissions: Settings → Actions → General → Workflow permissions → enable "Read and write permissions".
- The workflow is owner-gated; run it from the repository owner account (or relax the owner check in `.github/workflows/create-releases.yml`).

### Local Usage

Package ZIP files locally (without creating GitHub releases):

```bash
mvn clean compile
mvn exec:java -Dexec.mainClass="org.standict.codelist.release.ReleasePackagerMain" \
             -Dexec.args="--package-only"
```

This will:
1. Extract all unique effective dates from `downloaded-files.json`
2. Create one ZIP per date in `target/releases/`
3. Each ZIP contains:
   - Filtered `downloaded-files.json` (only entries matching that date)
   - Matching files from `downloaded-files/` directory
   - `LICENSE` file
   - `README-RELEASE.md` documentation
4. Calculate SHA-256 checksums and compare with stored checksums
5. Skip creating ZIPs if checksum matches existing release
6. Update `src/main/resources/releases/checksums.json` with new checksums

### GitHub Releases

To create/update GitHub releases automatically:

```bash
mvn clean compile
mvn exec:java -Dexec.mainClass="org.standict.codelist.release.ReleasePackagerMain" \
             -Dexec.args="--create-releases --github-token YOUR_TOKEN --owner OWNER --repo REPO"
```

Or use the GitHub Actions workflow:

1. Go to **Actions** tab in GitHub
2. Select **Create Releases** workflow
3. Click **Run workflow**
4. The workflow will:
   - Verify you are the repository owner
   - Package all ZIPs
   - Create/update GitHub releases for new or changed ZIPs
   - Upload ZIP files as release assets
   - Commit updated checksums.json (if changed)

If you prefer running locally instead of the workflow:

1. Create a Personal Access Token (classic) with `repo` scope
2. Set it in your shell as `GITHUB_TOKEN`
3. Run:

```bash
mvn exec:java -Dexec.mainClass="org.standict.codelist.release.ReleasePackagerMain" \
             -Dexec.args="--create-releases --github-token $GITHUB_TOKEN --owner OWNER --repo REPO"
```

### Release Structure

Each release is:
- **Tag**: `release-YYYY-MM-DD` (e.g., `release-2025-11-15`)
- **Title**: `EU Code Lists - Effective Date YYYY-MM-DD`
- **Asset**: `eu-codelists-YYYY-MM-DD.zip`

The ZIP filename reflects the effective date, making it easy to identify which code lists are included.

### Checksums

Checksums are stored in `src/main/resources/releases/checksums.json` (tracked in Git). This file maps effective dates (YYYY-MM-DD format) to SHA-256 checksums of the ZIP files. The packaging process:

1. Creates ZIP in `target/releases/`
2. Calculates checksum
3. Compares with stored checksum
4. If unchanged: deletes ZIP and skips release creation
5. If changed: keeps ZIP, updates checksum, and creates/updates release