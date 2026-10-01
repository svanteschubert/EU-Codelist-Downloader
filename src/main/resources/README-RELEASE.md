# EU Code Lists Release Package

This ZIP file contains European e-Invoice code lists and supporting artefacts for a specific effective date.

> **Unofficial showcase.** The authoritative code lists and supporting artefacts are those the European Commission
> publishes in its [Registry of supporting artefacts to implement EN16931](https://ec.europa.eu/digital-building-blocks/sites/spaces/DIGITAL/pages/467108974/Registry+of+supporting+artefacts+to+implement+EN16931). Where this package differs, the
> Registry prevails.

## Contents

- **downloaded-files.json**: Filtered registry containing only entries with the matching effective date
- **downloaded-files/**: Artefacts for this effective date, flattened into one directory. Superseded revisions are excluded once their replacement has been successfully downloaded.
- **LICENSE**: the GNU AGPL v3 of EU-Codelist-Downloader, the tool that assembled this package
- **README-RELEASE.md**: This file

## Usage

1. Extract the ZIP file
2. The `downloaded-files.json` file contains metadata for all files in this package
3. Files are stored directly under `downloaded-files/`
4. Refer to individual file metadata in `downloaded-files.json` for version numbers, publishing dates, and other details

## Source

These files were downloaded from the [Registry of supporting artefacts to implement EN16931](https://ec.europa.eu/digital-building-blocks/sites/spaces/DIGITAL/pages/467108974/Registry+of+supporting+artefacts+to+implement+EN16931) maintained by the European Commission.

## Integrity

All files include SHA-256 hash verification. The hash for each file is stored in `downloaded-files.json` under the `actual_hash` field.

## License

The files in `downloaded-files/` are published by the European Commission and remain subject to its terms. The
enclosed LICENSE, the GNU Affero General Public License version 3 or later, covers only `downloaded-files.json` and
this README, as written by EU-Codelist-Downloader.

