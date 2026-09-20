# EU Code Lists Release Package

This ZIP file contains European e-Invoice code lists and supporting artefacts for a specific effective date.

## Contents

- **downloaded-files.json**: Filtered registry containing only entries with the matching effective date
- **downloaded-files/**: Artefacts for this effective date, flattened into one directory. Superseded revisions are excluded once their replacement has been successfully downloaded.
- **LICENSE**: Apache License 2.0
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

This package is licensed under the Apache License, Version 2.0. See LICENSE file for details.

