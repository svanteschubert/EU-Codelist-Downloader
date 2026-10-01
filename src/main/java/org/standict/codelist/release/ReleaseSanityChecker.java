/*
 * Copyright 2025-2026 Svante Schubert
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
package org.standict.codelist.release;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.standict.codelist.shared.FileMetadata;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Performs sanity checks on the set of artefacts for a given effective date.
 *
 * Rules (all are now warnings, not errors):
 *  - SHOULD: One validation artefact for UBL (.zip) and one for CII (.zip)
 *  - SHOULD: One EN16931 codelist as XLSX (.xlsx)
 *  - SHOULD: One Genericode package (.zip)
 *
 * <p><strong>Scope: completeness of a delivery, not agreement between its files.</strong> This is the gate in front of
 * packaging and publishing a release, so it must run here and must stay cheap: it judges a delivery by which
 * categories of file are present, from the registry metadata alone, without opening an archive.
 *
 * <p>Whether those files agree with each other — the same code list shipped twice with different codes, a validation
 * artefact that does not enforce the values the Genericode package publishes, codes that contradict the effective date
 * — needs the normalized contents of the archives and therefore belongs to the reporting stage built on
 * {@code EU-Codelist-Normalizer}. The two checks overlap in wording but not in evidence; passing this one says nothing
 * about the other, and neither replaces the other.
 */
public class ReleaseSanityChecker {
    private static final Logger logger = LoggerFactory.getLogger(ReleaseSanityChecker.class);

    public static class SanityResult {
        public final List<String> errors = new ArrayList<>();
        public final List<String> warnings = new ArrayList<>();

        public boolean hasErrors() { return !errors.isEmpty(); }
        public boolean hasWarnings() { return !warnings.isEmpty(); }
    }

    public SanityResult check(LocalDate effectiveDate, Map<String, FileMetadata> entries) {
        SanityResult result = new SanityResult();

        boolean hasUbl = false;
        boolean hasCii = false;
        boolean hasEn16931Xlsx = false;
        boolean hasGenericode = false;

        for (Map.Entry<String, FileMetadata> e : entries.entrySet()) {
            FileMetadata fm = e.getValue();
            String category = toLowerSafe(fm.getCategory());
            String name = toLowerSafe(fm.getDecodedFilename());

            // Validation artefacts: UBL and CII
            if (name.contains("en16931-ubl-") && name.endsWith(".zip")) {
                hasUbl = true;
            }
            if (name.contains("en16931-cii-") && name.endsWith(".zip")) {
                hasCii = true;
            }

            // EN16931 codelist XLSX (category detector maps to en16931-code-lists)
            if (name.contains("en16931") && name.contains("code lists") && name.endsWith(".xlsx")) {
                hasEn16931Xlsx = true;
            }

            // Genericode (optional SHOULD)
            if (("genericodes".equals(category) || "en 16931 code list - genericode".equals(category)) && name.endsWith(".zip")) {
                hasGenericode = true;
            }
        }

        if (!hasUbl) {
            result.warnings.add("Missing recommended validation artefact: UBL (.zip)");
        }
        if (!hasCii) {
            result.warnings.add("Missing recommended validation artefact: CII (.zip)");
        }
        if (!hasEn16931Xlsx) {
            result.warnings.add("Missing recommended EN16931 codelist XLSX");
        }
        if (!hasGenericode) {
            result.warnings.add("Missing Genericode ZIP (recommended)");
        }

        // Bundle output per effective date with line breaks
        StringBuilder summary = new StringBuilder();
        summary.append("\nEffective date: ").append(effectiveDate).append('\n');
        summary.append("  SHOULD UBL validation ZIP: ").append(hasUbl ? "OK" : "MISSING").append('\n');
        summary.append("  SHOULD CII validation ZIP: ").append(hasCii ? "OK" : "MISSING").append('\n');
        summary.append("  SHOULD EN16931 XLSX:       ").append(hasEn16931Xlsx ? "OK" : "MISSING").append('\n');
        summary.append("  SHOULD Genericode ZIP:     ").append(hasGenericode ? "OK" : "MISSING").append('\n');

        if (result.hasWarnings()) {
            if (!result.warnings.isEmpty()) {
                summary.append("  Warnings: ").append(String.join("; ", result.warnings)).append('\n');
            }
            logger.warn(summary.toString());
        } else {
            logger.info(summary.toString());
        }
        return result;
    }

    private static String toLowerSafe(String value) {
        return value == null ? "" : value.toLowerCase(Locale.ROOT);
    }
}

