/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
            if ("en16931-code-lists".equals(category) && name.endsWith(".xlsx")) {
                hasEn16931Xlsx = true;
            }

            // Genericode (optional SHOULD)
            if ("genericodes".equals(category) && name.endsWith(".zip")) {
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


