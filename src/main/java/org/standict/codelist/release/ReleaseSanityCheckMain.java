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
import java.util.Map;
import java.util.Set;

/**
 * Standalone entry point to run sanity checks on all effective dates.
 *
 * Usage:
 * mvn exec:java -Dexec.mainClass="org.standict.codelist.release.ReleaseSanityCheckMain" \
 *   -Dexec.args="--registry src/main/resources/downloaded-files.json"
 */
public class ReleaseSanityCheckMain {
    private static final Logger logger = LoggerFactory.getLogger(ReleaseSanityCheckMain.class);

    public static void main(String[] args) throws Exception {
        String registryPath = "src/main/resources/downloaded-files.json";
        for (int i = 0; i < args.length - 1; i++) {
            if ("--registry".equals(args[i])) {
                registryPath = args[i + 1];
            }
        }

        EffectiveDateExtractor extractor = new EffectiveDateExtractor();
        ReleaseSanityChecker checker = new ReleaseSanityChecker();

        // Announce test input and requirements
        logger.info("Sanity check source registry: {}", registryPath);
        logger.info("Test requirements (per effective date):");
        logger.info("  - MUST: Validation artefact for UBL (.zip)");
        logger.info("  - MUST: Validation artefact for CII (.zip)");
        logger.info("  - MUST: EN16931 codelist XLSX (.xlsx)");
        logger.info("  - SHOULD: Genericode ZIP (.zip)");
        LocalDate startDate = LocalDate.of(2019, 11, 15);
        logger.info("Tests start with effective date: {}", startDate);

        Set<LocalDate> dates = extractor.extractUniqueEffectiveDates(registryPath);
        // Only evaluate dates starting at 2019-11-15
        dates.removeIf(d -> d.isBefore(startDate));
        logger.info("Running sanity checks on {} effective dates (from {} onward)", dates.size(), startDate);

        int errors = 0;
        int warnings = 0;
        for (LocalDate date : dates) {
            Map<String, FileMetadata> entries = extractor.filterByEffectiveDate(registryPath, date);
            ReleaseSanityChecker.SanityResult result = checker.check(date, entries);
            if (result.hasErrors()) errors++;
            if (result.hasWarnings()) warnings++;
        }

        logger.info("Sanity check complete. Errors: {}, Warnings: {}", errors, warnings);
        if (errors > 0) {
            System.exit(2);
        }
    }
}


