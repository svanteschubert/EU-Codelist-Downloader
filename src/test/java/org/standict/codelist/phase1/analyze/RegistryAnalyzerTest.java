/*
 * Copyright 2025-2026 Svante Schubert
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
package org.standict.codelist.phase1.analyze;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.standict.codelist.shared.Configuration;
import org.standict.codelist.shared.FileMetadata;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for Phase 1 - Registry Analysis with CSV output.
 */
public class RegistryAnalyzerTest {
    
    @Test
    public void testConfigurationDefaults() {
        Configuration config = new Configuration();
        
        assertNotNull(config.getPhase1CsvPath());
        assertTrue(config.getPhase1CsvPath().contains("phase1"));
        assertTrue(config.isWriteLatestCopy());
    }
    
    @Test
    public void testCsvOutputDirectory(@TempDir Path tempDir) throws IOException {
        Configuration config = new Configuration();
        config.setCsvOutputBasePath(tempDir.toString());
        
        String phase1Path = config.getPhase1CsvPath();
        assertTrue(phase1Path.contains("phase1"));
        
        // Verify path would create the directory
        Path phase1Dir = Paths.get(phase1Path);
        Files.createDirectories(phase1Dir);
        assertTrue(Files.exists(phase1Dir));
    }

    @Test
    void detectsLatestValidationVersionsAndPairedCodeLists(@TempDir Path output) throws Exception {
        com.sun.net.httpserver.HttpServer server = com.sun.net.httpserver.HttpServer.create(
                new java.net.InetSocketAddress("127.0.0.1", 0), 0);
        String html = "<ul>" +
                "<li>15/05/26 | Published: 16/04/26 | (latest versions) " +
                "<a href='/download/en16931-ubl-1.3.16.zip'>EN16931 Validation artefacts for UBL 2.1 - version 1.3.16</a> " +
                "<a href='/download/en16931-cii-1.3.16.zip'>EN16931 Validation artefacts for CII 16b - version 1.3.16</a></li>" +
                "<li>15/11/22 | Published: 17/10/22 | <a href='/download/en16931-ubl-1.3.9.zip'>EN16931 Validation artefacts for UBL 2.1 - version 1.3.9</a></li>" +
                "<li>15/05/26 | Published: 26/04/16 | (latest version) " +
                "<a href='/download/EN16931%20code%20lists%20values%20v17b%20-%20used%20from%202026-05-15.xlsx'>Full listing of the code lists as used in EN16931 - version 17.0</a> " +
                "<a href='/download/digital-genericodes-2026-05-15.zip?version=2'>Genericode files</a></li></ul>";
        server.createContext("/", exchange -> {
            if (exchange.getRequestMethod().equals("HEAD")) {
                exchange.sendResponseHeaders(200, -1);
            } else {
                byte[] bytes = html.getBytes(java.nio.charset.StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(200, bytes.length);
                exchange.getResponseBody().write(bytes);
            }
            exchange.close();
        });
        server.start();
        Configuration config = new Configuration();
        config.setRegistryUrl("http://127.0.0.1:" + server.getAddress().getPort() + "/");
        config.setCsvOutputBasePath(output.toString());
        RegistryAnalyzer analyzer = new RegistryAnalyzer(config);
        try {
            List<FileMetadata> files = analyzer.analyzeRegistry();
            assertEquals(5, files.size());
            assertEquals(4, files.stream().filter(FileMetadata::isLatestRelease).count());
            assertFalse(files.stream().filter(f -> f.getFilename().contains("1.3.9")).findFirst().orElseThrow().isLatestRelease());
            FileMetadata genericode = files.stream().filter(f -> f.getFilename().contains("genericodes")).findFirst().orElseThrow();
            assertEquals("17", genericode.getVersion());
            assertEquals(java.time.LocalDate.of(2026, 5, 15), genericode.getEffectiveDate());
            // Preserve the upstream anomaly instead of inventing a publication date.
            assertEquals(java.time.LocalDate.of(2016, 4, 26), genericode.getPublishingDate());
        } finally {
            analyzer.close();
            server.stop(0);
        }
    }

    @Test
    void rejectsFailedRegistryResponseWithoutReplacingInventory(@TempDir Path output) throws Exception {
        com.sun.net.httpserver.HttpServer server = com.sun.net.httpserver.HttpServer.create(
                new java.net.InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> { exchange.sendResponseHeaders(503, -1); exchange.close(); });
        server.start();
        Configuration config = new Configuration();
        config.setRegistryUrl("http://127.0.0.1:" + server.getAddress().getPort() + "/");
        config.setCsvOutputBasePath(output.toString());
        Path inventory = output.resolve("phase1/inventory-latest.csv");
        Files.createDirectories(inventory.getParent());
        Files.writeString(inventory, "existing inventory");
        RegistryAnalyzer analyzer = new RegistryAnalyzer(config);
        try {
            assertThrows(IOException.class, analyzer::analyzeRegistry);
            assertEquals("existing inventory", Files.readString(inventory));
        } finally {
            analyzer.close();
            server.stop(0);
        }
    }
}

