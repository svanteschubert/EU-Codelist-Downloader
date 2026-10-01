/*
 * Copyright 2025-2026 Svante Schubert
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
package org.standict.codelist.phase3.download;

import org.junit.jupiter.api.Test;
import org.standict.codelist.shared.Configuration;
import org.standict.codelist.shared.FileRegistry;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for Phase 3 - File Download with CSV output.
 */
public class FileDownloaderTest {
    
    @Test
    public void testPhase3CsvPath() {
        Configuration config = new Configuration();
        
        assertNotNull(config.getPhase3CsvPath());
        assertTrue(config.getPhase3CsvPath().contains("phase3"));
    }
    
    @Test
    public void testDownloadDelayConfiguration() {
        Configuration config = new Configuration();
        
        assertTrue(config.getDownloadDelaySeconds() > 0);
        assertEquals(1, config.getDownloadDelaySeconds()); // Default value
    }
    
    @Test
    public void testFileRegistryInitialization() {
        FileRegistry registry = new FileRegistry("test-registry.json");
        
        assertNotNull(registry);
        assertEquals(0, registry.getAllFiles().size());
    }
}

