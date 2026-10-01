/*
 * Copyright 2025-2026 Svante Schubert
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
package org.standict.codelist.phase2.compare;

import org.junit.jupiter.api.Test;
import org.standict.codelist.shared.Configuration;
import org.standict.codelist.shared.FileMetadata;
import org.standict.codelist.shared.FileRegistry;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for Phase 2 - File Comparison with CSV output.
 */
public class FileComparatorTest {
    
    @Test
    public void testChangeTypeEnum() {
        FileComparator.ChangeType[] values = FileComparator.ChangeType.values();
        assertEquals(4, values.length);
        assertTrue(List.of(values).contains(FileComparator.ChangeType.NEW));
        assertTrue(List.of(values).contains(FileComparator.ChangeType.CHANGED));
        assertTrue(List.of(values).contains(FileComparator.ChangeType.DELETED));
        assertTrue(List.of(values).contains(FileComparator.ChangeType.UNCHANGED));
    }
    
    @Test
    public void testPhase2CsvPath() {
        Configuration config = new Configuration();
        
        assertNotNull(config.getPhase2CsvPath());
        assertTrue(config.getPhase2CsvPath().contains("phase2"));
    }
    
    @Test
    public void testFileMetadataCreation() {
        FileMetadata metadata = new FileMetadata("https://example.com/test.xml");
        
        assertNotNull(metadata);
        assertEquals("https://example.com/test.xml", metadata.getUrl());
        assertNotNull(metadata.getFilename());
        assertNotNull(metadata.getCategory());
    }
}

