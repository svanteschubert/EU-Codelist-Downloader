package org.standict.codelist.shared;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class RegistryPersistenceTest {
    @TempDir Path directory;

    @Test
    void filenameSurvivesRepeatedJsonRoundTrips() throws Exception {
        ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule());
        FileMetadata file = new FileMetadata("https://example.com/original.zip");
        file.setFilename("custom%20%2B%20%2520.zip");
        for (int i = 0; i < 3; i++) {
            file = mapper.readValue(mapper.writeValueAsString(file), FileMetadata.class);
            assertEquals("custom + %20.zip", file.getDecodedFilename());
        }
    }

    @Test
    void repairsLegacyMissingFilenameAndWindowsPath() throws Exception {
        Path path = directory.resolve("registry.json");
        Files.writeString(path, "{\"https://example.com/code%20list.xlsx\": {" +
                "\"localPath\":\"downloaded-files\\\\code list.xlsx\",\"downloaded\":true}}");
        FileRegistry registry = new FileRegistry(path.toString());
        FileMetadata file = registry.getAllFiles().get(0);
        assertEquals("code list.xlsx", file.getDecodedFilename());
        assertEquals("downloaded-files/code list.xlsx", file.getLocalPath());
        registry.saveRegistry();
        assertTrue(Files.readString(path).contains("\"filename\" : \"code list.xlsx\""));
        assertEquals("code list.xlsx", new FileRegistry(path.toString()).getAllFiles().get(0).getDecodedFilename());
    }

    @Test
    void reconcilesMetadataAndRenamedRevisionWithoutLosingHash() {
        FileRegistry registry = new FileRegistry(directory.resolve("registry.json").toString());
        FileMetadata old = spreadsheet("v17");
        old.setFileHash("original-hash");
        old.setLatestRelease(true);
        registry.registerFile(old);
        FileMetadata replacement = spreadsheet("v17b");
        replacement.setLatestRelease(true);
        registry.reconcileInventory(List.of(replacement));
        assertEquals(replacement.getUrl(), old.getSupersededBy());
        assertFalse(old.isLatestRelease());
        assertEquals("original-hash", old.getFileHash());
        registry.registerFile(replacement);
        FileMetadata current = spreadsheet("v17b");
        current.setVersion("17");
        current.setPublishingDate(LocalDate.of(2026, 4, 16));
        current.setLatestRelease(true);
        registry.reconcileInventory(List.of(current));
        assertEquals(current.getPublishingDate(), replacement.getPublishingDate());
        assertTrue(replacement.isLatestRelease());
        assertNull(replacement.getSupersededBy());
    }

    private FileMetadata spreadsheet(String revision) {
        FileMetadata file = new FileMetadata("https://example.com/EN16931%20code%20lists%20values%20" + revision + ".xlsx");
        file.setEffectiveDate(LocalDate.of(2026, 5, 15));
        file.setVersion("17");
        return file;
    }
}
