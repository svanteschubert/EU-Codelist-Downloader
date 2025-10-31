package org.standict.codelist.release;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.*;
import java.time.LocalDate;
import java.util.Comparator;

public class ReleasePackagerMainTest {

    private Path tempDir;

    @BeforeEach
    void setup() throws IOException {
        tempDir = Files.createTempDirectory("releases-test-");
    }

    @AfterEach
    void cleanup() throws IOException {
        if (tempDir != null) {
            Files.walk(tempDir)
                 .sorted(Comparator.reverseOrder())
                 .forEach(p -> {
                     try { Files.deleteIfExists(p); } catch (IOException ignored) {}
                 });
        }
    }

    @Test
    void packagesZipAndSkipsOnSameChecksum() throws IOException {
        // Arrange test fixture under temp dir
        Path resources = tempDir.resolve("resources");
        Path downloadsDir = resources.resolve("downloaded-files");
        Files.createDirectories(downloadsDir);

        // Create a small file to include in ZIP
        Path sampleFile = downloadsDir.resolve("sample.txt");
        Files.writeString(sampleFile, "hello world");

        // Minimal registry JSON with URL key -> metadata including effective_date
        String registryJson = "{" +
                "\n  \"http://example.com/sample.txt\": {" +
                "\n    \"category\": \"TEST\"," +
                "\n    \"filename\": \"sample.txt\"," +
                "\n    \"effective_date\": [2025, 10, 29]" +
                "\n  }" +
                "\n}";
        Path registryPath = resources.resolve("downloaded-files.json");
        Files.createDirectories(resources);
        Files.writeString(registryPath, registryJson);

        Path outputDir = tempDir.resolve("out");
        Path checksumsPath = tempDir.resolve("checksums.json");
        Files.createDirectories(outputDir);

        // Act: first run should create ZIP
        ReleasePackagerMain packager = new ReleasePackagerMain();
        packager.packageReleases(
                registryPath.toString(),
                downloadsDir.toString(),
                Paths.get("LICENSE").toString(),
                Paths.get("src/main/resources/README-RELEASE.md").toString(),
                checksumsPath.toString(),
                outputDir.toString(),
                false,
                null,
                null,
                null
        );

        // Assert ZIP exists after first run
        Path zipPath = outputDir.resolve("eu-codelists-" + LocalDate.of(2025,10,29) + ".zip");
        Assertions.assertTrue(Files.exists(zipPath), "ZIP should be created on first run");

        // Act: second run should detect same checksum and delete ZIP
        packager.packageReleases(
                registryPath.toString(),
                downloadsDir.toString(),
                Paths.get("LICENSE").toString(),
                Paths.get("src/main/resources/README-RELEASE.md").toString(),
                checksumsPath.toString(),
                outputDir.toString(),
                false,
                null,
                null,
                null
        );

        // Assert ZIP removed due to identical checksum
        Assertions.assertFalse(Files.exists(zipPath), "ZIP should be removed on second run when unchanged");
    }
}


