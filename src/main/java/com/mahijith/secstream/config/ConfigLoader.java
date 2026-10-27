package com.mahijith.secstream.config;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.List;
import java.util.function.Function;

/** Reads YAML strictly (unknown keys are errors, so typos fail loudly) and validates it. */
public final class ConfigLoader {
  private static final int MAX_CONFIG_BYTES = 1024 * 1024;

  private static final ObjectMapper YAML =
      YAMLMapper.builder()
          .enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
          .enable(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES)
          .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
          .build();

  private ConfigLoader() {}

  public static SecStreamConfig load(Path path, Function<String, String> env)
      throws ConfigException {
    String yaml;
    try {
      if (Files.size(path) > MAX_CONFIG_BYTES) {
        throw new ConfigException(List.of(path + ": larger than " + MAX_CONFIG_BYTES + " bytes"));
      }
      yaml = Files.readString(path);
    } catch (NoSuchFileException e) {
      throw new ConfigException(List.of("config file not found: " + path));
    } catch (IOException e) {
      throw new ConfigException(List.of("cannot read " + path + ": " + e.getMessage()));
    }
    return parse(yaml, env);
  }

  public static SecStreamConfig parse(String yaml, Function<String, String> env)
      throws ConfigException {
    SecStreamConfig config;
    try {
      config = YAML.readValue(yaml, SecStreamConfig.class);
    } catch (UnrecognizedPropertyException e) {
      throw new ConfigException(
          List.of(
              "unknown key '"
                  + e.getPropertyName()
                  + "' at "
                  + path(e)
                  + "; known keys: "
                  + e.getKnownPropertyIds()));
    } catch (JacksonException e) {
      throw new ConfigException(List.of(e.getOriginalMessage() + " at " + path(e)));
    }
    if (config == null) {
      throw new ConfigException(List.of("configuration is empty"));
    }
    List<String> problems = ConfigValidator.validate(config, env);
    if (!problems.isEmpty()) {
      throw new ConfigException(problems);
    }
    return config;
  }

  private static String path(JacksonException e) {
    String path = e instanceof JsonMappingException m ? m.getPathReference() : null;
    var loc = e.getLocation();
    String line = loc == null ? "" : " (line " + loc.getLineNr() + ")";
    return (path == null ? "document" : path.replace("com.mahijith.secstream.config.", "")) + line;
  }
}
