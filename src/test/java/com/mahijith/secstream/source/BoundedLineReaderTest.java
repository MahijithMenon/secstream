package com.mahijith.secstream.source;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class BoundedLineReaderTest {
  private static List<String> lines(String input, int max) throws Exception {
    BoundedLineReader r = new BoundedLineReader(new StringReader(input), max);
    List<String> out = new ArrayList<>();
    String line;
    while ((line = r.readLine()) != null) {
      out.add(line);
    }
    return out;
  }

  @Test
  void splitsOnLfAndCrLfAndKeepsLastUnterminatedLine() throws Exception {
    assertThat(lines("a\r\nb\n\nc", 10)).containsExactly("a", "b", "", "c");
    assertThat(lines("", 10)).isEmpty();
  }

  @Test
  void overlongLineIsTruncatedToMaxPlusOneAndTheRestSkipped() throws Exception {
    String big = "x".repeat(100_000);
    List<String> result = lines("ok\n" + big + "\nnext\n", 16);
    assertThat(result).containsExactly("ok", "x".repeat(17), "next");
  }
}
