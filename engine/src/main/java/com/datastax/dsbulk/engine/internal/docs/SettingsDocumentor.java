/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.docs;

import static com.datastax.dsbulk.engine.internal.utils.SettingsUtils.CONFIG_FILE_OPTION;
import static com.datastax.dsbulk.engine.internal.utils.SettingsUtils.DEFAULT;
import static com.datastax.dsbulk.engine.internal.utils.SettingsUtils.GROUPS;
import static com.datastax.dsbulk.engine.internal.utils.SettingsUtils.LONG_TO_SHORT_OPTIONS;

import com.datastax.dsbulk.commons.internal.config.ConfigUtils;
import com.datastax.dsbulk.commons.internal.utils.StringUtils;
import com.datastax.dsbulk.engine.internal.utils.SettingsUtils;
import com.datastax.dsbulk.engine.internal.utils.SettingsUtils.Group;
import com.google.common.base.CharMatcher;
import com.typesafe.config.ConfigRenderOptions;
import com.typesafe.config.ConfigValue;
import com.typesafe.config.ConfigValueType;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

public class SettingsDocumentor {
  private static final String TITLE = "DataStax Bulk Loader Options";

  public static void main(String[] args) throws IOException {
    new SettingsDocumentor(Paths.get(args[0]));
  }

  @SuppressWarnings("WeakerAccess")
  SettingsDocumentor(Path filePath) throws IOException {
    Files.createDirectories(filePath.getParent());
    try (PrintWriter out =
        new PrintWriter(
            new BufferedWriter(
                new OutputStreamWriter(
                    new FileOutputStream(filePath.toFile()), StandardCharsets.UTF_8)))) {
      // Print page title
      out.printf(
          "# %s%n%n"
              + "*NOTE:* The long options described here can be persisted in `conf/application.conf` "
              + "and thus permanently override defaults and avoid specifying options on the command "
              + "line.%n%n"
              + "A template configuration file can be found [here](./application.template.conf).%n%n"
              + "## Sections%n%n",
          TITLE);

      // Print links to relevant sections.
      for (String groupName : GROUPS.keySet()) {
        out.printf(
            "%s<a href=\"#%s\">%s Settings</a><br>%n",
            tocIndent(groupName), groupName, prettifyName(groupName));
      }

      // Walk through groups, emitting a group title followed by settings
      // for each group.
      for (Map.Entry<String, Group> groupEntry : GROUPS.entrySet()) {
        String groupName = groupEntry.getKey();
        out.printf("<a name=\"%s\"></a>%n", groupName);
        out.printf("%s %s Settings%n%n", titleFormat(groupName), prettifyName(groupName));
        if (!groupName.equals("Common")) {
          out.printf("%s%n%n", getSanitizedDescription(DEFAULT.getValue(groupName)));
        } else {
          // Emit the help for the "-f" option in the Common section.
          out.printf(
              "#### -f _&lt;%s&gt;_%n%n%s%n%n",
              StringUtils.htmlEscape("string"), CONFIG_FILE_OPTION.getDescription());
        }
        for (String settingName : groupEntry.getValue().getSettings()) {
          ConfigValue settingValue = DEFAULT.getValue(settingName);
          String shortOpt =
              LONG_TO_SHORT_OPTIONS.containsKey(settingName)
                  ? "-" + LONG_TO_SHORT_OPTIONS.get(settingName) + ","
                  : "";
          out.printf(
              "#### %s--%s _&lt;%s&gt;_%n%n%s%n%n",
              shortOpt,
              settingName,
              StringUtils.htmlEscape(ConfigUtils.getTypeString(DEFAULT, settingName)),
              getSanitizedDescription(settingValue));
        }
      }
    }
  }

  /**
   * When emitting a link to a group in the toc section, emit it based on how nested the group
   * specification is (e.g. driver.auth will be indented more than driver).
   *
   * @param groupName Name of settings group
   * @return As many non-breaking-whitespaces as is needed for this group.
   */
  private static String tocIndent(String groupName) {
    return StringUtils.nCopies("&nbsp;&nbsp;&nbsp;", CharMatcher.is('.').countIn(groupName));
  }

  /**
   * When emitting a section title (just before the relevant settings), format its font size based
   * on its nesting (e.g. driver.auth may be an h3, while driver may be an h2).
   *
   * @param groupName Name of settings group
   * @return format string (markdown headers)
   */
  private static String titleFormat(String groupName) {
    return StringUtils.nCopies("#", CharMatcher.is('.').countIn(groupName) + 2);
  }

  /**
   * Convert the group name to a prettier representation (e.g. driver.auth => Driver Auth).
   *
   * @param groupName Name of settings group
   * @return pretty representation of the group name.
   */
  private static String prettifyName(String groupName) {
    return Arrays.stream(groupName.split("\\."))
        .map(StringUtils::ucfirst)
        .collect(Collectors.joining(" "));
  }

  /**
   * Process the comment of a particular setting to produce markdown for rendering in the doc.
   *
   * @param value ConfigValue object for the desired setting.
   * @return markdown string.
   */
  private static String getSanitizedDescription(ConfigValue value) {
    // We collect all the lines in the comment block for the setting and join with newlines.
    // However, each line starts with a single leading space that we want to remove.
    String desc =
        value
            .origin()
            .comments()
            .stream()
            .filter(line -> !SettingsUtils.isAnnotation(line))
            .map(s -> s.length() > 0 ? s.substring(1) : s)
            .collect(Collectors.joining("\n"));
    if (value.valueType() != ConfigValueType.OBJECT) {
      String defaultValue = value.render(ConfigRenderOptions.concise()).replace("*", "\\*");
      if (defaultValue.equals("\"\"")) {
        defaultValue = "&lt;unspecified&gt;";
      }
      desc += String.format("%n%nDefault: **%s**.", defaultValue);
    }
    return desc;
  }
}
