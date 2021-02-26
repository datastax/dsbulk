/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.dsbulk.runner.ccm;

import static com.datastax.oss.dsbulk.runner.ExitStatus.STATUS_OK;
import static com.datastax.oss.dsbulk.runner.tests.EndToEndUtils.assertStatus;
import static com.datastax.oss.dsbulk.runner.tests.EndToEndUtils.validateOutputFiles;
import static com.datastax.oss.dsbulk.tests.assertions.TestAssertions.assertThat;
import static com.datastax.oss.dsbulk.tests.logging.StreamType.STDERR;
import static com.datastax.oss.dsbulk.tests.utils.StringUtils.quoteJson;
import static org.assertj.core.api.Assertions.entry;

import com.datastax.dse.driver.api.core.data.geometry.LineString;
import com.datastax.dse.driver.api.core.data.geometry.Point;
import com.datastax.dse.driver.api.core.data.geometry.Polygon;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.data.GettableByName;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.driver.shaded.guava.common.collect.Sets;
import com.datastax.oss.dsbulk.runner.DataStaxBulkLoader;
import com.datastax.oss.dsbulk.runner.ExitStatus;
import com.datastax.oss.dsbulk.tests.ccm.CCMCluster;
import com.datastax.oss.dsbulk.tests.ccm.CCMCluster.Type;
import com.datastax.oss.dsbulk.tests.ccm.annotations.CCMConfig;
import com.datastax.oss.dsbulk.tests.ccm.annotations.CCMRequirements;
import com.datastax.oss.dsbulk.tests.ccm.annotations.CCMVersionRequirement;
import com.datastax.oss.dsbulk.tests.logging.LogCapture;
import com.datastax.oss.dsbulk.tests.logging.LogInterceptor;
import com.datastax.oss.dsbulk.tests.logging.StreamCapture;
import com.datastax.oss.dsbulk.tests.logging.StreamInterceptor;
import java.util.List;
import org.assertj.core.util.Lists;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

@CCMConfig(numberOfNodes = 1)
@CCMRequirements(
    compatibleTypes = Type.DSE,
    versionRequirements = @CCMVersionRequirement(type = Type.DSE, min = "5.0"))
@Tag("medium")
class GeoTypesEndToEndCCMIT extends EndToEndCCMITBase {

  private static final Point POINT_1 = Point.fromWellKnownText("POINT (-1.1 -2.2)");
  private static final Point POINT_2 = Point.fromWellKnownText("POINT (0 0)");

  private static final LineString LINE_1 =
      LineString.fromWellKnownText("LINESTRING (30 10, 10 30, 40 40)");
  private static final LineString LINE_2 = LineString.fromWellKnownText("LINESTRING EMPTY");

  private static final Polygon POLYGON_1 =
      Polygon.fromWellKnownText("POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))");

  private final LogInterceptor logs;
  private final StreamInterceptor stderr;
  private final String keyspace;
  private UserDefinedType udt;

  GeoTypesEndToEndCCMIT(
      CCMCluster ccm,
      CqlSession session,
      @LogCapture(loggerName = "com.datastax.oss.dsbulk") LogInterceptor logs,
      @StreamCapture(STDERR) StreamInterceptor stderr) {
    super(ccm, session);
    this.logs = logs;
    this.stderr = stderr;
    keyspace =
        session
            .getKeyspace()
            .map(CqlIdentifier::asInternal)
            .orElseThrow(IllegalStateException::new);
  }

  @BeforeAll
  void createTables() {
    session.execute(
        SimpleStatement.newInstance(
                "CREATE TYPE IF NOT EXISTS geo_udt ("
                    + "point 'PointType', "
                    + "line 'LineStringType', "
                    + "polygon 'PolygonType', "
                    + "points frozen<list<'PointType'>>, "
                    + "lines frozen<set<'LineStringType'>>, "
                    + "polygons frozen<map<int,'PolygonType'>>"
                    + ")")
            .setExecutionProfile(SessionUtils.slowProfile(session)));
    session.execute(
        SimpleStatement.newInstance(
                "CREATE TABLE IF NOT EXISTS geo_table ("
                    + "pk int PRIMARY KEY, "
                    + "point 'PointType', "
                    + "line 'LineStringType', "
                    + "polygon 'PolygonType', "
                    + "points list<'PointType'>, "
                    + "lines set<'LineStringType'>, "
                    + "polygons map<int,'PolygonType'>, "
                    + "udt geo_udt"
                    + ")")
            .setExecutionProfile(SessionUtils.slowProfile(session)));
    udt =
        session
            .getMetadata()
            .getKeyspace(keyspace)
            .flatMap(ks -> ks.getUserDefinedType("geo_udt"))
            .orElseThrow(IllegalStateException::new);
  }

  @ParameterizedTest
  @CsvSource({
    "csv,WKT,BASE64",
    "csv,JSON,BASE64",
    "csv,WKB,BASE64",
    "csv,WKB,HEX",
    "json,WKT,BASE64",
    "json,JSON,BASE64",
    "json,WKB,BASE64",
    "json,WKB,HEX",
  })
  void csv_full_unload_and_load_geo_types(String connector, String geoFormat, String binaryFormat)
      throws Exception {

    truncateTable();
    populateTable();

    List<String> args =
        Lists.newArrayList(
            "unload",
            "-c",
            connector,
            "-k",
            keyspace,
            "-t",
            "geo_table",
            "-m",
            "pk,point,line,polygon,points,lines,polygons,udt",
            "-url",
            quoteJson(unloadDir),
            "--codec.geo",
            geoFormat,
            "--codec.binary",
            binaryFormat,
            "--connector.csv.maxConcurrentFiles",
            "1");

    if (connector.equals("csv")) {
      args.add("-header");
      args.add("false");
    }

    ExitStatus status = new DataStaxBulkLoader(addCommonSettings(args)).run();
    assertStatus(status, STATUS_OK);
    validateOutputFiles(1, unloadDir);
    validateLogs();

    truncateTable();

    args =
        Lists.newArrayList(
            "load",
            "-c",
            connector,
            "-k",
            keyspace,
            "-t",
            "geo_table",
            "-m",
            "pk,point,line,polygon,points,lines,polygons,udt",
            "--codec.geo",
            geoFormat,
            "--codec.binary",
            binaryFormat,
            "-url",
            quoteJson(unloadDir));

    if (connector.equals("csv")) {
      args.add("-header");
      args.add("false");
    }

    status = new DataStaxBulkLoader(addCommonSettings(args)).run();
    assertStatus(status, STATUS_OK);
    validateTableContents();
    validateLogs();
  }

  private void truncateTable() {
    session.execute(
        SimpleStatement.newInstance("TRUNCATE geo_table")
            .setExecutionProfile(SessionUtils.slowProfile(session)));
  }

  private void populateTable() {
    session.execute(
        "INSERT INTO geo_table (pk, point, line, polygon, points, lines, polygons, udt) "
            + "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        0,
        POINT_1,
        LINE_1,
        POLYGON_1,
        Lists.newArrayList(POINT_1, POINT_2),
        Sets.newHashSet(LINE_1, LINE_2),
        ImmutableMap.of(1, POLYGON_1),
        udt.newValue(
            POINT_1,
            LINE_1,
            POLYGON_1,
            Lists.newArrayList(POINT_1, POINT_2),
            Sets.newHashSet(LINE_1, LINE_2),
            ImmutableMap.of(1, POLYGON_1)));
  }

  private void validateTableContents() {
    Row row = session.execute("SELECT * from geo_table WHERE pk = 0").one();
    assertThat(row).isNotNull();
    validateData(row);
    UdtValue udtValue = row.getUdtValue("udt");
    assertThat(udtValue).isNotNull();
    validateData(udtValue);
  }

  private void validateData(GettableByName data) {
    assertThat(data.get("point", Point.class)).isEqualTo(POINT_1);
    assertThat(data.get("line", LineString.class)).isEqualTo(LINE_1);
    assertThat(data.get("polygon", Polygon.class)).isEqualTo(POLYGON_1);
    assertThat(data.getList("points", Point.class)).containsExactly(POINT_1, POINT_2);
    assertThat(data.getSet("lines", LineString.class))
        .usingElementComparator(this::compareLines)
        .containsExactlyInAnyOrder(LINE_1, LINE_2);
    assertThat(data.getMap("polygons", Integer.class, Polygon.class))
        .containsOnly(entry(1, POLYGON_1));
  }

  private void validateLogs() {
    assertThat(logs).hasMessageContaining("completed successfully");
    assertThat(stderr.getStreamAsStringPlain()).contains("completed successfully");
    logs.clear();
    stderr.clear();
  }

  private int compareLines(LineString l1, LineString l2) {
    // empty line strings never compare as equal
    if (l1.getPoints().isEmpty()) {
      return l2.getPoints().isEmpty() ? 0 : 1;
    }
    return l1.equals(l2) ? 0 : 1;
  }
}
