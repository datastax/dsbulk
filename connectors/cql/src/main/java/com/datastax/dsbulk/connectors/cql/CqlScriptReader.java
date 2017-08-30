/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.connectors.cql;

import static com.datastax.driver.core.BatchStatement.Type.COUNTER;
import static com.datastax.driver.core.BatchStatement.Type.LOGGED;
import static com.datastax.driver.core.BatchStatement.Type.UNLOGGED;
import static com.google.common.base.Preconditions.checkArgument;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import java.io.IOException;
import java.io.LineNumberReader;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * A {@link LineNumberReader reader} for CQL script files.
 *
 * <p>The reader can operate in two modes:
 *
 * <ol>
 *   <li>Single-line: each line is assumed to contain one single, complete statement.
 *   <li>Multi-line: statements can span accross multiple lines.
 * </ol>
 *
 * The single-line mode is more reliable and considerably faster (about 20 times faster) then the
 * multi-line mode.
 *
 * <p>The multi-line mode, however, is the only suitable mode if the script contains statements
 * spanning multiple lines, because it actually performs a high-level parsing and is capable of
 * detecting statement boundaries in the file. It is compatible with all versions of the CQL
 * grammar.
 *
 * <p>This reader also exposes methods to read the script using both Java 8 streams and Reactive
 * streams.
 */
public class CqlScriptReader extends LineNumberReader {

  private static final char SEMICOLON = ';';
  private static final char DASH = '-';
  private static final char SLASH = '/';
  private static final char STAR = '*';
  private static final char SINGLE_QUOTE = '\'';
  private static final char DOUBLE_QUOTE = '"';
  private static final char DOLLAR = '$';
  private static final char LINE_FEED = '\n';
  private static final char CARRIAGE_RETURN = '\r';

  private final Parser parser;

  /**
   * Creates a new instance in single-line mode.
   *
   * @param in the script to read.
   */
  public CqlScriptReader(Reader in) {
    super(in);
    parser = new SingleLineParser();
  }

  /**
   * Creates a new instance.
   *
   * @param in the script to read.
   * @param multiLine whether to use multi-line mode or not.
   */
  public CqlScriptReader(Reader in, boolean multiLine) {
    super(in);
    this.parser = multiLine ? new MultiLineParser() : new SingleLineParser();
  }

  /**
   * Creates a new instance.
   *
   * @param in the script to read.
   * @param multiLine whether to use multi-line mode or not.
   * @param size the size of the buffer.
   */
  public CqlScriptReader(Reader in, boolean multiLine, int size) {
    super(in, size);
    this.parser = multiLine ? new MultiLineParser() : new SingleLineParser();
  }

  /**
   * Reads the entire script and returns a stream of {@link Statement statement}s.
   *
   * @return a stream of {@link Statement statement}s.
   */
  public Stream<Statement> readStream() {
    Iterator<Statement> iter =
        new Iterator<Statement>() {
          Statement nextStatement = null;

          @Override
          public boolean hasNext() {
            if (nextStatement != null) {
              return true;
            } else {
              try {
                nextStatement = readStatement();
                return (nextStatement != null);
              } catch (IOException e) {
                throw new UncheckedIOException(e);
              }
            }
          }

          @Override
          public Statement next() {
            if (nextStatement != null || hasNext()) {
              Statement stmt = nextStatement;
              nextStatement = null;
              return stmt;
            } else {
              throw new NoSuchElementException();
            }
          }
        };
    return StreamSupport.stream(
        Spliterators.spliteratorUnknownSize(iter, Spliterator.ORDERED | Spliterator.NONNULL),
        false);
  }

  /**
   * Reads and returns the next {@link Statement statement} in the script.
   *
   * @return the next {@link Statement statement} in the script.
   * @throws IOException If an I/O error occurs.
   */
  public Statement readStatement() throws IOException {
    return parser.parseNext();
  }

  private interface Parser {
    Statement parseNext() throws IOException;
  }

  private class SingleLineParser implements Parser {

    @Override
    public Statement parseNext() throws IOException {
      String line;
      while ((line = readLine()) != null) {
        if (containsStatement(line)) break;
      }
      return line == null ? null : new SimpleStatement(line);
    }

    private boolean containsStatement(String line) {
      for (int i = 0; i < line.length(); i++) {
        char c = line.charAt(i);
        if (!Character.isWhitespace(c)) return c != DASH && c != SLASH;
      }
      return false;
    }
  }

  private static class MultiLineParseContext {

    private BatchStatement batch;
  }

  private class MultiLineParser implements Parser {

    @Override
    public Statement parseNext() throws IOException {
      StringBuilder sb = new StringBuilder();
      int current;
      MultiLineParsePhase phase = MultiLineParsePhase.WHITESPACE;
      MultiLineParseContext ctx = new MultiLineParseContext();
      int l = 0, col = 0;
      while ((current = read()) != -1) {
        char c = (char) current;
        col = l < getLineNumber() ? 1 : col + 1;
        l = getLineNumber();
        phase = phase.parseAndAdvance(c, sb, l, col, ctx);
        if (phase == MultiLineParsePhase.STATEMENT_END) break;
      }
      if (phase != MultiLineParsePhase.WHITESPACE && phase != MultiLineParsePhase.STATEMENT_END)
        throw new IllegalStateException(String.format("Premature EOF at line %d", getLineNumber()));
      if (ctx.batch != null) return ctx.batch;
      return sb.length() == 0 ? null : new SimpleStatement(sb.toString());
    }
  }

  private enum MultiLineParsePhase {
    WHITESPACE {
      @Override
      MultiLineParsePhase parseAndAdvance(
          char c, StringBuilder sb, int line, int col, MultiLineParseContext ctx) {
        switch (c) {
          case SLASH:
            return COMMENT_START;
          case DASH:
            return SINGLE_LINE_COMMENT;
          default:
            if (!Character.isWhitespace(c)) {
              sb.append(c);
              return STATEMENT_START;
            }
            return WHITESPACE;
        }
      }
    },

    STATEMENT_START {
      @Override
      MultiLineParsePhase parseAndAdvance(
          char c, StringBuilder sb, int line, int col, MultiLineParseContext ctx) {
        switch (c) {
          case SINGLE_QUOTE:
            sb.append(c);
            return STRING_LITERAL;
          case DOUBLE_QUOTE:
            sb.append(c);
            return QUOTED_IDENTIFIER;
          case SEMICOLON:
            sb.append(c);
            if (ctx.batch == null) return STATEMENT_END;
            ctx.batch.add(new SimpleStatement(sb.toString()));
            sb.setLength(0);
            return WHITESPACE;
          case DOLLAR:
            sb.append(c);
            return DOLLAR_QUOTED_STRING_START;
          default:
            sb.append(c);
            if (sb.length() == 5 && endsWithIgnoreCase(sb, "BEGIN")) return BATCH_START;
            if (sb.length() == 3 && endsWithIgnoreCase(sb, "END")) return BATCH_END;
            return STATEMENT_START;
        }
      }
    },

    STATEMENT_END {
      @Override
      MultiLineParsePhase parseAndAdvance(
          char c, StringBuilder sb, int line, int col, MultiLineParseContext ctx) {
        checkArgument(c == SEMICOLON, "Expecting ';', got %s (at line %s, col %s)", c, line, col);
        sb.append(c);
        return WHITESPACE;
      }
    },

    COMMENT_START {
      @Override
      MultiLineParsePhase parseAndAdvance(
          char c, StringBuilder sb, int line, int col, MultiLineParseContext ctx) {
        checkArgument(
            c == SLASH || c == STAR,
            "Expecting '/' or '*', got %s (at line %s, col %s)",
            c,
            line,
            col);
        return c == SLASH ? SINGLE_LINE_COMMENT : MULTI_LINE_COMMENT_START;
      }
    },

    SINGLE_LINE_COMMENT {
      @Override
      MultiLineParsePhase parseAndAdvance(
          char c, StringBuilder sb, int line, int col, MultiLineParseContext ctx) {
        return c == LINE_FEED || c == CARRIAGE_RETURN ? WHITESPACE : SINGLE_LINE_COMMENT;
      }
    },

    MULTI_LINE_COMMENT_START {
      @Override
      MultiLineParsePhase parseAndAdvance(
          char c, StringBuilder sb, int line, int col, MultiLineParseContext ctx) {
        return c == STAR ? MULTI_LINE_COMMENT_END : MULTI_LINE_COMMENT_START;
      }
    },

    MULTI_LINE_COMMENT_END {
      @Override
      MultiLineParsePhase parseAndAdvance(
          char c, StringBuilder sb, int line, int col, MultiLineParseContext ctx) {
        return c == SLASH ? WHITESPACE : MULTI_LINE_COMMENT_START;
      }
    },

    STRING_LITERAL {
      @Override
      MultiLineParsePhase parseAndAdvance(
          char c, StringBuilder sb, int line, int col, MultiLineParseContext ctx) {
        sb.append(c);
        return c == SINGLE_QUOTE ? STATEMENT_START : STRING_LITERAL;
      }
    },

    QUOTED_IDENTIFIER {
      @Override
      MultiLineParsePhase parseAndAdvance(
          char c, StringBuilder sb, int line, int col, MultiLineParseContext ctx) {
        sb.append(c);
        return c == DOUBLE_QUOTE ? STATEMENT_START : QUOTED_IDENTIFIER;
      }
    },

    DOLLAR_QUOTED_STRING_START {
      @Override
      MultiLineParsePhase parseAndAdvance(
          char c, StringBuilder sb, int line, int col, MultiLineParseContext ctx) {
        sb.append(c);
        return c == DOLLAR ? DOLLAR_QUOTED_STRING_CONTENTS : STATEMENT_START;
      }
    },

    DOLLAR_QUOTED_STRING_CONTENTS {
      @Override
      MultiLineParsePhase parseAndAdvance(
          char c, StringBuilder sb, int line, int col, MultiLineParseContext ctx) {
        sb.append(c);
        return c == DOLLAR ? DOLLAR_QUOTED_STRING_END : DOLLAR_QUOTED_STRING_CONTENTS;
      }
    },

    DOLLAR_QUOTED_STRING_END {
      @Override
      MultiLineParsePhase parseAndAdvance(
          char c, StringBuilder sb, int line, int col, MultiLineParseContext ctx) {
        sb.append(c);
        return c == DOLLAR ? STATEMENT_START : DOLLAR_QUOTED_STRING_CONTENTS;
      }
    },

    BATCH_START {
      @Override
      MultiLineParsePhase parseAndAdvance(
          char c, StringBuilder sb, int line, int col, MultiLineParseContext ctx) {
        sb.append(c);
        if (endsWithIgnoreCase(sb, "UNLOGGED")) {
          ctx.batch = new BatchStatement(UNLOGGED);
        } else if (endsWithIgnoreCase(sb, "LOGGED")) {
          ctx.batch = new BatchStatement(LOGGED);
        } else if (endsWithIgnoreCase(sb, "COUNTER")) {
          ctx.batch = new BatchStatement(COUNTER);
        } else if (endsWithIgnoreCase(sb, "BATCH")) {
          if (ctx.batch == null) {
            ctx.batch = new BatchStatement();
          }
          sb.setLength(0);
          return WHITESPACE;
        }
        return BATCH_START;
      }
    },

    BATCH_END {
      @Override
      MultiLineParsePhase parseAndAdvance(
          char c, StringBuilder sb, int line, int col, MultiLineParseContext ctx) {
        sb.append(c);
        return c == SEMICOLON ? STATEMENT_END : BATCH_END;
      }
    };

    abstract MultiLineParsePhase parseAndAdvance(
        char c, StringBuilder sb, int line, int col, MultiLineParseContext ctx);
  }

  private static boolean endsWithIgnoreCase(StringBuilder sb, String suffix) {
    int length = sb.length();
    int span = suffix.length();
    if (length >= span) {
      for (int i = 1; i <= span; i++) {
        char c0 = sb.charAt(length - i);
        char c1 = suffix.charAt(span - i);
        if (Character.toUpperCase(c0) != c1) return false;
      }
      return true;
    }
    return false;
  }
}
