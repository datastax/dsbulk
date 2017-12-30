/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.connectors.cql;

import static com.datastax.dsbulk.commons.cql3.CqlLexer.WS;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.dsbulk.commons.cql3.CqlLexer;
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
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CommonTokenFactory;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.UnbufferedCharStream;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link LineNumberReader reader} for CQL script files.
 *
 * <p>The reader can operate in two modes:
 *
 * <ol>
 *   <li>Single-line: each line is assumed to contain one single, complete statement.
 *   <li>Multi-line: statements can span across multiple lines.
 * </ol>
 *
 * The single-line mode is considerably faster (about 20 times faster) than the multi-line mode.
 *
 * <p>The multi-line mode, however, is the only suitable mode if the script contains statements
 * spanning multiple lines, because it parses the input file and is thus capable of detecting
 * statement boundaries in the file.
 *
 * <p>The multi-line mode is compatible with CQL 3.4.5 (as used in Apache Cassandra 3.11.1). Future
 * versions of CQL might introduce new grammar rules that this reader will not be able to parse; the
 * reader will emit a warning in such situations.
 *
 * <p>Note that both modes discard comments found in the script.
 *
 * <p>This reader also implements {@link Iterable} and can be used in a regular for-loop block to
 * iterate over the statements contained in the underlying CQL script.
 *
 * <p>It also exposes a {@link #statements()} method to read the script using Java 8 streams.
 *
 * <p>This class has a subclass {@link AbstractReactiveCqlScriptReader} that exposes a {@link
 * AbstractReactiveCqlScriptReader#publish() method} to consume the script in a reactive fashion.
 *
 * @see AbstractReactiveCqlScriptReader
 * @see ReactorCqlScriptReader
 * @see RxJavaCqlScriptReader
 * @see <a href="http://cassandra.apache.org/doc/latest/cql/index.html">The Cassandra Query Language
 *     (CQL)</a>
 */
public class CqlScriptReader extends LineNumberReader implements Iterable<Statement> {

  private static final Logger LOGGER = LoggerFactory.getLogger(CqlScriptReader.class);

  private static final char DASH = '-';
  private static final char SLASH = '/';

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

  @Override
  @NotNull
  public Iterator<Statement> iterator() {
    return new Iterator<Statement>() {
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
  }

  /**
   * Reads the entire script and returns a stream of {@link Statement statement}s.
   *
   * @return a stream of {@link Statement statement}s.
   * @throws UncheckedIOException If an I/O error occurs.
   */
  public Stream<Statement> statements() {
    return StreamSupport.stream(
        Spliterators.spliteratorUnknownSize(iterator(), Spliterator.ORDERED | Spliterator.NONNULL),
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
        if (containsStatement(line)) {
          break;
        }
      }
      return line == null ? null : new SimpleStatement(line);
    }

    private boolean containsStatement(String line) {
      for (int i = 0; i < line.length(); i++) {
        char c = line.charAt(i);
        if (!Character.isWhitespace(c)) {
          return c != DASH && c != SLASH;
        }
      }
      return false;
    }
  }

  private class MultiLineParser implements Parser {

    private CqlLexer lexer;

    private MultiLineParser() {
      lexer = new CqlLexer(new UnbufferedCharStream(CqlScriptReader.this));
      lexer.setTokenFactory(new CommonTokenFactory(true));
      lexer.removeErrorListeners();
      lexer.addErrorListener(
          new BaseErrorListener() {
            @Override
            public void syntaxError(
                Recognizer<?, ?> recognizer,
                Object offendingSymbol,
                int line,
                int charPositionInLine,
                String msg,
                RecognitionException e) {
              LOGGER.warn("{}:{}: {}", line, charPositionInLine, msg);
            }
          });
    }

    @Override
    public Statement parseNext() {
      StringBuilder buffer = new StringBuilder();
      StringBuilder whitespace = new StringBuilder();
      boolean batchMode = false;
      BatchStatement batch = null;
      while (true) {
        Token t = lexer.nextToken();
        switch (t.getType()) {
          case CqlLexer.K_BEGIN:
            batchMode = true;
            break;
          case CqlLexer.K_APPLY:
            batchMode = false;
            break;
          case CqlLexer.K_UNLOGGED:
            if (batchMode) {
              batch = new BatchStatement(BatchStatement.Type.UNLOGGED);
            }
            break;
          case CqlLexer.K_COUNTER:
            if (batchMode) {
              batch = new BatchStatement(BatchStatement.Type.COUNTER);
            }
            break;
          case CqlLexer.K_BATCH:
            if (batchMode && batch == null) {
              batch = new BatchStatement(BatchStatement.Type.UNLOGGED);
            }
            break;
          case CqlLexer.EOS:
            if (batchMode) {
              assert batch != null;
              batch.add(new SimpleStatement(buffer.toString()));
              buffer.setLength(0);
              break;
            } else if (batch != null) {
              return batch;
            } else {
              return new SimpleStatement(buffer.toString());
            }
          case CqlLexer.EOF:
            if (buffer.length() > 0) {
              return new SimpleStatement(buffer.toString());
            }
            return null;
          case CqlLexer.COMMENT:
          case CqlLexer.MULTILINE_COMMENT:
            break;
          case WS:
            if (buffer.length() > 0) {
              whitespace.append(t.getText());
            }
            break;
          default:
            if (whitespace.length() > 0) {
              buffer.append(whitespace);
            }
            whitespace.setLength(0);
            buffer.append(t.getText());
        }
      }
    }
  }
}
