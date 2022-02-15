package redis.clients.jedis.client;

import java.io.IOException;
import java.io.PrintWriter;

import org.jline.reader.EndOfFileException;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.UserInterruptException;
import org.jline.reader.impl.completer.StringsCompleter;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;

public class JedisCmdClient {

	public static void main(String[] args) throws IOException {
		if (args == null || args.length < 1) {
			System.err.println(""
					+ "=================================" + System.lineSeparator()
					+ "usage:" + System.lineSeparator()
					+ "e.g. config.properties" + System.lineSeparator()
					+ "=================================");
			System.exit(-1);
		}

		try (Terminal terminal = TerminalBuilder.terminal()) {
			JedisCmdExecutor jedisCmdExecutor = new JedisCmdExecutor(new JedisClientConfig(args[0]));
			PrintWriter printWriter = terminal.writer();
			printWriter.println("============Welcome!=============");
			printWriter.flush();
			LineReader lineReader = LineReaderBuilder.builder()
					.terminal(terminal)
					.completer(new StringsCompleter(jedisCmdExecutor.commands()))
					.build();
			String line;
			while ((line = lineReader.readLine("Type here> ")) != null) {
				String[] words = line.split(" ", -1);
				if (words.length == 0) {
					continue;
				}
				if ("exit".equalsIgnoreCase(words[0]) && words.length == 1) {
					printWriter.println("Exiting... ");
					printWriter.flush();
					break;
				}
				try {
					jedisCmdExecutor.run(words, printWriter);
				} catch (Throwable ex) {
					printWriter.println("Error: " + ex);
					printWriter.flush();
				}
			}
			printWriter.println("Bye bye");
			printWriter.flush();
		} catch (UserInterruptException | EndOfFileException ignored) {
			// Ctrl+C Ctrl+D
		}
	}
}
