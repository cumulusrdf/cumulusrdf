package edu.kit.aifb.cumulus;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import javax.servlet.ServletOutputStream;

/**
 * Stub outputstream used in tests.
 * 
 * @author Andrea Gazzarini
 * @since 1.0.1
 */
public class StubServletOutputStream extends ServletOutputStream {
		
	private final OutputStream _out;

	/**
	 * Builds a new (fake) outputstream with a given outputFile.
	 * 
	 * @param outputFile the output file.
	 */
	public StubServletOutputStream(final OutputStream outputStream) {
		_out = outputStream;
	}

	/**
	 * Builds a new (fake) outputstream with a given outputFile.
	 * 
	 * @param outputFile the output file.
	 */
	public StubServletOutputStream(final File outputFile) {
		try {
			_out = new FileOutputStream(outputFile);
		} catch (final FileNotFoundException exception) {
			throw new RuntimeException(exception);
		}
	}

	@Override
	public void write(final int b) throws IOException {
		_out.write(b);
	}

	@Override
	public void close() throws IOException {
		_out.close();
	}
}