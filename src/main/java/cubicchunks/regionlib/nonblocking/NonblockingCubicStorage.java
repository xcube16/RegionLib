/*
 *  This file is part of RegionLib, licensed under the MIT License (MIT).
 *
 *  Copyright (c) 2016 contributors
 *
 *  Permission is hereby granted, free of charge, to any person obtaining a copy
 *  of this software and associated documentation files (the "Software"), to deal
 *  in the Software without restriction, including without limitation the rights
 *  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 *  copies of the Software, and to permit persons to whom the Software is
 *  furnished to do so, subject to the following conditions:
 *
 *  The above copyright notice and this permission notice shall be included in
 *  all copies or substantial portions of the Software.
 *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 *  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 *  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 *  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 *  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 *  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 *  THE SOFTWARE.
 */
package cubicchunks.regionlib.nonblocking;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

import cubicchunks.regionlib.SaveSection;
import cubicchunks.regionlib.impl.EntryLocation2D;
import cubicchunks.regionlib.impl.EntryLocation3D;
import cubicchunks.regionlib.impl.RegionLocation2D;
import cubicchunks.regionlib.impl.RegionLocation3D;

public class NonblockingCubicStorage {

	private static ExecutorService DEFAULT_EXECUTOR = null;

	private final SaveSection<RegionLocation2D, EntryLocation2D> saveSection2D;
	private final SaveSection<RegionLocation3D, EntryLocation3D> saveSection3D;

	private final ExecutorService executor;
	private int inFlight = 0;

	public NonblockingCubicStorage(Path directory) throws IOException {
		this(directory, null);
	}

	public NonblockingCubicStorage(Path directory, ExecutorService executor) throws IOException {
		Files.createDirectories(directory);

		Path part2d = directory.resolve("region2d");
		Files.createDirectory(part2d);

		Path part3d = directory.resolve("region3d");
		Files.createDirectory(part3d);

		this.saveSection2D = new SaveSection<>(part2d);
		this.saveSection3D = new SaveSection<>(part3d);

		this.executor = executor == null ?
			(DEFAULT_EXECUTOR == null ? DEFAULT_EXECUTOR = Executors.newFixedThreadPool(2) : DEFAULT_EXECUTOR)
			: executor;
	}

	public void load3D(int x, int y, int z, Consumer<Optional<byte[]>> callback) {
		inFlight++;
		executor.submit(() -> {
			try {
				callback.accept(saveSection3D.load(new EntryLocation3D(x, y, z)));
			} catch (Exception e) {
				e.printStackTrace();
				callback.accept(Optional.empty());
			}
			inFlight--;
		});
	}

	public void load2D(int x, int z, Consumer<Optional<byte[]>> callback) {
		inFlight++;
		executor.submit(() -> {
			try {
				callback.accept(saveSection2D.load(new EntryLocation2D(x, z)));
			} catch (Exception e) {
				e.printStackTrace();
				callback.accept(Optional.empty());
			}
			inFlight--;
		});
	}

	public void save3D(int x, int y, int z, byte[] data) {
		inFlight++;
		executor.submit(() -> {
			try {
				saveSection3D.save(new EntryLocation3D(x, y, z), data);
			} catch (Exception e) {
				e.printStackTrace();
			}
			inFlight--;
		});
	}

	public void save2D(int x, int z, byte[] data) {
		inFlight++;
		executor.submit(() -> {
			try {
				saveSection2D.save(new EntryLocation2D(x, z), data);
			} catch (Exception e) {
				e.printStackTrace();
			}
			inFlight--;
		});
	}

	public void close() {
		while (inFlight > 0) {
			try {
				Thread.sleep(1); // wait for all IO to stop
			} catch (InterruptedException e) {
				e.printStackTrace(); // don't interrupt me :P
			}
		}

		// TODO: SaveSection does not yet support closing :(
	}
}
