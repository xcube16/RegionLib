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
package cubicchunks.regionlib.region;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.BitSet;
import java.util.Optional;

import cubicchunks.regionlib.CurruptedDataException;
import cubicchunks.regionlib.IEntryLocation;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

public class Region<R extends IRegionLocation<R, L>, L extends IEntryLocation<R, L>> implements Closeable {

	private static final int PRE_DATA_SIZE = Integer.BYTES;
	private static final boolean FORCE_WRITE_LOCATIONS = true;

	private ByteBuffer metaBuf = ByteBuffer.allocate(PRE_DATA_SIZE);
	private ByteBuffer secLocBuf = metaBuf; // currently they are the same size, so just reuse it

	private final SeekableByteChannel file;
	private final int sectorSize;
	private final BitSet usedSectors;

	private int[] entrySectorOffsets;

	public Region(Path path, int entriesPerRegion, int sectorSize) throws IOException {
		this.file = Files.newByteChannel(path, CREATE, READ, WRITE);
		this.sectorSize = sectorSize;
		this.usedSectors = new BitSet(32);
		this.entrySectorOffsets = new int[entriesPerRegion];

		int entryMappingBytes = entriesPerRegion*Integer.BYTES;
		int entryMapSectors = ceilDiv(entryMappingBytes, sectorSize);
		for (int i = 0; i < entryMapSectors; i++) {
			this.usedSectors.set(i, true);
		}

		if(file.size() < entryMappingBytes){
			file.write(ByteBuffer.allocate((int) (entryMappingBytes - file.size())));
		}
		file.position(0);

		ByteBuffer buffer = ByteBuffer.allocate(entryMappingBytes);
		file.read(buffer);
		buffer.flip();
		buffer.asIntBuffer().get(entrySectorOffsets);
	}

	public synchronized void writeEntry(L location, byte[] data) throws IOException {
		int oldSectorLocation = getExistingSectorLocationFor(location);
		int sectorLocation = findSectorFor(data.length, oldSectorLocation);

		int bytesOffset = unpackOffset(sectorLocation)*sectorSize;

		metaBuf.clear();
		metaBuf.putInt(0, data.length);
		file.position(bytesOffset).write(metaBuf);

		file.write(ByteBuffer.wrap(data));

		writeSectorLocationFor(location, sectorLocation);
		updateUsedSectorsFor(oldSectorLocation, sectorLocation);
	}

	private void updateUsedSectorsFor(int oldSectorLocation, int newSectorLocation) {
		// mark old parts as unused
		int oldOffset = unpackOffset(oldSectorLocation);
		int oldSize = unpackSize(oldSectorLocation);
		for (int i = 0; i < oldSize; i++) {
			usedSectors.set(oldOffset + i, false);
		}

		// mark new parts as used, this will work even if they overlap
		int newOffset = unpackOffset(newSectorLocation);
		int newSize = unpackSize(newSectorLocation);
		for (int i = 0; i < newSize; i++) {
			usedSectors.set(newOffset + i, true);
		}
	}

	public synchronized Optional<byte[]> readEntry(L location) throws IOException, CurruptedDataException {
		int sectorLocation = getExistingSectorLocationFor(location);

		if (sectorLocation == 0) {
			return Optional.empty();
		}

		int sectorOffset = unpackOffset(sectorLocation);
		int sectorCount = unpackSize(sectorLocation);

		metaBuf.clear();
		file.position(sectorOffset*sectorSize).read(metaBuf);
		int dataLength = metaBuf.getInt(0);
		if (dataLength > sectorCount*sectorSize) {
			throw new CurruptedDataException("Expected data size max" + sectorCount*sectorSize + " but found " + dataLength);
		}

		ByteBuffer bytes = ByteBuffer.allocate(dataLength);
		file.read(bytes);
		return Optional.of(bytes.array());
	}

	private int findSectorFor(int length, int oldSectorLocation) {
		int entryBytes = length + PRE_DATA_SIZE;
		int oldSectorSize = unpackSize(oldSectorLocation);
		int newSectorSize = getSectorSize(entryBytes);

		if (newSectorSize <= oldSectorSize) {
			return unpackOffset(oldSectorLocation) << 8 | newSectorSize;
		} else {
			// first try at old sector location
			int oldSectorOffset = unpackOffset(oldSectorLocation);
			boolean isEnough = true;
			for (int i = oldSectorOffset + oldSectorSize; i < oldSectorOffset + newSectorSize; i++) {
				if (!isSectorFree(i)) {
					isEnough = false;
					break;
				}
			}
			if (isEnough) {
				return unpackOffset(oldSectorLocation) << 8 | newSectorSize;
			}
			return findNextFree(newSectorSize);
		}
	}

	private int findNextFree(int requestedSize) {
		int currentRun = 0;
		int currentSector = 0;
		do {
			// the first sector is always used no matter what
			currentSector++;
			if (isSectorFree(currentSector)) {
				currentRun++;
			} else {
				currentRun = 0;
			}
		} while (currentRun != requestedSize);

		// go back to the beginning
		currentSector -= currentRun - 1;

		return currentSector << 8 | requestedSize;
	}

	private boolean isSectorFree(int sector) {
		return !usedSectors.get(sector);
	}

	private int getExistingSectorLocationFor(L location) {
		return this.entrySectorOffsets[location.getId()];
	}

	private void writeSectorLocationFor(L location, int sectorLocation) throws IOException {
		int entryId = location.getId();
		this.entrySectorOffsets[entryId] = sectorLocation;
		if (FORCE_WRITE_LOCATIONS) {
			secLocBuf.clear();
			secLocBuf.putInt(0, sectorLocation);
			file.position(entryId*Integer.BYTES).write(secLocBuf);
		}
	}

	private int getSectorSize(int bytes) {
		return ceilDiv(bytes, sectorSize);
	}

	@Override public void close() throws IOException {
		this.file.close();
	}

	private static int unpackOffset(int sectorLocation) {
		return sectorLocation >>> 8;
	}

	public static int unpackSize(int sectorLocation) {
		return sectorLocation & 0xFF;
	}

	private static int ceilDiv(int x, int y) {
		return -Math.floorDiv(-x, y);
	}
}
