/*
 * Members (Section 1)
 * 1. Kittinun Aukkapinyo	5888006
 * 2. Chatchawan Kotarasu	5888084
 * 3. Thatchapon Unprasert	5888220
 */

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.LinkedList;
import java.util.List;
import java.util.Vector;

public class VBIndex implements BaseIndex {

	/**
	 * This method is used to return a PostingList which is read from the FileChannel fc.
	 * <br>It is used Variable Byte decoding, so it reads as ByteStream.
	 * @param fc a FileChannel that connects to a file (index file)
	 * @return <b>PostingList</b> a posting list contains termId and a list of docIds
	 * 
	 */
	@Override
	public PostingList readPosting(FileChannel fc) {
		try {
			ByteBuffer buffer = ByteBuffer.allocate(1); // TODO: possible overkill
			Vector<Integer> numbers = new Vector<Integer>(); // To store numbers decoded from ByteStream
			int n = 0, docFreq = 0;
			// Start changing from gaps to real numbers, stored in a list
			while (fc.read(buffer) != -1) {
				buffer.rewind();
				Byte b = buffer.get(); // Get a byte, b, from ByteStream
				if ((b & 0xff) < 128) { // Check if the first bit of b is 0 which means not ending for an encoded byte
					n = 128 * n + b;
				} else { // If the first bit of b is 1 meaning ending for an encoded byte
					n = 128 * n + ((b & 0xff) - 128); // Extract the first bit of b out and calculate a number
					numbers.add(n); // Store a decoded number to a vector
					n = 0;
					if (numbers.size() == 2) // Check if it decodes document frequency
						docFreq = numbers.lastElement();
					if (numbers.size() - 2 == docFreq) // Check if it is the last element of {docId}
						break;
				}
				buffer.clear();
			}
			if (!numbers.isEmpty()) {
				numbers.trimToSize();
				List<Integer> postings = docGap(numbers.subList(2, numbers.size()), true); // Convert a sequence of docId in term of docGap to a normal sequence
				return new PostingList(numbers.get(0), postings); // Return PostingList
			}
			return null;
		} catch (IOException e) {
			return null;
		}

	}

	/**
	 * This method is used to write a PostingList p to a FileChannel fc.
	 * <br>It is used Variable Byte encoding with p before writing to fc.
	 * @param fc a FileChannel that connects to a file (index file)
	 * @param p a PostingList written to fc
	 */
	@Override
	public void writePosting(FileChannel fc, PostingList p) {
		List<Byte> encodedTermId = VBEncodeNumber(p.getTermId()); // Encoding a termID with VBEncoding
		List<Byte> encodedDocFreq = VBEncodeNumber(p.getList().size()); // Encoding a docFreq with VBEncoding
		List<Byte> encodedDocIds = VBEncode(docGap(p.getList(), false)); // Convert a sequence of docIds to be a sequence of docGap and then VBEncoding it

		ByteBuffer byteBuffer = ByteBuffer
				.allocate(encodedTermId.size() + encodedDocFreq.size() + encodedDocIds.size()); // Allocate the buffer size
		
		/*Put all encoded numbers into ByteBuffer*/
		for (Byte b : encodedTermId)
			byteBuffer.put(b);
		for (Byte b : encodedDocFreq)
			byteBuffer.put(b);
		for (Byte b : encodedDocIds)
			byteBuffer.put(b);
		byteBuffer.flip();
		try {
			fc.write(byteBuffer);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * This method is used to encode a sequence of numbers with Variable Byte encoding algorithm
	 * @param numbers a sequence of numbers
	 * @return ByteStream
	 */
	private static List<Byte> VBEncode(List<Integer> numbers) {
		Vector<Byte> byteStream = new Vector<Byte>();
		for (Integer number : numbers) {
			List<Byte> bytes = VBEncodeNumber(number); // Encode each number
			byteStream.addAll(bytes); // Append the new byteStream to the old ByteStream
		}
		byteStream.trimToSize();
		return byteStream;
	}

	/**

	 * This method is used to encode a number with Variable Byte encoding algorithm
	 * @param number
	 * @return
	 */
	private static List<Byte> VBEncodeNumber(int number) {
		LinkedList<Byte> bytes = new LinkedList<Byte>();
		while (true) {
			bytes.addFirst((byte) (number % 128)); // Add into the front of the list
			if (number < 128)
				break;
			number /= 128;
		}
		byte b = (byte) (bytes.getLast() + (byte) 128); // The first bit of the last byte change to 1 for ending bit.
		bytes.removeLast();
		bytes.addLast(b);
		return bytes;
	}

	/**
	 * change in-order list to be gap list for example docIds 824 829 215406 gap 824 5 214577
	 * 
	 * @param postings
	 * @param inverse
	 * @return
	 */
	private static List<Integer> docGap(List<Integer> postings, boolean inverse) {
		if (inverse) {
			for (int i = 1; i < postings.size(); i++)
				postings.set(i, postings.get(i) + postings.get(i - 1));
		} else {
			for (int i = postings.size() - 1; i > 0; i--)
				postings.set(i, postings.get(i) - postings.get(i - 1));
		}
		return postings;
	}

	/**
	 * To test the correctness of writePosting
	 * 
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		File file = new File("./index/small/corpus2.index");
		file.delete();
		file = new File("./index/small/corpus2.index");
		RandomAccessFile raf = new RandomAccessFile(file, "rw");
		FileChannel fc = raf.getChannel();
		VBIndex index = new VBIndex();
		int i = 0;
		while (i++ < 3) {
			List<Integer> l = new Vector<Integer>();
			l.add(824 + i * 100);
			l.add(829 + i * 9000);
			l.add(215406 + i * 4);
			PostingList p = new PostingList(i, l);
			index.writePosting(fc, p);
		}

		fc.position(0);
		PostingList posting = null;
		while ((posting = index.readPosting(fc)) != null) {
			System.out.println("Term id: " + posting.getTermId());
			System.out.println("List: " + posting.getList());
		}

		raf.close();
	}
}
