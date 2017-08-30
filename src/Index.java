
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Map;
import java.util.TreeMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class Index {

	// Term id -> (position in index file, doc frequency) dictionary
	private static Map<Integer, Pair<Long, Integer>> postingDict = new TreeMap<Integer, Pair<Long, Integer>>();
	// Doc name -> doc id dictionary
	private static Map<String, Integer> docDict = new TreeMap<String, Integer>();
	// Term -> term id dictionary
	private static Map<String, Integer> termDict = new TreeMap<String, Integer>();
	// Block queue
	private static LinkedList<File> blockQueue = new LinkedList<File>();
	// Term id -> doc id list
	private static Map<Integer, List<Integer>> termDoc = new TreeMap<Integer, List<Integer>>();

	// Total file counter
	private static int totalFileCount = 0;
	// Document counter
	private static int docIdCounter = 0;
	// Term counter
	private static int wordIdCounter = 0;
	// Index
	private static BaseIndex index = null;

	/*
	 * Write a posting list to the given file You should record the file position of this posting list so that you can
	 * read it back during retrieval
	 * 
	 */
	private static void writePosting(FileChannel fc, PostingList posting) throws IOException {
		/*
		 * TODO: Your code here
		 * 
		 */
		List<Integer> docIds = posting.getList();
		int size = docIds.size();
		ByteBuffer buffer = ByteBuffer.allocate(8 + size * 4);
		buffer.putInt(posting.getTermId());
		buffer.putInt(size);
		for (int docId : docIds)
			buffer.putInt(docId);
		buffer.flip();
		fc.write(buffer);
	}

	/**
	 * Pop next element if there is one, otherwise return null
	 * 
	 * @param iter
	 *            an iterator that contains integers
	 * @return next element or null
	 */
	private static Integer popNextOrNull(Iterator<Integer> iter) {
		if (iter.hasNext()) {
			return iter.next();
		} else {
			return null;
		}
	}

	private static void writeByteBuffer(FileChannel fc, ByteBuffer buffer, int value) throws IOException {
		buffer.clear();
		buffer.putInt(value);
		buffer.flip();
		fc.write(buffer);
	}
	
	private static void writeExcessTermsToPosting(FileChannel fc, ByteBuffer buffer, RandomAccessFile block, int termId) throws IOException {
		writeByteBuffer(fc, buffer, termId);
		int freq;
		writeByteBuffer(fc, buffer, freq = block.readInt());
		while (freq-- != 0)
			writeByteBuffer(fc, buffer, block.readInt());
	}

	/**
	 * Main method to start the indexing process.
	 * 
	 * @param method
	 *            :Indexing method. "Basic" by default, but extra credit will be given for those who can implement
	 *            variable byte (VB) or Gamma index compression algorithm
	 * @param dataDirname
	 *            :relative path to the dataset root directory. E.g. "./datasets/small"
	 * @param outputDirname
	 *            :relative path to the output directory to store index. You must not assume that this directory exist.
	 *            If it does, you must clear out the content before indexing.
	 */
	public static int runIndexer(String method, String dataDirname, String outputDirname) throws IOException {
		/* Get index */
		String className = method + "Index";
		try {
			Class<?> indexClass = Class.forName(className);
			index = (BaseIndex) indexClass.newInstance();
		} catch (Exception e) {
			System.err.println("Index method must be \"Basic\", \"VB\", or \"Gamma\"");
			throw new RuntimeException(e);
		}

		/* Get root directory */
		File rootdir = new File(dataDirname);
		if (!rootdir.exists() || !rootdir.isDirectory()) {
			System.err.println("Invalid data directory: " + dataDirname);
			return -1;
		}

		/* Get output directory */
		File outdir = new File(outputDirname);
		if (outdir.exists() && !outdir.isDirectory()) {
			System.err.println("Invalid output directory: " + outputDirname);
			return -1;
		}

		/*
		 * TODO: delete all the files/sub folder under outdir
		 * 
		 */
		for (File file : outdir.listFiles()) {
			file.delete();
		}

		if (!outdir.exists()) {
			if (!outdir.mkdirs()) {
				System.err.println("Create output directory failure");
				return -1;
			}
		}

		/* BSBI indexing algorithm */
		File[] dirlist = rootdir.listFiles();

		/* For each block */
		for (File block : dirlist) {
			File blockFile = new File(outputDirname, block.getName());
			System.out.println("Processing block " + block.getName());
			blockQueue.add(blockFile);

			File blockDir = new File(dataDirname, block.getName());
			File[] filelist = blockDir.listFiles();

			/* For each file */
			for (File file : filelist) {
				++totalFileCount;
				String fileName = block.getName() + "/" + file.getName();
				System.out.println(fileName);

				// use pre-increment to ensure docID > 0
				int docId = ++docIdCounter;
				docDict.put(fileName, docId);

				BufferedReader reader = new BufferedReader(new FileReader(file));
				String line;
				List<String> readTokens = new ArrayList<String>(); // save tokens already read
				while ((line = reader.readLine()) != null) {
					String[] tokens = line.trim().split("\\s+");
					for (String token : tokens) {
						/*
						 * TODO: Your code here For each term, build up a list of documents in which the term occurs
						 */
						if (readTokens.contains(token))
							continue; // we don't care duplicates
						readTokens.add(token);
						termDict.put(token, termDict.getOrDefault(token, 1 + termDict.size())); // assign term ID in
																								// increasing manner
						Integer termId = termDict.get(token);
						Pair<Long, Integer> pair = postingDict.get(termId);
						if (pair == null)
							postingDict.put(termId, pair = new Pair<Long, Integer>(0L, 0));
						pair.setSecond(pair.getSecond() + 1);
						List<Integer> docIds = termDoc.get(termId);
						if (docIds == null)
							termDoc.put(termId, docIds = new ArrayList<Integer>());
						if (!docIds.contains(docId))
							docIds.add(docId);
					}
				}
				reader.close();
			}

			/* Sort and output */
			if (!blockFile.createNewFile()) {
				System.err.println("Create new block failure.");
				return -1;
			}

			RandomAccessFile bfc = new RandomAccessFile(blockFile, "rw");
			FileChannel bfcc = bfc.getChannel();

			/*
			 * Write all posting lists for all terms to file (bfc)
			 */
			for (Integer termId = 1; termId <= termDict.size(); termId++) {
				List<Integer> docIds = termDoc.get(termId);
				Collections.sort(docIds);
				writePosting(bfcc, new PostingList(termId, docIds));
			}

			bfc.close();
		}
		
		/* Assign position to each term */
		Long position = 0L;
		for (Integer termId = 1; termId <= termDict.size(); termId++) {
			Pair<Long, Integer> pair = postingDict.get(termId);
			pair.setFirst(position);
			List<Integer> docIds = termDoc.get(termId);
			position += docIds.size() * 4 + 8;
		}

		/* Required: output total number of files. */
		System.out.println("Total Files Indexed: " + totalFileCount);

		/* Merge blocks */
		while (true) {
			if (blockQueue.size() <= 1)
				break;

			File b1 = blockQueue.removeFirst();
			File b2 = blockQueue.removeFirst();

			File combfile = new File(outputDirname, b1.getName() + "+" + b2.getName());
			if (!combfile.createNewFile()) {
				System.err.println("Create new block failure.");
				return -1;
			}

			RandomAccessFile bf1 = new RandomAccessFile(b1, "r");
			RandomAccessFile bf2 = new RandomAccessFile(b2, "r");
			RandomAccessFile mf = new RandomAccessFile(combfile, "rw");
			FileChannel mfc = mf.getChannel();
			long b1Ptr = 0L, b1Length = bf1.length();
			long b2Ptr = 0L, b2Length = bf2.length();
			int i, j, f, t1, t2, f1, f2, d1, d2;
			long floc, cloc; // freq location, current location
			ByteBuffer buffer = ByteBuffer.allocate(4);
			while (((b1Ptr = bf1.getFilePointer()) < b1Length && (t1 = bf1.readInt()) != 0) && ((b2Ptr = bf2.getFilePointer()) < b2Length && (t2 = bf2.readInt()) != 0)) {
				f1 = bf1.readInt();
				f2 = bf2.readInt();
				if (t1 == t2) {
					i = j = f = 0;
					writeByteBuffer(mfc, buffer, t1);
					floc = mfc.position();
					writeByteBuffer(mfc, buffer, 0);
					while (i < f1 && j < f2) {
						d1 = bf1.readInt();
						d2 = bf2.readInt();
						if (d1 < d2) {
							writeByteBuffer(mfc, buffer, d1);
							i++;
						} else if (d2 < d1) {
							writeByteBuffer(mfc, buffer, d2);
							j++;
						} else {
							writeByteBuffer(mfc, buffer, d2);
							i++;
							j++;
						}
						f++;
					}
					while (i++ < f1) {
						writeByteBuffer(mfc, buffer, d1 = bf1.readInt());
						f++;
					}
					while (j++ < f2) {
						writeByteBuffer(mfc, buffer, d2 = bf2.readInt());
						f++;
					}
					cloc = mfc.position();
					mfc.position(floc);
					writeByteBuffer(mfc, buffer, f);
					mfc.position(cloc);
				} else {
					if (t1 < t2) {
						while (t1 < t2) {
							if ((t1 = bf1.readInt()) == 0) break;
							writeExcessTermsToPosting(mfc, buffer, bf1, t1);
						}
					} else {
						while (t2 < t1) {
							if ((t2 = bf2.readInt()) == 0) break;
							writeExcessTermsToPosting(mfc, buffer, bf2, t2);
						}
					}
				}
			}
			if (b1Ptr < b1Length) {
				while (((b1Ptr = bf1.getFilePointer()) < b1Length) && (t1 = bf1.readInt()) != 0)
					writeExcessTermsToPosting(mfc, buffer, bf1, t1);
			} else if (b2Ptr < b2Length) {
				while (((b2Ptr = bf2.getFilePointer()) < b2Length) && (t2 = bf2.readInt()) != 0)
					writeExcessTermsToPosting(mfc, buffer, bf2, t2);
			}

			bf1.close();
			bf2.close();
			mf.close();
			b1.delete();
			b2.delete();
			blockQueue.add(combfile);
		}

		/* Dump constructed index back into file system */
		File indexFile = blockQueue.removeFirst();
		indexFile.renameTo(new File(outputDirname, "corpus.index"));

		BufferedWriter termWriter = new BufferedWriter(new FileWriter(new File(outputDirname, "term.dict")));
		for (String term : termDict.keySet()) {
			termWriter.write(term + "\t" + termDict.get(term) + "\n");
		}
		termWriter.close();

		BufferedWriter docWriter = new BufferedWriter(new FileWriter(new File(outputDirname, "doc.dict")));
		for (String doc : docDict.keySet()) {
			docWriter.write(doc + "\t" + docDict.get(doc) + "\n");
		}
		docWriter.close();

		BufferedWriter postWriter = new BufferedWriter(new FileWriter(new File(outputDirname, "posting.dict")));
		for (Integer termId : postingDict.keySet()) {
			postWriter.write(termId + "\t" + postingDict.get(termId).getFirst() + "\t"
					+ postingDict.get(termId).getSecond() + "\n");
		}
		postWriter.close();

		return totalFileCount;
	}

	public static void main(String[] args) throws IOException {
		/* Parse command line */
		if (args.length != 3) {
			System.err.println("Usage: java Index [Basic|VB|Gamma] data_dir output_dir");
			return;
		}

		/* Get index */
		String className = "";
		try {
			className = args[0];
		} catch (Exception e) {
			System.err.println("Index method must be \"Basic\", \"VB\", or \"Gamma\"");
			throw new RuntimeException(e);
		}

		/* Get root directory */
		String root = args[1];

		/* Get output directory */
		String output = args[2];
		runIndexer(className, root, output);
	}

}
