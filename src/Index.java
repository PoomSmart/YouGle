
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.Vector;

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
	private static Map<Integer, Set<Integer>> termDoc = new TreeMap<Integer, Set<Integer>>();

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
		index.writePosting(fc, posting);
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

	private static void writeByteBuffer(FileChannel fc, int value) throws IOException {
		ByteBuffer buffer = ByteBuffer.allocate(4);
		buffer.putInt(value);
		buffer.flip();
		fc.write(buffer);
		buffer = null;
	}
	
	private static void writeBytesBuffer(FileChannel fc, List<Integer> values) throws IOException {
		ByteBuffer buffer = ByteBuffer.allocate(values.size() * 4);
		for (int value : values)
			buffer.putInt(value);
		buffer.flip();
		fc.write(buffer);
		buffer = null;
	}
	
	private static void writeExcessTermsToPosting(FileChannel fc, RandomAccessFile block, int termId, List<Integer> docs) throws IOException {
		docs.clear();
		docs.add(termId);
		int freq;
		docs.add(freq = block.readInt());
		while (freq-- != 0)
			docs.add(block.readInt());
//		System.out.println("DEBUG: EXCESS");
		writeBytesBuffer(fc, docs);
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
//				System.out.println(fileName);

				// use pre-increment to ensure docID > 0
				int docId = ++docIdCounter;
				docDict.put(fileName, docId);

				BufferedReader reader = new BufferedReader(new FileReader(file));
				String line;
				while ((line = reader.readLine()) != null) {
					String[] tokens = line.trim().split("\\s+");
					for (String token : tokens) {
						termDict.put(token, termDict.getOrDefault(token, 1 + termDict.size())); // assign term ID in
																								// increasing manner
						Integer termId = termDict.get(token);
						Set<Integer> docIds = termDoc.get(termId);
						if (docIds == null)
							termDoc.put(termId, docIds = new HashSet<Integer>());
						docIds.add(docId);
					}
					tokens = null;
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
			System.out.println("DEBUG: Write posting start");
			for (Integer termId = 1; termId <= termDict.size(); termId++) {
				List<Integer> docIds = new Vector<Integer>(termDoc.get(termId));
				Collections.sort(docIds);
				writePosting(bfcc, new PostingList(termId, docIds));
			}
			System.out.println("DEBUG: Write posting done");

			bfc.close();
		}
		
		/* Assign position and document frequency to each term */
		System.out.println("DEBUG: Assign start");
		Long position = 0L;
		for (Integer termId = 1; termId <= (wordIdCounter = termDict.size()); termId++) {
			Pair<Long, Integer> pair = postingDict.get(termId);
			if (pair == null)
				postingDict.put(termId, pair = new Pair<Long, Integer>(0L, 0));
			pair.setFirst(position);
			Set<Integer> docIds = termDoc.get(termId);
			int docFreq = docIds.size();
			position += docFreq * 4 + 8;
			pair.setSecond(docFreq);
		}
		System.out.println("DEBUG: Assign done");

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
			System.out.println("DEBUG: merging " + b1.getName() + "+" + b2.getName() + " start");

			RandomAccessFile bf1 = new RandomAccessFile(b1, "r");
			RandomAccessFile bf2 = new RandomAccessFile(b2, "r");
			RandomAccessFile mf = new RandomAccessFile(combfile, "rw");
			FileChannel bf1c = bf1.getChannel();
			FileChannel bf2c = bf2.getChannel();
			FileChannel mfc = mf.getChannel();
			int i, j, f, t1, t2, f1, f2, d1, d2;
			List<Integer> docs = new Vector<Integer>();
			PostingList p1, p2;
			List<Integer> docs1, docs2;
			while ((p1 = index.readPosting(bf1c)) != null && (p2 = index.readPosting(bf2c)) != null) {
				docs.clear();
				f1 = (docs1 = p1.getList()).size();
				f2 = (docs2 = p2.getList()).size();
				if ((t1 = p1.getTermId()) == (t2 = p2.getTermId())) {
					docs.add(t1);
					i = j = f = 0;
					while (i < f1 && j < f2) {
						d1 = docs1.get(i);
						d2 = docs2.get(j);
						if (d1 < d2) {
							docs.add(d1);
							i++;
						} else {
							docs.add(d2);
							if (d2 >= d1)
								i++;
							j++;
						}
						f++;
					}
					while (i < f1) {
						docs.add(docs1.get(i++));
						f++;
					}
					while (j < f2) {
						docs.add(docs2.get(j++));
						f++;
					}
					docs.add(1, f);
					writeBytesBuffer(mfc, docs);
				} else {
					if (t1 < t2)
						index.writePosting(mfc, p1 = index.readPosting(bf1c));
					else
						index.writePosting(mfc, p2 = index.readPosting(bf2c));
				}
			}
			while ((p1 = index.readPosting(bf1c)) != null)
				index.writePosting(mfc, p1);
			while ((p2 = index.readPosting(bf2c)) != null)
				index.writePosting(mfc, p2);
			docs = null;
			bf1.close();
			bf1 = null;
			bf2.close();
			bf2 = null;
			mf.close();
			mf = null;
			System.out.println("DEBUG: merging " + b1.getName() + "+" + b2.getName() + " done");
			b1.delete();
			b1 = null;
			b2.delete();
			b2 = null;
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