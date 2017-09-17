
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
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
		boolean singleBlock = dirlist.length == 1;

		/* For each block */
		for (File block : dirlist) {
			File blockFile = new File(outputDirname, block.getName());
			System.out.println("Processing block " + block.getName());
			blockQueue.add(blockFile);

			File blockDir = new File(dataDirname, block.getName());
			File[] filelist = blockDir.listFiles();

			Map<Integer, Set<Integer>> localTermDoc = new TreeMap<Integer, Set<Integer>>();

			/* For each file */
			for (File file : filelist) {
				++totalFileCount;
				String fileName = block.getName() + "/" + file.getName();
//				System.out.println(fileName); // TODO: change as code

				// use pre-increment to ensure docID > 0
				int docId = ++docIdCounter;
				docDict.put(fileName, docId);

				BufferedReader reader = new BufferedReader(new FileReader(file));
				String line;
				while ((line = reader.readLine()) != null) {
					String[] tokens = line.trim().split("\\s+");
					for (String token : tokens) {
						int termId = termDict.getOrDefault(token, -1);
						if (termId == -1)
							termDict.put(token, termId = 1 + termDict.size()); // assign term ID in increasing manner
						Set<Integer> localDocIds = localTermDoc.get(termId);
						if (localDocIds == null)
							localTermDoc.put(termId, localDocIds = new TreeSet<Integer>());
						localDocIds.add(docId);
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
			System.out.println("DEBUG: Write posting start");
			for (Integer termId : localTermDoc.keySet()) {
				List<Integer> docIds = new Vector<Integer>(localTermDoc.get(termId));
				if (singleBlock)
					postingDict.put(termId, new Pair<Long, Integer>(bfcc.position(), docIds.size()));
				index.writePosting(bfcc, new PostingList(termId, docIds));
			}
			localTermDoc.clear();
			System.out.println("DEBUG: Write posting done");

			bfc.close();
		}

		/* Required: output total number of files. */
		System.out.println("Total Files Indexed: " + totalFileCount);

		/* Temporary variables for merging blocks */
		int t1, t2;
		Integer d1 = null, d2 = null;
		List<Integer> docs = new Vector<Integer>();
		PostingList p1 = null, p2 = null;
		Iterator<Integer> idocs1 = null, idocs2 = null;

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

			p1 = index.readPosting(bf1c);
			p2 = index.readPosting(bf2c);
			while (p1 != null && p2 != null) {
				if ((t1 = p1.getTermId()) == (t2 = p2.getTermId())) {
					idocs1 = p1.getList().iterator();
					idocs2 = p2.getList().iterator();
					d1 = popNextOrNull(idocs1);
					d2 = popNextOrNull(idocs2);
					while (d1 != null && d2 != null) {
						if (d1.compareTo(d2) < 0) {
							docs.add(d1);
							d1 = popNextOrNull(idocs1);
						} else {
							docs.add(d2);
							if (d2.compareTo(d1) >= 0)
								d1 = popNextOrNull(idocs1);
							d2 = popNextOrNull(idocs2);
						}
					}
					while (d1 != null) {
						docs.add(d1);
						d1 = popNextOrNull(idocs1);
					}
					while (d2 != null) {
						docs.add(d2);
						d2 = popNextOrNull(idocs2);
					}
					postingDict.put(t1, new Pair<Long, Integer>(mfc.position(), docs.size()));
					index.writePosting(mfc, new PostingList(t1, docs));
					docs.clear();
					p1 = index.readPosting(bf1c);
					p2 = index.readPosting(bf2c);
				} else {
					if (t1 < t2) {
						postingDict.put(t1, new Pair<Long, Integer>(mfc.position(), p1.getList().size()));
						index.writePosting(mfc, p1);
						p1 = index.readPosting(bf1c);
					} else {
						postingDict.put(t2, new Pair<Long, Integer>(mfc.position(), p2.getList().size()));
						index.writePosting(mfc, p2);
						p2 = index.readPosting(bf2c);
					}
				}
			}
			while (p1 != null) {
				postingDict.put(p1.getTermId(), new Pair<Long, Integer>(mfc.position(), p1.getList().size()));
				index.writePosting(mfc, p1);
				p1 = index.readPosting(bf1c);
			}
			while (p2 != null) {
				postingDict.put(p2.getTermId(), new Pair<Long, Integer>(mfc.position(), p2.getList().size()));
				index.writePosting(mfc, p2);
				p2 = index.readPosting(bf2c);
			}
			bf1.close();
			bf2.close();
			mf.close();
			System.out.println("DEBUG: merging " + b1.getName() + "+" + b2.getName() + " done");
			b1.delete();
			b2.delete();
			blockQueue.add(combfile);
		}

		/* Dump constructed index back into file system */
		File indexFile = blockQueue.removeFirst();
		indexFile.renameTo(new File(outputDirname, "corpus.index"));

		BufferedWriter termWriter = new BufferedWriter(new FileWriter(new File(outputDirname, "term.dict")));
		wordIdCounter = termDict.size();
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