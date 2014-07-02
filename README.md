paths-to-wikipedia
==================

This project parses a 44GB XML data dump of Wikipedia into network graph representative of 4.2 million articles (nodes) and their edges. It uses MapReduce programs for Hadoop in Python to execute a single-source breadth-first search algorithm to analyze connections. It optimizes batch processing of files using MPI and thread pooling. From it, we concluded that from the network graph analysis of data that the article “Philosophy” is a common hub for ~94% of all Wikipedia articles.


<h2>DOWNLOAD WIKIPEDIA</h2>
To run our code, you'll need to download the most recent version of Wikipedia.
You can do so [here](http://dumps.wikimedia.org/backup-index.html).

We're certainly not responsible for any changes the good folks at Wikipedia make, so we'll tell you only our code worked on Wikipedia data download throughout November.

<h2>HOW TO RUN THE CODE</h2>
1. Download the Wikipedia data dump. Depending on when you download the data dump, the software will output different results, as Wikipedia is constantly changing and thus so is the data. This download is approximately several GBs, even compressed, it will take several hours to download.

2. Use the attached wikipediaPageParser.py file.  This code finds when the page begins ( <page> ) and ends ( </page> ) and then the script copies the xml into a new xml file until it hits the </page> and then opens a new file. After 1000 files have been written, the script compresses them so it’s easier to process them.  This code is run serially.  The usage is:
Usage: python", argv[0], "<wikipedia_archive> <output_dir>

3. Use either the block_processing_serial, block_processing_parallel or thread pools file to unblock the sites and create a tsv of all the article titles and a list of all the links on their page.  Before running either file, you must open the file and adjust the current directory to the directory where your parsed wikipedia files are (currently “tmp_output”).  To run the serial version of the file, please type “python blockSerialProcessing.py all_articlesList_.tsv” into your terminal in the correct directory.  BE SURE THAT NO FILES ARE IN THE DIRECTORY OTHER THAN THE BLOCKS YOU ARE PROCESSING.  To run the parallel version, please type “mpiexec -n 8 python blockSerialProcessing.py all_articlesList_.tsv” into the terminal while logged into the appropriate node for MPI.  You will then have access to the files.  I recommend beginning with a small subset of the blocks in a different folder to see that the code runs without errors… it really stinks if the code has processed several thousand blocks and then can’t write the file for some reason.  This will create a file in the directory from which you’re reading your blocks which contains all of the appropriate links from Wikipedia.  This is parallel using MPI. The thread pools version is by far the fastest version, and can run on all of Wikipedia in 20-40 minutes. It also only takes the first few links for each article.

4.  Once the file from step 3 has been saved, run the mapreducecodeFirstLinkPath.py file with the terminal in the same as all_articlesList_.tsv.  Run the code by typing “python mapreducecode.py all_articlesList_.tsv >path2phil.tsv” into the terminal.  When the code finishes, path2phil.tsv should contain the path of each article, navigating only through first links, to philosophy.  This code is the crux of the project.

5. Finally, run the infiniteLoopSerialFinder.py file.  This code can be run by running the python code in the directory with your path2phil.tsv file by typing “python infinite_loop.py >infiniteloops_path.tsv”.  The parallel code can be run in the terminal (logged into the appropriate node) by typing “mpiexec –n 8 python infiniteLoopParallelFinder.py >infiniteloops_path.tsv”