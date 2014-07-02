from mrjob.job import MRJob
from mrjob.protocol import JSONProtocol
center  = "philosophy"
center2 = "PAGE HAS NO LINKS!"
class PrepareGraph(MRJob):
	INPUT_PROTOCOL = JSONProtocol #so input is in easy to read format
	# Yields the first link with its article title
	def mapper1(self, key, values):
		links_list = list(values)
		testlist = [value for value in links_list]
		first_link = "PAGE HAS NO LINKS!"
		if len(testlist) > 0:
			first_link = testlist[0]	
 		yield first_link.upper(), key.upper()
	# Reduces so that we have links and a list of article titles for which it is first
 	def reducer1(self, first_link, article_titles):
		article_titles_list = list(article_titles)
		new_list = [title for title in article_titles_list]
		# yields each article title with its first link
		# If True, then the center has been reached or cannot be reached
		if first_link.upper() == center.upper() or first_link == center2:
			for article in new_list:
				yield article, ([article, first_link], True)
		else:
			for article in new_list:
				yield article, ([article, first_link], False)
				
	def mapper2(self, articleTitle, theTuple):
		myList = [i for i in theTuple]		
		if myList[1] == True:
			yield articleTitle, list(myList)[0]
		else:
			yield myList[0][-1], list(myList)[0]

	def reducer2(self, nextArticle, myList):
		finishList = None
		links_list = list(myList)
		testlist = [value for value in links_list]

		for sequence in testlist:
			if sequence[-1].upper() == center.upper() or sequence[-1] == center2:
				finishList = sequence

		#True if we have reached the center for a given article; false otherwise
		if finishList == None:
			for sequence in testlist:
				yield sequence[0], (sequence, False)
		else:
			for sequence in testlist:
				if (sequence == finishList) or (not len(set(sequence)) == len(sequence)) or ('PAGE HAS NO LINKS!' in sequence):
					yield sequence[0], (sequence, True)
				else:
					newSequence = sequence[:-1] + finishList
					yield sequence[0], (newSequence, True)


	def mapper3(self, articleTitle, theTuple):
		myList = [i for i in theTuple]		
		if myList[1] == True:
			yield articleTitle, list(myList)[0]
		else:
			yield myList[0][-1], list(myList)[0]

	def reducer3(self, nextArticle, myList):
		finishList = None
		links_list = list(myList)
		testlist = [value for value in links_list]

		for sequence in testlist:
			if sequence[-1].upper() == center.upper() or sequence[-1] == center2:
				finishList = sequence

		if finishList == None:
			for sequence in testlist:
				yield sequence[0], (sequence, "INFINITE LOOP")
		else:
			for sequence in testlist:
				if (sequence == finishList) or (not len(set(sequence)) == len(sequence)) or ('PAGE HAS NO LINKS!' in sequence):
					yield sequence[0], (sequence, len(sequence))
				else:
					newSequence = sequence[:-1] + finishList
					yield sequence[0], (newSequence, len(newSequence))



	# MapReduce does not allow us to run steps indefinitely; here, we have included 53 
	# steps but can easily include more by copying and pasting the mapper2/reducer2 lines.
	def steps(self):
		return [self.mr(mapper = self.mapper1, reducer = self.reducer1),
		self.mr(mapper = self.mapper2, reducer = self.reducer2),
		self.mr(mapper = self.mapper2, reducer = self.reducer2),
		self.mr(mapper = self.mapper2, reducer = self.reducer2),
		self.mr(mapper = self.mapper2, reducer = self.reducer2),
		self.mr(mapper = self.mapper2, reducer = self.reducer2),
		self.mr(mapper = self.mapper2, reducer = self.reducer2),
		self.mr(mapper = self.mapper2, reducer = self.reducer2),
		self.mr(mapper = self.mapper2, reducer = self.reducer2),
		self.mr(mapper = self.mapper2, reducer = self.reducer2),
		self.mr(mapper = self.mapper2, reducer = self.reducer2),
		self.mr(mapper = self.mapper2, reducer = self.reducer2),
		self.mr(mapper = self.mapper2, reducer = self.reducer2),
		self.mr(mapper = self.mapper2, reducer = self.reducer2),
		self.mr(mapper = self.mapper2, reducer = self.reducer2),
		self.mr(mapper = self.mapper2, reducer = self.reducer2),
		self.mr(mapper = self.mapper2, reducer = self.reducer2),
		self.mr(mapper = self.mapper2, reducer = self.reducer2),
		self.mr(mapper = self.mapper2, reducer = self.reducer2),
		self.mr(mapper = self.mapper2, reducer = self.reducer2),
		self.mr(mapper = self.mapper2, reducer = self.reducer2),
		self.mr(mapper = self.mapper2, reducer = self.reducer2),
		self.mr(mapper = self.mapper2, reducer = self.reducer2),
		self.mr(mapper = self.mapper2, reducer = self.reducer2),
		self.mr(mapper = self.mapper2, reducer = self.reducer2),
		self.mr(mapper = self.mapper2, reducer = self.reducer2),
		self.mr(mapper = self.mapper2, reducer = self.reducer2),
		self.mr(mapper = self.mapper2, reducer = self.reducer2),
		self.mr(mapper = self.mapper2, reducer = self.reducer2),
		self.mr(mapper = self.mapper2, reducer = self.reducer2),
		self.mr(mapper = self.mapper2, reducer = self.reducer2),
		self.mr(mapper = self.mapper2, reducer = self.reducer2),
		self.mr(mapper = self.mapper2, reducer = self.reducer2),
		self.mr(mapper = self.mapper2, reducer = self.reducer2),
		self.mr(mapper = self.mapper2, reducer = self.reducer2),
		self.mr(mapper = self.mapper2, reducer = self.reducer2),
		self.mr(mapper = self.mapper2, reducer = self.reducer2),
		self.mr(mapper = self.mapper2, reducer = self.reducer2),
		self.mr(mapper = self.mapper2, reducer = self.reducer2),
		self.mr(mapper = self.mapper2, reducer = self.reducer2),
		self.mr(mapper = self.mapper2, reducer = self.reducer2),
		self.mr(mapper = self.mapper2, reducer = self.reducer2),
		self.mr(mapper = self.mapper2, reducer = self.reducer2),
		self.mr(mapper = self.mapper2, reducer = self.reducer2),
		self.mr(mapper = self.mapper2, reducer = self.reducer2),
		self.mr(mapper = self.mapper2, reducer = self.reducer2),
		self.mr(mapper = self.mapper2, reducer = self.reducer2),
		self.mr(mapper = self.mapper2, reducer = self.reducer2),
		self.mr(mapper = self.mapper2, reducer = self.reducer2),
		self.mr(mapper = self.mapper2, reducer = self.reducer2),
		self.mr(mapper = self.mapper2, reducer = self.reducer2),
		self.mr(mapper = self.mapper2, reducer = self.reducer2),
		self.mr(mapper = self.mapper2, reducer = self.reducer2),
		self.mr(mapper = self.mapper2, reducer = self.reducer2),
		self.mr(mapper = self.mapper2, reducer = self.reducer2),
		self.mr(mapper = self.mapper3, reducer = self.reducer3)]



if __name__ == '__main__':
	PrepareGraph.run()