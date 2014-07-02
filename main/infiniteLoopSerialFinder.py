#from mpi4py import MPI



import ast

import time



def infiniteLoopFinder():



	theDict = {}

	fread = open('path2phil.tsv', 'rb').read()

	theFile = fread.decode('utf-16')

	majorDict = {}

	linesList = theFile.split('\n')

	for line in linesList:

		try:

			theKey, theList = line.split('\t')

			theKey = ast.literal_eval(theKey)

			theList = ast.literal_eval(theList)

			if theList[1] == "INFINITE LOOP":

				majorDict[theKey] = theList[0][1]

		except ValueError:

			pass



	#print majorDict

	for key in majorDict:

		keysList = [key]

		try:

			if majorDict[key] in theDict:

				keysList = [key]

				for i in theDict[majorDict[key]]:

					if not i in keysList:

						keysList.append(i)

				keysList.append(key)

			else:

				while not majorDict[keysList[-1]] in keysList:

					keysList.append(majorDict[keysList[-1]])

				keysList.append(majorDict[keysList[-1]])

			theDict[key] = keysList

			print key, keysList

		except KeyError:

			keysList.append('The article "'+ keysList[-1] +'" appears to be a dead link.')

			print key, keysList







if __name__ == '__main__':

	start = time.time()

	infiniteLoopFinder()

	print 'TIME:', time.time() - start