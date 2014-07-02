from mpi4py import MPI



import ast

import time



def infiniteLoopFinder():

    comm = MPI.COMM_WORLD

    rank = comm.Get_rank()

    size = comm.Get_size()

    mpi_count = 0

    sync_time = 0





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



    theDict = {}

    for key in majorDict:

        mpi_count += 1

        if mpi_count % size == rank:

            sync_time += 1

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

            if sync_time % 10 == 0:

                giantList = comm.gather(theDict, root = 0)

                if rank == 0:

                    newDict = {}

                    for aDict in giantList:

                        dict(newDict.items() + aDict.items())

                    for i in range(size-1):

                        comm.send(theDict, dest = i + 1, tag = 0)

                else:

                    theDict = comm.recv(source = 0, tag = 0)





if __name__ == '__main__':

    start = time.time()

    infiniteLoopFinder()

    if MPI.COMM_WORLD.Get_rank() == 0:

        print 'TIME:', time.time() - start