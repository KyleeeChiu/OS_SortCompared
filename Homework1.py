import time
import threading as td
import multiprocessing as mp
from multiprocessing import Pool
from datetime import datetime, timezone, timedelta
from itertools import chain
import os

class Num:
    def __init__(self):
        self.numArray = []

def bubblesort( set ):
    n = len( set.numArray )
    sorted = False
    for passes in range( 1, n ):
        sorted = True
        for index in range( 0, n-passes ):
            if set.numArray[index] > set.numArray[index+1]:
                set.numArray[index], set.numArray[index+1] = set.numArray[index+1], set.numArray[index]
                sorted = False

        if sorted == True:
            break

    return set

def Mergesort( out, left, right ):
    i = j = 0
    temp = Num()
    sum = len(left.numArray)+len( right.numArray )
    k = 0
    while k < sum:
        if i < len(left.numArray) and j < len( right.numArray ):
            if left.numArray[i] <= right.numArray[j]:
                temp.numArray.append(left.numArray[i])
                i += 1
            else:
                temp.numArray.append(right.numArray[j])
                j += 1
        elif i < len(left.numArray) and j >= len( right.numArray ):
            temp.numArray.append(left.numArray[i])
            i += 1
        else:
            temp.numArray.append(right.numArray[j])
            j += 1

        k+=1
    
    out.append(temp)
    return temp

def readF( fileName, total ):
    try:
        fp = open(fileName+'.txt', "r")
        total.numArray = fp.readlines()
        total.numArray = [int(x) for x in total.numArray]

    except FileNotFoundError:

        print('\n###',fileName,'does not exist! ###' )
        return False

    return True

def readF2( fileName, total, num, cut ):
    try:
        fp = open(fileName+'.txt', "r")
        total.numArray = fp.readlines()
        total.numArray = [int(x) for x in total.numArray]

        size = len(total.numArray) // num
        count = len (total.numArray) % num
        cur = 0
        i = 0
        while i < count :
            temp = Num()
            temp.numArray = total.numArray[cur:cur+size+1]
            cut.append(temp)
            cur = cur+size+1
            i = i+1
        i = 0
        while i < num-count:
            temp = Num()
            temp.numArray = total.numArray[cur:cur+size]
            cut.append(temp)
            cur = cur+size
            i = i+1

    except FileNotFoundError:
        print('\n###',fileName,'does not exist! ###' )
        return False

    return True

def writeF( fileName, total, sortTime ):
    fp = open(fileName, "w")
    fp.writelines( "Sort : \n" )
    for i in range(len(total.numArray)):
        fp.writelines("%s\n" %str(total.numArray[i]) )

    dt1 = datetime.utcnow().replace(tzinfo=timezone.utc)
    dt2 = dt1.astimezone(timezone(timedelta(hours=8)))
    fp.writelines( "CPU Time : %s\n" %str(sortTime) )
    fp.writelines( "Output Time : %s" %dt2 )
    fp.close()
    print( "\nCPU Time : %s" %str(sortTime) )
    print( "\nOutput Time : %s" %dt2 )

if __name__ == "__main__":
    while True:
        total = Num()
        print( "\n********************" )
        print( "* 1. Bubble Sort   *" )
        print( "* 2. K-threads     *" )
        print( "* 3. K-processes   *" )
        print( "* 4. 1-process     *" )
        print( "********************" )
        command = input("Input a choice(0, 1, 2, 3, 4): ")
        if ( command == "0" ):
            break
        elif (command == "1" ):
            while True:
                fileName = input('\nInput a file name ([0] Quit): ' )
                if fileName == '0':
                    break
                elif readF( fileName, total ):
                    start = time.process_time()
                    bubblesort( total )
                    end = time.process_time()
                    sortTime = end - start
                    fileName = fileName + '_output1.txt'
                    writeF( fileName, total, sortTime )
                    break
        elif ( command == "2" ):
            thrNum = []
            while True:
                fileName = input('\nInput a file name ([0] Quit): ' )
                if fileName == '0':
                    break
                else:
                    cutNum = int( input('\nInput a cut file number ([0] Quit): ') )
                    if (cutNum == 0):
                        break
                    elif (cutNum < 0) :
                        print( '\n### Number must bigger than 0! ###')
                    elif readF2( fileName, total, cutNum, thrNum ):
                        start = time.process_time()
                        threads = []
                        for i in range( cutNum ):
                            t = td.Thread(target=bubblesort, args=(thrNum[i],) )
                            t.start()
                            threads.append(t)

                        for i in threads:
                            i.join()

                        temp = thrNum
                        while cutNum > 1:
                            put = []
                            pos = 0
                            threads = []
                            while pos < len(temp):
                                L = temp[pos]
                                if pos+1 < len(temp):
                                    R = temp[pos+1]
                                    t = td.Thread(target=Mergesort, args=(put, L, R, ))
                                    t.start()
                                    threads.append(t)
                                else:
                                    put.append(L)
                                pos += 2

                            for i in threads:
                                i.join()

                            temp = put
                            if cutNum % 2 == 0:
                                cutNum = cutNum // 2
                            else:
                                cutNum = cutNum // 2 +1

                        total = temp[0]
                        end = time.process_time()
                        sortTime = end - start
                        fileName = fileName + '_output2.txt'
                        writeF( fileName, total, sortTime )
                        break
        elif ( command == "3" ):
            mpNum = []
            while True:
                fileName = input('\nInput a file name ([0] Quit): ' )
                if fileName == '0':
                    break
                else:
                    cutNum = int( input('\nInput a cut file number ([0] Quit): ') )
                    if (cutNum == 0):
                        break
                    elif (cutNum < 0) :
                        print( '\n### Number must bigger than 0! ###')
                    elif readF2( fileName, total, cutNum, mpNum ):
                        start = time.time()
                        results = []
                        pool = mp.Pool(cutNum)
                        print
                        for i in range( cutNum ):
                            mpNum = pool.map(bubblesort, mpNum )

                        temp = mpNum
                        while cutNum > 1:
                            results = []
                            put = []
                            pos = 0
                            while pos < len(temp):
                                L = temp[pos]
                                if pos+1 < len(temp):
                                    R = temp[pos+1]
                                    result = pool.apply_async(Mergesort, (results, L, R,))
                                    put.append(result.get())
                                else:
                                    put.append(L)
                                
                                pos += 2

                            temp = put
                            if cutNum % 2 == 0:
                                cutNum = cutNum // 2
                            else:
                                cutNum = cutNum // 2 +1

                        total = temp[0]
                        end = time.time()
                        sortTime = end - start
                        fileName = fileName + '_output3.txt'
                        writeF( fileName, total, sortTime )
                        break
        elif ( command == "4" ):
            num = []
            while True:
                fileName = input('\nInput a file name ([0] Quit): ' )
                if fileName == '0':
                    break
                else:
                    cutNum = int( input('\nInput a cut file number ([0] Quit): ') )
                    if (cutNum == 0):
                        break
                    elif (cutNum < 0) :
                        print( '\n### Number must bigger than 0! ###')
                    elif readF2( fileName, total, cutNum, num ):
                        start = time.process_time()
                        for i in range( cutNum ):
                            bubblesort( num[i] )

                        temp = num
                        while cutNum > 1:
                            put = []
                            pos = 0
                            while pos < len(temp):
                                L = temp[pos]
                                if pos+1 < len(temp):
                                    R = temp[pos+1]
                                    nothing = False
                                    Mergesort(put, L, R)
                                else:
                                    put.append(L)
                                pos += 2
                            temp = put
                            if cutNum % 2 == 0:
                                cutNum = cutNum // 2
                            else:
                                cutNum = cutNum // 2 +1

                        total = temp[0]
                        end = time.process_time()
                        sortTime = end - start
                        fileName = fileName + '_output4.txt'
                        writeF( fileName, total, sortTime )
                        break
        else:
            print( "\nCommand does not exist!")
