6830Project
===========

Fall 2015 6.830 Course Project: Efficient locking scheme for B-Trees (based on SimpleDB)

zgw:
Hey guys, I finally decided to use my repo as the baseline.
(I don't think mine is better than yours.
But I think the BufferPool and BTree implementation in this repo
is quite clean and easy to debug.)
However, I am not super confident about my code.
It's not perfect and is not guaranteed to be correct.
So feel free to debug it.

Currently I disabled 5-10 threads' Transaction system test and BTreeTest.
It's hard to debug them under timeout resolution strategy.
I am implementing the cycle detection scheme, and once that is done, we can move on.

So forget about concurrency. This is a reasonable baseline to implement at least the serial version
of the B-Tree that was proposed in the paper.

Other notes

For BLinkTree - got rid of the parent and left sibling pointers since I'm not sure whether its possible to make them work correctly under new semantics.

The notion of highkey is only valid if there is no right Sibling. Therefore the field highkey is not maintained if no right sibling.
(If it was to be maintained, then each page down the hierachy would have to be updated when a key greater than the largest on record is inserted)

I'm not too sure if the concurrent insert procedure given in the paper holds when the root splits. Therefore the current test makes sure that this doesn't happen (by some sequential inserts untils the root splits twice) 
http://stackoverflow.com/questions/15753534/how-blink-tree-cope-with-such-situation

http://www.cs.cmu.edu/~christos/courses/721.S03/LECTURES-PDF/0330-CC_Btrees.PDF

Deadlock not possible when doing inserts only. Paper proves this. 
But they can happen when doing a inserts and deletions together

Pointer to the repo.

Bugs that still exist

1. No clean pages to evict
2. Deadlock Detection scheme has some false positives

Numbers 

300 - 570 / 63
400 - 1300 / 75
500 - 2500 / 90
600 - 6000 / 110
700 - 12000  / 138

Median out of 9 runs considered, very significant variation

Tx - 100 200 300 400 500 600 700
Blink - 30 45 63 75 90 110 138
BTree with replay - 80 250 700 1300 3500 7000 -
Btree w/o replay - 85 300 570 1300 2500 6000 12000
serial insert - 36 65 89 119 172 190 221
Blink delete - 39 74 108 140 162 172 226

Tx - 1000 2000 4000 8000 16000
BTree - 302 602 1200 2400 4800
Blink - 199 368 629 1096 2126



Other results 

1. When doing inserts only, no effect on numPages

When doing deletions, first acquire a random tuple. Then delete. 
Acquiring tuple is done without locks, but deletions require a READ_WRITE lock. 
If two threads acquire the same tuple, then one has to wait for the other to release

Theoretically doing deletions should consume space. but in practice its not much. (unless its just deletions)
