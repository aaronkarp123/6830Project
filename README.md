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


