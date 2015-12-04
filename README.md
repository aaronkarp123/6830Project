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
