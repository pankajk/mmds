
Programming Assignment for Mining Massive Datasets
==================================================

The task here is to quickly find the number of pairs of sentences that are at the word-level edit distance at most 1. 
Two sentences S1 and S2 are at edit distance 1 if S1 can be transformed to S2 by: adding, removing or substituting 
a single word.

The input data has 9,397,023 sentences, each one divided by a new line and with the sentence id at the beginning of the 
line. The zip compressed file size is around 500MB (1400MB decompressed).


How I tackled the problem
=========================

First ideas involved hashing not only every sentence, but also all sentences from the input data where each has exactly 
one word missing. Then all matches would be in hashed to the same hash bucket. It turned out that this does not work well
because of too many collissions. It would require a huge hashtable to work well, but my goal was to solve the problem with
only 1GB of memory.

In the end I found a way that works with an even simpler way to find candidate pairs (which have a potential edit distance
of 1, according to the definition above):

Phase 1: 
 * Pretty obvious: Split the file into many files by sentence length, e.g. all sentences with length 10 go to 00010.txt

Phase 2: 
 * For sentence length n: All candidates must be length n or length n-1. Make sure to count same length candidates only once
   (by id1 < id2)
 * Edit distance of 1 by adding one word is the same as deleting one word when checking the longer sentence, so this is
   covered as well
 * To find candidates: Group sentences by first 5 words and last 5 words. All sentences have at least 10 words, so either the
   first 5 or the last 5 (or both) have to be identical. Grouping: put into HashMap with key first/last 5 words, value is a
   list of sentence ids.
 * Also keep these HashMaps from value n-1 and compare against it as well
 

Observations
============
* Java uses Unicode and as a result the Strings take a lot of memory.
* Runtime with 1 GB of memory ~20-25 minutes (+ 70 seconds for splitting the file)
* Runtime with 4 GB of memory ~10 minutes (+ 70 seconds for splitting the file) (both on Core i5-2500K@3.3GHz with 8GB RAM, HD - not SSD)


TODO
====
* Use more than one CPU core for the comparisions (by using the Akka Actor library)


