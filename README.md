scale-contest-evaluator
=======================

The evaluator script for the Prezi Scale contest

Download test log and decompress it:

    curl -O https://scale.contest.prezi.com.s3.amazonaws.com/week_1.log.bz2

    bzip2 -d week_1.log.bz2

Evaluate:

    ./simple_competitor.py < week_1.log | ./evaluator.py

For more information please visit https://prezi.com/scale/
