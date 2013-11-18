# cbreaker

C-C-C-C-Category Cycle Breaker (breaking the category cycles on
wikipedia, currently based on dumps for initial cycle-finding).

NOTE: no longer uses sqlite, but the extract scripts are left around
as additional examples of how to use the dump_loader.py

## dump_loader

dump_loader.py is a very basic pure-Python MySQL dump parser that is
suitable for loading some or all of a Wikipedia SQL dump into SQLite
or some other form of data structure/file. No dependencies, just
regexes and evals.  It's used for this and also Commons Radio (check
the hatnote Github account for that).
