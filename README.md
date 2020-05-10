# FilterPsql
Extracts records from a PervasiveSQL UNF file based on a pattern

this little tool extracts data records from multiple files created with BUTIL -RECOVER

when using BUTIL, if the resulting file is greater than 2Gb, it creates files like ".~1" and so on
the resulting files from BUTIL are binary files wich cannot be used with other tools like grep

My problem was to extract all records from a table where position 2-3 greater or equal to B9 and save these records to a new file

This tool works with Python 2.7 and later

Usage : 
usage: filterpsql.py [-h] [-i INFILE] [-o OUTFILE] [-p PATTERN] [-op OP]
                     [-pos POSITION] [-t] [-v]

filter Psql recovery files using a pattern to reduce file size

optional arguments:
  -h, --help            show this help message and exit
  -i INFILE, --infile INFILE
                        input file
  -o OUTFILE, --outfile OUTFILE
                        output file (overwritten if exists)
  -p PATTERN, --pattern PATTERN
                        pattern to search for (in the fixed record part)
                        (default "B9")
  -op OP, --operation OP
                        operation to check lines available operations : gt ge
                        lt le eq ne (defaut "ge")
  -pos POSITION, --position POSITION
                        pattern position from the beginning of the record
                        (comma = 0) (default 2)
  -t, --test_only       Tests input file only
  -v, --verbose         increase verbosity
  
