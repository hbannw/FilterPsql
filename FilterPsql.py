import sys
import glob
import os
import time
import argparse
from datetime import datetime
parser = argparse.ArgumentParser(description='filter Psql recovery files using a pattern to reduce file size')
parser.add_argument('-i','--infile', help='input file', default = '')
parser.add_argument('-o','--outfile', help='output file (overwritten if exists)', default = '',type = str)
group1 = parser.add_mutually_exclusive_group()
group1.add_argument('-p','--pattern', help='string pattern to search for (in the fixed record part)',type=str)
group1.add_argument('-d','--date', help='date pattern to search for, the binary is converted (in the fixed record part) ',type=str)
group1.add_argument('-b','--binary', help='binary pattern to search for, in hex format (4239 for "B9") (in the fixed record part) ',type=str)
parser.add_argument('-op','--operation', help='operation to check lines available operations : gt ge lt le eq ne (defaut "ge") ', dest ='op', type = str,default = 'ge')
parser.add_argument('-pos','--position', help='pattern position from the beginning of the record (comma = 0) (default 2)',type = int,default = 2)
parser.add_argument('-t','--test_only', help='tests input file only',action = 'store_true')
parser.add_argument("-v", "--verbose",help = 'increase verbosity', action="store_true")
args = parser.parse_args()
def printv(t):
  if args.verbose:
    print(t)
def cnvdate(t):
  return datetime.strptime(str(ord(t[0]))+'/'+str(ord(t[1]))+'/'+str(ord(t[3])*256+ord(t[2])),'%d/%m/%Y')
iname = args.infile
oname = args.outfile
testmode = False
isdate = False
if args.test_only:
  print('checking input files only : no output written')
  testmode= True
key = args.pattern
posit = args.position
if args.date<>None:
  isdate = True
  try:
    keydate = datetime.strptime(args.date,'%d/%m/%Y')
  except:
    print ('error : incorrect date pattern : '+args.date)
    exit()
else:
  if args.binary<>None:
    try:
      key = args.binary.decode("hex")
    except:
      print ('error : incorrect hex pattern : '+args.binary)
      exit()
def lessthan(v,p):
    return v<p
def lessequal(v,p):
    return (v<=p)
def greaterequal(v,p):
  return (v>=p)
def greaterthan(v,p):
  return(v>p)
def equalto(v,p):
  return (v==p)
def notequal(v,p):
  return(v<>p)
operations = {'le':lessequal,'lt':lessthan,'ge':greaterequal,'gt':greaterthan,'eq':equalto,'ne':notequal}
opmethod = operations.get(args.op.lower())
if opmethod is None:
  print('Error : operation ' +args.op+' not allowed')
  parser.print_help()
  exit()
if not testmode:
  try:
     outfile = open(oname, 'wb')
  except:
     print('the filename '+ oname + ' is invalid')
     parser.print_help()
     exit()
count = 0
cvalue = 100000
flen = 0
testlen = 0
rescnt = 0
if not os.path.exists(iname):
  print('the filename '+ iname + ' does not exist')
  parser.print_help()
  exit()
filelist = []
filelist.append(iname)
# read directory for files *.~*
for file in glob.glob(os.path.splitext(iname)[0]+'.~*'):
    filelist.append(file)
printv('list of files to read from : ' + str(filelist))
start_time=time.time()
# Check the files
fok = True
for fn in filelist:
# Checking some records of the file
  printv('Checking file : '+ fn)
  infile=open(fn,'rb')
# read the first line
  onerecord = infile.read(10)
  fpos = onerecord.find(',')
  if fpos<0:
    print('the first record is invalid : ' + onerecord)
    fok = False
    continue
  infile.close
if not fok:
  print('cannot parse file, aborting')
  exit()
for fn in filelist:
  printv('Reading file : '+fn)
  blob=False
  infile = open(fn, 'rb')
  onerecord = infile.read(100)
  fpos = onerecord.find(',')
  if fpos<0:
    print('Cannot parse file  ' + fn +' first record =  ' + onerecord)
    break
  srlen = onerecord[0:fpos+1]
  infile.seek(0,0)
  #record length = value of the number before the comma + number of digits before comma + one for the comma + 2 for CRLF
  flen = int(onerecord[0:fpos])+fpos + 1 + 2
  printv('record length : '+str(flen))
  while True:
      if blob:
        onerecord = infile.read(10)
        fpos = onerecord.find(',')
        if len(onerecord) < 10:
          break
        if fpos<0:
          print('Error reading file ' + fn +' line ' + str(count)+' current record =  ' + onerecord + ' stopping')
          exit()
        flen = int(onerecord[0:fpos])+fpos + 1 + 2
        infile.seek(-10,1)

      onerecord = infile.read(flen)
      if len(onerecord) < flen:
          break
      if not blob:
        if onerecord[0:fpos+1]<>srlen:
           blob = True
           infile.seek(-(flen),1)
           onerecord = infile.read(flen)
           infile.seek(-(flen),1)
           printv('Variable record detected. '+ onerecord[0:10])
           continue
      if count >= cvalue and (count % cvalue == 0):
      	  printv(str(count) + ' records read, '+ str(rescnt) + ' records selected so far in {:.{prec}f}'.format(time.time() - start_time, prec=2) + ' seconds')
      count = count + 1
      if testmode:
        print(onerecord)
        if count>4:
          break
      else:
        if isdate:
          if opmethod(cnvdate(onerecord[fpos+posit:fpos+posit+4]),keydate):
            rescnt = rescnt + 1
            outfile.write(onerecord)
        else:
          if opmethod(onerecord[fpos+posit:fpos+posit+len(key)],key):
            rescnt = rescnt + 1
            outfile.write(onerecord)
  infile.close()
  printv(str(count) + ' records read, '+ str(rescnt) + ' records selected ')
  printv(fn + ' finished')
  printv('Total time : {:.{prec}f}'.format(time.time() - start_time, prec=2) + ' seconds')
if not args.verbose:
  print(str(count) + ' records read, '+ str(rescnt) + ' records selected, total time : {:.{prec}f}'.format(time.time() - start_time, prec=2) + ' seconds')
if not testmode:
  outfile.close()
