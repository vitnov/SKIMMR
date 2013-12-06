#!/usr/bin/python
"""
exst_gt.py - script for extraction of co-occurrence statements from text

Guide to execution:

python exst_gt.py [FOLDER]

where FOLDER is a path to the folder that contains the texts to be processed

Copyright (C) 2012 Vit Novacek (vit.novacek@deri.org), Digital Enterprise
Research Institute (DERI), National University of Ireland Galway (NUIG)
All rights reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
"""

import sys, os, time
from skimmr_gt.extr import *

if __name__ == "__main__":
  path = os.getcwd()
  if len(sys.argv) > 1:
    path = os.path.abspath(sys.argv[1])
  else:
    # setting the path automatically
    path = os.path.join(os.getcwd(),'text')
  print 'Chopping up the text into paragraphs...'
  start = time.time()
  split_pars(path)
  end = time.time()
  print '...finished in %s seconds' % (str(end-start),)
  print 'POS tagging the paragraphs...'
  start = time.time()
  postag_texts(path)
  end = time.time()
  print '...finished in %s seconds' % (str(end-start),)
  print 'Computing the co-occurrence statements...'
  start = time.time()
  extract_cooc(path)
  end = time.time()
  print '...finished in %s seconds' % (str(end-start),)
  print 'Generating the source statement file...'
  # attempting to estimate the limit for the number of statements
  st_lim = 750000
  try:
    import psutil
    st_lim = 3000000*(psutil.TOTAL_PHYMEM / float(8*(2**30)))
  except ImportError:
    st_lim = 750000
  # generating up to st_lim statements from the co-occurrence info
  start = time.time()
  gen_src(path,max_stmt=st_lim)
  end = time.time()
  print '...finished in %s seconds' % (str(end-start),)
