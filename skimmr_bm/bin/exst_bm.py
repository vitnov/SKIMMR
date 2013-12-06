#!/usr/bin/python
"""
exst_bm.py - script for extraction of co-occurrence statements from text

Guide to execution:

python exst_bm.py [METHOD] [FOLDER]

where METHOD is one of 'lingpipe' (default) and 'builtin', determining which 
extraction method will be used (either the biomedically-specific LingPipe,
based on GENIA corpus, or the internal SKIMMR one, based on general language
corpus). FOLDER is a path to the folder with the alternative LingPipe 
installation (if different from the 'lingpipe' directory in the SKIMMR
working folder).

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

import sys, os, time, commands
from xml.etree.ElementTree import parse
from itertools import combinations
from math import fabs
from skimmr_bm.extr import *
from skimmr_bm.util import COOC_RELNAME
from skimmr_bm.util import SRCSTM_FNAME

def use_builtin(path):
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

def parse_xml(fname):
  root = parse(fname)
  # going through all sentences in the file, updating the term->sentence 
  # number inverse index
  terms2sentno = {}
  for sentence in root.findall('.//s'):
    sent_no = float(sentence.attrib.get('i'))
    for ne in sentence.findall('ENAMEX'):
      ne_text = ne.text.lower()
      # updating the inverse index
      if not ne_text in terms2sentno:
        terms2sentno[ne_text] = set()
      terms2sentno[ne_text].add(sent_no)
  return terms2sentno

def gen_stm(terms2sentno,file_id):
  # generate all 2-combinations of terms2sentno.keys(), compute their weights
  lines = []
  for t1, t2 in combinations(terms2sentno.keys(),2):
    w = 0
    for pos1 in terms2sentno[t1]:
      for pos2 in terms2sentno[t2]:
        w += 1.0/(1.0+fabs(pos1-pos2))
    # updating the line list with the co-occurrence statement
    lines.append('\t'.join([t1,COOC_RELNAME,t2,file_id,str(w)]))
  return lines

def use_lingpipe(text_path,lp_path):
  print 'Chopping up the text into paragraphs...'
  start = time.time()
  split_pars(text_path)
  end = time.time()
  print '...finished in %s seconds' % (str(end-start),)
  # remembering the SKIMMR working directory
  skimmr_wdir = os.path.abspath(os.getcwd())
  # change the working directory to the LingPipe script folder
  os.chdir(os.path.abspath(os.path.join(lp_path,'demos','generic','bin')))
  # command pattern for processing a single file (non-Windows by default)
  cmd_pattern = 'cmd_ne_en_bio_genia.sh -inFile=%s -outFile=%s'
  if sys.platform.startswith('win'):
    # if we're on a Windows system
    cmd_pattern = 'cmd_ne_en_bio_genia.bat -inFile=%s -outFile=%s'
  # processing the paragraph files
  print 'Running the NE recognition on the paragraphs...'
  start = time.time()
  fnames = [x for x in os.listdir(text_path) if \
    os.path.splitext(x)[-1].lower() == '.par']
  i = 0
  for fname in fnames:
    core_name = os.path.splitext(fname)[0].lower()
    i_fname = os.path.join(text_path,fname)
    o_fname = os.path.join(text_path,core_name+'.xml')
    cmd_string = cmd_pattern % (i_fname,o_fname)
    output = commands.getoutput(os.path.join(os.getcwd(),cmd_string))
    print output
    i += 1
    print '  ... NE recognition - processed', i, 'out of', len(fnames)
  end = time.time()
  print '...NE recognition finished in %s seconds' % (str(end-start),)
  # changing the working directory back to SKIMMR
  os.chdir(skimmr_wdir)
  # parsing the LingPipe output
  print 'Generating statements from the LingPipe output...'
  start = time.time()
  fnames = [x for x in os.listdir(text_path) if \
    os.path.splitext(x)[-1].lower() == '.xml']
  lines, i = [], 0
  for fname in fnames:
    terms2sentno = parse_xml(os.path.join(text_path,fname))
    file_id = os.path.splitext(fname)[0]
    lines += gen_stm(terms2sentno,file_id)
    i += 1
    print '  ... generating statements - processed', i, 'out of', len(fnames)
  end = time.time()
  print '...statement generation finished in %s seconds' % (str(end-start),)
  # storing the statement file
  print 'Storing the resulting statement file as:', \
    os.path.join(text_path,SRCSTM_FNAME)
  start = time.time()
  f = open(os.path.join(text_path,SRCSTM_FNAME),'w')
  errors = 0
  for line in lines:
    try:
      f.write(line.encode('ascii','ignore')+'\n')
    except UnicodeEncodeError:
      errors += 1
      sys.err.write('W@exst_bm.py - Unicode error, omitting statement: %s\n' %\
        line)
  f.close()
  end = time.time()
  print '...finished in %s seconds' % (str(end-start),)
  print '...number of Unicode errors and ommitted statements:', errors

if __name__ == "__main__":
  # setting the main parameters
  method = 'lingpipe'
  text_path = os.path.abspath(os.path.join(os.getcwd(),'text'))
  lp_path = os.path.abspath(os.path.join(os.getcwd(),'lingpipe'))
  if len(sys.argv) > 1:
    if sys.argv[1] in ['lingpipe','builtin']:
      method = sys.argv[1]
    elif sys.argv[1] not in ['lingpipe','builtin'] and \
    os.path.exists(os.path.abspath(sys.argv[1])):
      lp_path = os.path.abspath(sys.argv[1])
  if len(sys.argv) > 2:
    if sys.argv[1] in ['lingpipe','builtin']:
      method = sys.argv[1]
    elif os.path.exists(os.path.abspath(sys.argv[2])):
      lp_path = os.path.abspath(sys.argv[2])
  # executing the selected method of extraction
  if method == 'lingpipe':
    use_lingpipe(text_path,lp_path)
  elif method == 'builtin':
    use_builtin(text_path)
  else:
    print 'Unknown extraction method, try one of [lingpipe|builtin]'
