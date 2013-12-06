#!/usr/bin/python
"""
ixkb_gt.py - script for indexing the knowledge base

Guide to execution:

python ixkb_gt.py [FOLDER]

where FOLDER is a path to the knowledge base one wishes to index.

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
from skimmr_gt import util
from skimmr_gt.ifce import *

if __name__ == "__main__":
  # setting the paths to the store and index
  store_path = os.getcwd()
  if len(sys.argv) > 1:
    store_path = os.path.abspath(sys.argv[1])
  else:
    # setting the store to the default value
    store_path = os.path.join(os.getcwd(),'data','stre')
  lexicon_path = os.path.join(store_path,'lexicon.tsv.gz')
  sources_path = os.path.join(store_path,'sources.tsv.gz')
  corpus_path = os.path.join(store_path,'corpus.tsv.gz')
  index_path = os.path.join(store_path,'index')
  fulltext_path = os.path.join(store_path,'index','fulltext')
  if not os.path.exists(index_path):
    # create the index directory if necessary
    os.makedirs(index_path)
  if not os.path.exists(fulltext_path):
    # create the fulltext index directory if necessary
    os.makedirs(fulltext_path)
  # proceeding with creating the indices
  print '*** Creating the fulltext index'
  print '  ... loading the lexicon from:', lexicon_path
  start = time.time()
  lexicon = load_lex(lexicon_path)
  end = time.time()
  print '...finished in %s seconds' % (str(end-start),)
  print '  ... updating/creating the fulltext index at:', fulltext_path
  # opening/creating the fulltext index
  start = time.time()
  ftext = open_index(fulltext_path)
  text2index = {}
  # getting the text, identifier pairs from the lexicon
  print '  - generating the (text, identifier) tuples'
  for text, index in lexicon.lex2int.items():
    text2index[text] = str(index)
  print '  - using the tuples to fill the fulltext index'
  update_index(ftext,text2index)
  end = time.time()
  print '...finished in %s seconds' % (str(end-start),)
  print '*** Creating the corpus indices'
  print '  ... loading the corpus from:', corpus_path
  start = time.time()
  corpus = load_corpus(corpus_path)
  end = time.time()
  print '...finished in %s seconds' % (str(end-start),)
  i, suid_lines, term_dict = 0, [], {}
  print '  ... generating the CSV representations from the corpus'
  start = time.time()
  for (s,p,o), w in corpus.items():
    # updating the lines of the SUID CSV
    suid_lines.append('\t'.join([str(x) for x in [i,s,p,o,w]]))
    i += 1
    # updating the TERM CSV dictionary
    if not s in term_dict:
      term_dict[s] = set()
    if not o in term_dict:
      term_dict[o] = set()
    term_dict[s].add((o,w))
    term_dict[o].add((s,w))
  end = time.time()
  print '...finished in %s seconds' % (str(end-start),)
  print '  ... storing the CSV file:', os.path.join(index_path,'suids.tsv.gz')
  start = time.time()
  f = gzip.open(os.path.join(index_path,'suids.tsv.gz'),'wb')
  f.write('\n'.join(suid_lines))
  f.close()
  end = time.time()
  print '...finished in %s seconds' % (str(end-start),)
  print '  ... storing the CSV file:', \
    os.path.join(index_path,'termsets.tsv.gz')
  start = time.time()
  term_lines = []
  for t1, rel_set in term_dict.items():
    for t2, w in rel_set:
      term_lines.append('\t'.join([str(x) for x in [t1,t2,w]]))
  f = gzip.open(os.path.join(index_path,'termsets.tsv.gz'),'wb')
  f.write('\n'.join(term_lines))
  f.close()
  end = time.time()
  print '...finished in %s seconds' % (str(end-start),)
  print '*** Creating the provenance index'
  print '  ... loading the sources from:', sources_path
  start = time.time()
  sources = load_src(sources_path)
  end = time.time()
  print '...finished in %s seconds' % (str(end-start),)
  print '  ... loading the statement -> SUID mapping from:', \
    os.path.join(index_path,'suids.tsv.gz')
  start = time.time()
  stmt2suid = load_suids(os.path.join(index_path,'suids.tsv.gz'))
  end = time.time()
  print '...finished in %s seconds' % (str(end-start),)
  print '  ... generating the SUID->PUID mapping for co-occurrence statements'
  # SUID -> PUID dictionary for the SUID -> PUID similarity statements later on
  start = time.time()
  suid2puid = gen_cooc_suid2puid(sources,stmt2suid)
  end = time.time()
  print '...finished in %s seconds' % (str(end-start),)
  # compressed file for the provenance CSV
  f_out = gzip.open(os.path.join(index_path,'provenances.tsv.gz'),'wb')
  print '  ... storing the SUID->PUID mapping for co-occurrence statements'
  start = time.time()
  lines = []
  for suid in suid2puid:
    for puid, w in suid2puid[suid]:
      lines.append('\t'.join([str(suid),str(puid),str(w)]))
  f_out.write('\n'.join(lines))
  print '  ... generating/storing the SUID->PUID mapping for sim. statements'
  simrel_id = -1
  try:
    simrel_id = lexicon[util.SIMR_RELNAME]
  except KeyError:
    sys.stderr.write('\nW@ixkb.py - no similarity relationships present\n')
  missing, processed = gen_sim_suid2puid(stmt2suid,suid2puid,f_out,simrel_id)
  end = time.time()
  print '...finished in %s seconds' % (str(end-start),)
  print '  - missing sim. provenance info     :', missing
  print '  - generated sim. provenance entries:', processed
  f_out.close()
