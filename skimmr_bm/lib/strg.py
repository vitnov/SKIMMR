"""
strg.py - a storage module implementing the following functionalities:
- memory-based statement store
- lexicon mapping between the lexical and numerical ID representation of terms
  in the statements
- functions for import/export of data, (de)serialisation, corpus computation,
  normalisation, etc.

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

import sys, os, cPickle, gzip, time, re
import util
from util import Tensor
from proc import Analyser
from math import log

# types of all possible perspectives on a ternary corpus
PERSP_TYPES = ['LAxLIRA','LIxLARA','RAxLALI','LIRAxLA','LARAxLI','LALIxRA',\
'LAxLIRA_COMPRESSED','LIxLARA_COMPRESSED','RAxLALI_COMPRESSED',\
'LIRAxLA_COMPRESSED','LARAxLI_COMPRESSED','LALIxRA_COMPRESSED']

# pivot dimensions to 'lock' when computing a perspective matricisation
PERSP2PIVDIM = {
  'LAxLIRA' : 0,
  'LIxLARA' : 1,
  'RAxLALI' : 2,
  'LIRAxLA' : (1,2),
  'LARAxLI' : (0,2),
  'LALIxRA' : (0,1)
}

class Lexicon:
  """
  Two-way dictionary mapping lexical expressions to unique integer identifiers.
  """

  def __init__(self,items=[]):
    self.lex2int = {}
    self.int2lex = {}
    self.freqdct = {}
    self.current = 0
    if len(items):
      self.update(items)

  def __len__(self):
    return len(self.lex2int)

  def normalise(self,expr):
    # string normalisation
    return expr.lower().strip().replace(' ','_')

  def load(self,filename,normalise=True):
    # update the lexicon using a specified filename
    if normalised:
      self.update([self.normalise(x) for x in open(filename,'r')])
    else:
      self.update([x for x in open(filename,'r')])

  def from_file(self,filename):
    # import a lexicon from a tab-separated file (including the index mapping
    # and frequency of the token); expected format: token index frequency
    lines = []
    try:
      # assuming a file object
      lines = filename.read().split('\n')
    except AttributeError:
      # assuming a filename
      try:
        lines = open(filename,'r').read().split('\n')
      except:
        # if neither file nor filename, proceed with empty lines
        sys.stderr.write('W (importing a lexicon) - cannot import from: %s\n',\
          str(filename))
    for line in lines:
      try:
        expr, indx, freq = line.split('\t')[:3]
        indx = int(indx)
        freq = int(freq)
        self.lex2int[expr] = indx
        self.int2lex[indx] = expr
        self.freqdct[expr] = freq
      except:
        sys.stderr.write('W (importing a lexicon) - fishy line:\n%s' % (line,))
    self.current = max(self.int2lex.keys()) + 1

  def to_file(self,filename):
    errors = 0
    # exporting a lexicon - inverse to import
    try:
      # assuming a file object
      #filename.write('\n'.join(['\t'.join([x,str(self.lex2int[x]),\
      #  str(self.freqdct[x])]) for x in self.lex2int]))
      for lex in  self.lex2int:
        try:
          filename.write('\t'.join([lex,str(self.lex2int[lex]),\
            str(self.freqdct[lex])])+'\n')
        except UnicodeEncodeError, UnicodeDecodeError:
          errors += 1
      filename.flush()
      os.fsync(filename.fileno())
    except AttributeError:
      # assuming a filename
      try:
        f = open(filename,'w')
        f.write('\n'.join(['\t'.join([x,str(self.lex2int[x]),\
          str(self.freqdct[x])]) for x in self.lex2int]))
        f.close()
      except:
        # if neither file nor filename, proceed with empty lines
        sys.stderr.write('W (exporting a lexicon) - cannot export to: %s\n',\
          str(filename))
    return errors

  def update(self,items):
    updates = None
    if type(items) in [str,unicode]:
      # make sure that single word updates are handled correctly
      updates = [items]
    else:
      # expect iterable here
      updates = list(items)
    for item in updates:
      # updating the frequency dictionary first
      if item in self.freqdct:
        self.freqdct[item] += 1
      else:
        self.freqdct[item] = 1
      if item not in self.lex2int:
        # udpate the dictionaries if the item is not present
        self.lex2int[item] = self.current
        self.int2lex[self.current] = item
        self.current += 1

  def __getitem__(self,key):
    if type(key) in [int,long]:
      if self.int2lex.has_key(key):
        return self.int2lex[key]
      else:
        raise KeyError('Index %s not present in the lexicon' % (key,))
    elif type(key) in [unicode,str]:
      if self.lex2int.has_key(key):
        return self.lex2int[key]
      else:
        raise KeyError('Expression %s not present in the lexicon' % (key,))
    else:
      raise NotImplementedError('Unknown index or expression type: %s' % \
        (str(type(key)),))

  def has_key(self,key):
    if type(key) in [int,long]:
      return key in self.int2lex
    elif type(key) in [unicode,str]:
      return key in self.lex2int
    else:
      return False

  def __contains__(self,key):
    return self.has_key(key)

  def items(self):
    return self.lex2int.items()

  def token_size(self):
    # size in overall number of non-unique tokens
    return sum(self.freqdct.values())

  def freq(self,token):
    # frequency of a token in the lexicon
    if type(token) in [str,unicode]:
      if token in self.freqdct:
        return self.freqdct[token]
      else:
        return 0
    elif type(token) in [int,long]:
      if self.int2lex[token] in self.freqdct:
        return self.freqdct[self.int2lex[token]]
      else:
        return 0
    else:
      return 0

  def sorted(self,reverse=True,limit=-1,ignored=[],lexical=False):
    # list of lexicalised tokens sorted according to their frequency;
    # limit can be one of the following:
    # < 0 ... no limit, all terms are returned 
    # = 0 ... dynamic limit - all values with higher than average values, with
    #         the average computed while possibly omitting anything that 
    #         matches any of the REs in ignored list
    # > 0 ... impose a limit
    l = self.freqdct.items()
    l.sort(key=lambda x: x[1],reverse=reverse)
    # restricting the list according to a possible limit value
    if limit > 0:
      # impose a fixed limit
      l = l[:limit]
    elif limit == 0:
      # compute a dynamic limit
      l_cut = []
      # creating a list without ignored stuff
      for item in l:
        ignore = False
        for regexp in ignored:
          if re.search(regexp,item[0]):
            ignore = True
            break
        if not ignore:
          l_cut.append(item)
      # copmputing average from that list
      avg = sum([x[1] for x in l_cut])/float(len(l_cut))
      # including only the values >= average
      l = [x for x in l_cut if x[1] >= avg]
    else:
      # keep the list as is
      pass
    if lexical:
      # returning lexical values
      return [x[0] for x in l]
    # returning integer ID values
    return [self.lex2int[x[0]] for x in l]

class MemStore:

  def __init__(self,trace=False):
    self.lexicon = Lexicon()
    self.sources = Tensor(rank=4)
    self.corpus = Tensor(rank=3)
    self.perspectives = dict([(x,Tensor(rank=2)) for x in PERSP_TYPES])
    self.types = {}
    self.synonyms = {}
    self.trace = trace

  def convert(self,statement):
    """
    Backwards compatibility function for converting between integer and 
    string representations of statements.
    """

    return tuple([self.lexicon[x] for x in statement])

  def incorporate(self,path,ext='.tsv'):
    """
    Imports the statements into the store, processing all files with the 
    specified extension ext in the path location. Lexicon and sources 
    structures are updated (not overwritten) in the process. 
    """

    # first pass to update the lexicon
    expressions = []
    for fname in [os.path.join(path,x) for x in os.listdir(path) if 
    os.path.isfile(os.path.join(path,x)) and \
    os.path.splitext(x)[-1].lower() == ext.lower()]:
      for line in open(fname,'r'):
        try:
          s,p,o,prov,rel = line.split('\t')[:5]
          rel = float(rel)
          expressions += [s,p,o,prov]
        except:
          sys.stderr.write('W (loading memory-based store) - '+\
            'something wrong with line:\n%s' % (line,))
    self.lexicon.update(expressions)
    # second pass to update the sources tensor
    for fname in [os.path.join(path,x) for x in os.listdir(path) if 
    os.path.isfile(os.path.join(path,x)) and \
    os.path.splitext(x)[-1].lower() == ext.lower()]:
      for line in open(fname,'r'):
        try:
          s,p,o,prov,rel = line.split('\t')[:5]
          rel = float(rel)
          key = tuple([self.lexicon[x] for x in [s,p,o,prov]])
          self.sources[key] = rel
        except:
          sys.stderr.write('W (loading memory-based store) - '+\
            'something wrong with line:\n%s' % (line,))

  def dump(self,filename):
    # straightforward (but somehow slow) (de)serialisation using cPickle
    cPickle.dump(self,open(filename,'wb'))

  def load(self,filename):
    # straightforward (but somehow slow) (de)serialisation using cPickle
    self = cPickle(open(filename,'rb'))

  def exp(self,path,compress=True,core_only=True):
    # exporting the whole store as tab-separated value files to a directory
    # (gzip compression is used by default)
    # note that only lexicon, sources and corpus structures are exported, any
    # possibly precomputed corpus perspectives have to be re-created!
    # also, integer indices are used - for lexicalised (human readable) export
    # of sources and corpus, use exportSources() and exportCorpus() functions
    # setting the filenames
    lex_fn = os.path.join(path,'lexicon.tsv')
    src_fn = os.path.join(path,'sources.tsv')
    crp_fn = os.path.join(path,'corpus.tsv')
    if compress:
      lex_fn += '.gz'
      src_fn += '.gz'
      crp_fn += '.gz'
    openner, sig = open, 'w'
    if compress:
      openner, sig = gzip.open, 'wb'
    lex_f = openner(lex_fn,sig)
    src_f = openner(src_fn,sig)
    crp_f = openner(crp_fn,sig)
    self.lexicon.to_file(lex_f)
    self.sources.to_file(src_f)
    self.corpus.to_file(crp_f)
    lex_f.close()
    src_f.close()
    crp_f.close()

  def imp(self,path,compress=True):
    # importing the whole store as tab-separated value files from a directory
    # effectively an inverse of the exp() function
    lex_fn = os.path.join(path,'lexicon.tsv')
    src_fn = os.path.join(path,'sources.tsv')
    crp_fn = os.path.join(path,'corpus.tsv')
    if compress:
      lex_fn += '.gz'
      src_fn += '.gz'
      crp_fn += '.gz'
    openner, sig = open, 'r'
    if compress:
      openner, sig = gzip.open, 'rb'
    lex_f = openner(lex_fn,sig)
    src_f = openner(src_fn,sig)
    crp_f = openner(crp_fn,sig)
    self.lexicon.from_file(lex_f)
    self.sources.from_file(src_f)
    self.corpus.from_file(crp_f)
    lex_f.close()
    src_f.close()
    crp_f.close()

  def computeCorpus(self):
    # number of all triples
    N = 0 
    # x -> number of independednt occurences in the store
    indep_freq = {}
    # (x,y) -> number of joint occurences in the store
    joint_freq = {}
    # (s,p,o) -> number of occurences
    tripl_freq = {}
    # (s,p,o) -> (provenance, relevance)
    spo2pr = {}
    # going through all the statements in the sources
    for s,p,o,d in self.sources.keys():
      N += 1
      if indep_freq.has_key(s):
        indep_freq[s] += 1
      else:
        indep_freq[s] = 1
      if indep_freq.has_key(o):
        indep_freq[o] += 1
      else:
        indep_freq[o] = 1
      if joint_freq.has_key((s,o)):
        joint_freq[(s,o)] += 1
      else:
        joint_freq[(s,o)] = 1
      if tripl_freq.has_key((s,p,o)):
        tripl_freq[(s,p,o)] += 1
      else:
        tripl_freq[(s,p,o)] = 1
      if not spo2pr.has_key((s,p,o)):
        spo2pr[(s,p,o)] = []
      spo2pr[(s,p,o)].append((d,self.sources[(s,p,o,d)]))
    # going only through the unique triples now regardless of their provenance
    for s,p,o in spo2pr:
      # a list of relevances of particular statement sources
      src_rels = [x[1] for x in spo2pr[(s,p,o)]]
      # absolute frequency of the triple times it's mutual information score
      joint = joint_freq[(s,o)]
      if (o,s) in joint_freq:
        joint += joint_freq[(o,s)]
      # frequency times mutual information score
      fMI = 0.0
      try:
        fMI = \
        tripl_freq[(s,p,o)]*log(float(N*joint)/(indep_freq[s]*indep_freq[o]),2)
      except ValueError:
        continue
      # setting the corpus tensor value
      self.corpus[(s,p,o)] = fMI*(float(sum(src_rels))/len(src_rels))

  def normaliseCorpus(self,cut_off=0.95,min_quo=0.1):
    # corpus normalisation by a value that is greater or equal to the 
    # percentage of weight values given by the cut_off parameter
    # (if the values are below zero, they are set to the min_quo 
    # fraction of the minimal normalised value
    ws = sorted(self.corpus.values())
    norm_cons = ws[int(cut_off*len(ws)):][0]
    min_norm = min([x for x in ws if x > 0])*min_quo
    for key in self.corpus:
      w = self.corpus[key]/norm_cons
      if w < 0:
        w = min_norm
      if w > 1:
        w = 1.0
      self.corpus[key] = w

  def computePerspective(self,ptype):
    self.perspectives[ptype] = self.corpus.matricise(PERSP2PIVDIM[ptype])

  def indexSources(self):
    self.sources.index()

  def indexCorpus(self):
    self.corpus.index()

  def indexPerspective(self,ptype):
    self.perspectives[ptype].index()

  def getProvenance(self,statement):
    # getting the statement elements
    s,p,o = statement
    # getting integer ID versions of the statement elements
    if type(s) in [unicode,str]:
      s = self.lexicon[s]
    if type(p) in [unicode,str]:
      p = self.lexicon[p]
    if type(o) in [unicode,str]:
      o = self.lexicon[o]
    # evalating query on the sources tensor and collating the results
    return [x[3] for x, rel in self.sources.query((s,p,o,None))]
    
  def getRelevance(self,prov):
    if type(prov) in [unicode,str]:
      prov = self.lexicon[prov]
    return max(set([rel for x, rel in \
      self.sources.query((None,None,None,prov))]))

  def exportSources(self,filename,lexicalised=True):
    # export the sources tensor to a file, in a tab-separated value format,
    # either with integer or lexicalised keys
    f = open(filename,'w')
    if lexicalised:
      f.write('\n'.join(['\t'.join([self.lexicon[x] for x in [s,p,o,d]]+\
        [str(w)]) for (s,p,o,d),w in self.sources.items()]))
    else:
      f.write('\n'.join(['\t'.join([str(x) for x in [s,p,o,d,w]]) for \
        (s,p,o,d),w in self.sources.items()]))
    f.close()

  def exportCorpus(self,filename,lexicalised=True):
    # export the corpus tensor to a file, in a tab-separated value format
    # either with integer or lexicalised keys
    f = open(filename,'w')
    if lexicalised:
      f.write('\n'.join(['\t'.join([self.lexicon[x] for x in [s,p,o]]+[str(w)])\
        for (s,p,o),w in self.corpus.items()]))
    else:
      f.write('\n'.join(['\t'.join([str(x) for x in [s,p,o,w]]) for \
        (s,p,o),w in self.corpus.items()]))
    f.close()

if __name__ == "__main__":
  action, in_path, out_path = 'create', os.getcwd(), os.getcwd()
  if len(sys.argv) > 1:
    action = sys.argv[1]
  if len(sys.argv) > 2:
    in_path = os.path.abspath(sys.argv[2])
  if len(sys.argv) > 3:
    out_path = os.path.abspath(sys.argv[3])
  if action == 'create':
    # creating a new store
    store = MemStore()
    print 'Loading the statements from:', in_path
    start = time.time()
    store.incorporate(in_path)
    end = time.time()
    print '...finished in %s seconds' % (str(end-start),)
    print 'Computing the corpus'
    start = time.time()
    store.computeCorpus()
    end = time.time()
    print '...finished in %s seconds' % (str(end-start),)
    print 'Normalising the corpus'
    start = time.time()
    store.normaliseCorpus()
    end = time.time()
    print '...finished in %s seconds' % (str(end-start),)
    print 'Exporting the store to:', out_path
    start = time.time()
    store.exp(out_path)
    end = time.time()
    print '...finished in %s seconds' % (str(end-start),)
  elif action == 'compsim':
    # computing the similarities in an existing store
    # name of the similarity relation
    REL_NAME = 'related_to'
    # maximum number of similar items
    SIM_LIM = 10
    store = MemStore(trace=True)
    print '*** Loading the store from:', in_path
    store.imp(in_path)
    orig_size = len(store.corpus)
    print '  ... size as loaded:', len(store.corpus)
    # updating the lexicon with the similarity relation name
    store.lexicon.update(util.SIMR_RELNAME)
    rel_id = store.lexicon[util.SIMR_RELNAME]
    print '*** Computing the LAxLIRA perspective'
    store.computePerspective('LAxLIRA')
    print '*** Initialising the analyser'
    analyser = Analyser(store,'LAxLIRA',compute=False)
    print '*** Computing the similar terms...'
    # processing all the terms in the lexicon, computing the similarities
    #term_ids = store.lexicon.lex2int.values()
    # processing only average and higher frequencies
    term_ids = store.lexicon.sorted(limit=0,\
      ignored=['.*_[0-9]+$','close_to','related_to'])
    sim_dict = {}
    i = 0
    for t1 in term_ids:
      i += 1
      print '  ...', i, 'out of', len(term_ids)
      # getting a fresh similarity statement provenance index
      sims2src = {}
      # computing the similarities for the given term
      for t2, s in analyser.similarTo(t1,top=SIM_LIM,sims2src=sims2src):
        if not ((t1,rel_id,t2) in sim_dict or (t2,rel_id,t1) in sim_dict):
          # adding if a symmetric one was not added before
          sim_dict[(t1,rel_id,t2)] = s
    # storing the computed values to the corpus
    for key, value in sim_dict.items():
      store.corpus[key] = value
    print '*** Exporting the updated store to:', out_path
    print '  ... size as loaded                 :', orig_size
    print '  ... size with similarities computed:', len(store.corpus)
    store.exp(out_path)
  else:
    print 'Unknown action, try again'
