"""
util.py - utility functions for the SKIMMR modules, namely the following 
(including implementation of the minimalistic tensor and fuzzy set data
structures, and wrapper for parallel computations)

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

import sys, os, datetime, time, math
from multiprocessing import Process, Queue, Lock, cpu_count
from Queue import Empty
from nltk.stem.porter import PorterStemmer
from nltk.corpus import wordnet as wn
from nltk.stem.wordnet import WordNetLemmatizer

lmtzr = WordNetLemmatizer()

# verb nominalisation stuff

STP = PorterStemmer()
# @TODO - possibly add some more
SUFFIXES = ['ion', 'tion', 'ation', 'ment', 'al', 'ure', 'ing']
# co-occurrence relation name
COOC_RELNAME = 'close_to'
# semantic similarity relation name
SIMR_RELNAME = 'related_to'
# default source statement file name
SRCSTM_FNAME = 'srcstm.tsv'

def dir_size(start_path='.'):
  # total directory size, recursive

  total_size = 0
  for dirpath, dirnames, filenames in os.walk(start_path):
    for f in filenames:
      fp = os.path.join(dirpath,f)
      total_size += os.path.getsize(fp)
  return total_size

def nominalise(verb):
  # to be used when turning verbs to noun forms when extracting simple co-occ.
  stem = STP.stem(verb)
  # going with pure stem
  for suff in SUFFIXES:
    noun_form = wn.morphy(stem+suff,wn.NOUN)
    if noun_form != None:
      return noun_form
  # trying with the last character of the stem doubled
  stem += stem[-1]
  for suff in SUFFIXES:
    noun_form = wn.morphy(stem+suff,wn.NOUN)
    if noun_form != None:
      return noun_form
  # returning None if nothing works
  return None

def norm_np(term,sep=' '):
  # cleaning up and lemmatizing a noun phrase
  # the separator is dependent on what is used as word separators in the 
  # extracted data (usually it's either a space or an underscore)

  l = [lmtzr.lemmatize(x,'n') for x in term.split()]
  np = sep.join(l)
  np = np.lower()
  # replacing troublesome (re. the future rendering) characters
  #np = np.replace('-','_')
  for c in ['(',')','.','?','!',',',';','{','}','[',']','<','>','/','\\','+',\
  "'",'"','`','*']:
    np = np.replace(c,'')
  return np

def qiter(q):
  # iterator from a queue object
  while not q.empty():
    yield q.get()

def logMsg(logger,msg):
  logger.write(datetime.datetime.now().isoformat().replace('T','  ')+'\n'+msg)
  logger.flush()

# fuzzy set stuff

class FuzzySet:
  """
  A simplistic (but efficient) implementation of a fuzzy set class and the 
  basic operations (union, intersection, subtraction and complement) according 
  to the standard definitions.
  """

  def __init__(self,elements=[]):
    # initialising set, possibly with the (member,degree) pairs from elements

    self.members = {}
    for member, degree in elements:
      try:
        if degree > 0 and degree <= 1:
          self.members[member] = float(degree)
      except ValueError:
        sys.stderr.write('\nW @ FuzzySet(): invalid membership degree: '+\
          str(degree)+' for the member: '+str(member)+'\n')

  def __repr__(self):
    return str(self.members.items())

  def __len__(self):
    return len(self.members)

  def __iter__(self):
    # iterator over the members (keys) to simulate a dictionary

    for member in self.members.keys():
      yield member

  def __getitem__(self,member):
    # getting a membership degree of the member (zero if not present)

    if member in self.members:
      return self.members[member]
    else:
      return 0.0

  def __setitem__(self,member,degree):
    # setting a new degree for a member, possibly deleting it if the degree is
    # zero

    try:
      if degree == 0:
        if member in self.members:
          del self.members[member]
      else:
        if degree <= 1:
          self.members[member] = float(degree)
    except ValueError:
      sys.stderr.write('\nW @ FuzzySet(): invalid membership degree: '+\
        str(degree)+' for the member: '+str(member)+'\n')

  def items(self):
    # all (member,degree) tuples via an iterator

    #for member, degree in self.members.items():
    #  yield (member,degree)
    return self.members.items()

  def keys(self):
    # all members

    #for member in self.members.keys():
    #  yield member
    return self.members.keys()

  def values(self):
    # all degrees

    #for degree in self.members.values():
    #  yield degree
    return self.members.values()

  def sort(self,reverse=False,limit=0):
    # iterator over items sorted from the least to most relevant (or reverse)

    l = [(x,d) for x,d in self.members.items()]
    l.sort(key=lambda x: x[1],reverse=reverse)
    i = 0
    for member, degree in l:
      yield member, degree
      i += 1
      if limit > 0 and i >= limit:
        # if there is a limit and it was reached, stop
        break

  def update(self,elements,overwrite=True):
    # update the set values according to (member,degree) pairs in the elements,
    # overwriting possibly present elements by default

    for member, degree in elements:
      try:
        if overwrite:
          self.members[member] = float(degree)
        else:
          if not member in self.members:
            self.members[member] = float(degree)
      except ValueError:
        sys.stderr.write('\nW @ FuzzySet(): invalid membership degree: '+\
          str(degree)+' for the member: '+str(member)+'\n')

  def cut(self,alpha=1.0):
    # returns an alpha-cut crisp set with the specified alpha

    return set([member for member, degree in self.items() if degree >= alpha])

  def complement(self,universe=set()):
    # standard fuzzy set complement w.r.t. a given universe set (empty by 
    # default, limiting the universe to the members present in this set)

    result = FuzzySet()
    # present member complement values
    for member, degree in self.items():
      result[member] = 1.0-degree
    # missing members from the universe
    for member in set(universe) - set(self.members.keys()):
      result[member] = 1.0
    return result

  def __sub__(self,other):
    # standard fuzzy set subtraction

    result = FuzzySet()
    # setting the result to self first
    for member, degree in self.members.items():
      result[member] = degree
    # processing the elements from the other one
    for member in other.members.keys():
      d = min(result[member],1.0-other[member])
      if d > 0:
        result[member] = d
    return result

  def __and__(self,other):
    # standard fuzzy set intersection

    result = FuzzySet()
    # process only shared members, the others are zero by definition
    for member in set(self.members.keys()) & set(other.members.keys()):
      result[member] = min(self.__getitem__(member),other[member])
    return result

  def __or__(self,other):
    # standard fuzzy set union

    result = FuzzySet()
    # process all members
    for member in set(self.members.keys()) | set(other.members.keys()):
      result[member] = max(self.__getitem__(member),other[member])
    return result

# parallel processing stuff

class Worker(Process):
  """
  Parallel processing wrapper.
  """

  def __init__(self,identifier,w_q,processor,r_q=None,lock=None,args=()):
    # base class initialization
    Process.__init__(self)
    # job management stuff
    self.identifier = identifier
    self.work_queue = w_q
    self.result_queue = r_q
    self.processor = processor
    self.lock = lock
    if self.lock == None:
      self.lock = Lock()
    self.args = args
    self.kill_received = False
 
  def run(self):
    while not self.kill_received:
      # get a task
      job = None
      try:
        job = self.work_queue.get_nowait()
      except Empty:
        break
      # the actual processing
      result = self.processor(self.identifier,job,self.lock,self.args)
      # store the result if necessary (i.e., if not stored before during the
      # processing)
      if self.result_queue != None:
        self.result_queue.put(result)

def parex(jobs,processor,lock=None,args=(),procn=cpu_count(),\
store_results=False):
  """
  Execution of a process in parallel (using the Worker class and Python 
  multiprocessing interface).
  """

  # create a queue to pass to workers to store the results
  result_queue = None
  if store_results:
    result_queue = Queue()
  # initialising worker processes
  workers = [Worker(i,jobs,processor,result_queue,lock,args) for i in \
    range(procn)]
  # spawning the processes
  for worker in workers:
    worker.start()
  # waiting for the processes to finish
  for worker in workers:
    worker.join()
  #for i in range(procn):
  #  worker = Worker(i,jobs,processor,result_queue,lock,args)
  #  worker.start()
  # collect the results off the queue
  results = []
  if store_results:
    while not result_queue.empty(): # should be safe after everything's done
      result = result_queue.get()
      if result != None:
        # append only meaningful results
        results.append(result)
  return results

class Tensor:
  """
  A sparse, dictionary-like implementation of square (cube, hyper-cube, etc.) 
  tensors, including basic operations allowing for linear combinations 
  (implemented in parallel).
  """

  # @TODO:
  # - perhaps add loading/dumping functions for (de)serialisation
  # - add row-based indexing for matrices (or solve otherwise via interface to
  #   the dict.-based matrix implementation in strg.py
  # - write a wrapper for the evolution of tensors
  #   * related to the fitness function interface - sort that out conceptually
  #     and generally enough
  #   * general class for crossover, mutation and iterative evolution of the KB

  def __init__(self,rank):
    self.rank = rank # rank (index field lengt or dimension) of the tensor
    self.base_dict = {} # core data structure mapping index tuples to values
    self.midx = {} # auxiliary faster cross-dimensional indices (master)
    self.ridx = {} # index mapping unique row IDs to particular base_dict keys

  def __getitem__(self,key):
    # returns the value indexed by the key
    tpl = tuple(key)
    if len(tpl) == self.rank:
      if tpl in self.base_dict:
        return self.base_dict[tpl]
      else:
        return 0.0 # not present <-> zero value
    else:
      raise ValueError('Key is rank-incompatible ... key: %s, rank: %s', \
        (str(tpl),str(self.rank)))

  def __delitem__(self,key):
    # deletes the value indexed by the key (and destroys any indices)
    tpl = tuple(key)
    if self.has_key(tpl):
      del self.base_dict[tpl]
      self.midx, self.ridx = {}, {}

  def __setitem__(self,key,value):
    # sets a new value to the key index
    tpl = tuple(key)
    if len(tpl) == self.rank:
      if value != 0:
        self.base_dict[tpl] = value
      else:
        # deleting if setting a value to zero
        if tpl in self.base_dict:
          del self.base_dict[tpl]
      self.midx, self.ridx = {}, {}
    else:
      raise ValueError('Key is rank-incompatible ... key: %s, rank: %s', \
        (str(tpl),str(self.rank)))

  def __iter__(self):
    # iterates through the list of all indices
    for key in self.base_dict:
      yield key

  def __contains__(self,key):
    # checks for the presence of key among the basic indices
    tpl = tuple(key)
    return tpl in self.base_dict

  def __len__(self):
    # returns length of the tensor in terms of non-zero indices
    return len(self.base_dict)

  def density(self):
    # density of the tensor in terms of the ratio of number of non-zero 
    # elements w.r.t. the maximum possible number of elements in the current
    # tensor
    unique_indvals = set()
    for key in self.base_dict:
      unique_indvals |= set(key)
    return float(len(self.base_dict))/(len(unique_indvals)**self.rank)

  def dim_size(self,dim):
    # size of a dimension (i.e., number of unique index IDs in a dimension)
    # WARNING: can be relatively slow for large/dense tensors
    if dim >= self.rank:
      return 0 
    return len(set([x[dim] for x in self.base_dict]))

  def lex_size(self):
    # return the current lexicon size
    unique_indvals = set()
    for key in self.base_dict:
      unique_indvals |= set(key)
    return len(unique_indvals)

  def items(self):
    # return all the (key,value) tuples of the tensor
    return self.base_dict.items()

  def keys(self):
    # return all the keys of the tensor
    return self.base_dict.keys()

  def values(self):
    # return all the values of the tensor
    return self.base_dict.values()

  def has_key(self,key):
    # checks for the presence of the key among the tensor indices
    return key in self.base_dict

  def __eq__(self,other):
    if not isinstance(other,Tensor):
      raise NotImplementedError('Cannot compare tensor with a non-tensor: %s',\
        (str(type(other)),))
    if self.rank != other.rank:
      return False
    return self.base_dict == other.base_dict

  def __ne__(self,other):
    return not self.__eq__(other)

  def __add__(self,other):
    # tensor addition operator
    try:
      if self.rank != other.rank:
        raise NotImplementedError('Cannot add two tensors of different ranks')
    except AttributeError:
      raise NotImplementedError('Cannot add %s to tensor', (str(type(other)),))
    result = Tensor(rank=self.rank)
    # sequential processing
    for key in set(other.keys() + self.keys()):
      value = self.__getitem__(key) + other[key]
      if value:
        # if the addition is not zero, set it to the resulting tensor
        result[key] = value
    return result

  def __mul__(self,other):
    # scalar*tensor multiplication (aT, where a, T are the scalar and tensor
    # expressions, respectively)
    if type(other) not in [int,float]:
      raise NotImplementedError('Wrong scalar type: %',(str(type(other)),))
    if other == 0:
      return Tensor(rank=self.rank)
    result = Tensor(rank=self.rank)
    # sequential processing
    for key in self.base_dict:
      result[key] = other*self.base_dict[key]
    return result

  def __rmul__(self,other):
    # scalar*tensor multiplication (swapped operators to allow Ta being 
    # computed as aT)
    return self*other

  def __or__(self,other):
    # max-based element-wise aggregation of two tensors
    try:
      if self.rank != other.rank:
        raise NotImplementedError('Cannot aggregate tensors of different'+\
          ' ranks')
    except AttributeError:
      raise NotImplementedError('Cannot aggregate %s with a tensor', \
        (str(type(other)),))
    result = Tensor(rank=self.rank)
    # sequential processing
    for key in set(other.keys() + self.keys()):
      value = max(self.__getitem__(key),other[key])
      if value:
        # if the result is not zero, set it to the output tensor
        result[key] = value
    return result

  def __and__(self,other):
    # min-based element-wise aggregation of two tensors
    try:
      if self.rank != other.rank:
        raise NotImplementedError('Cannot aggregate tensors of different'+\
          ' ranks')
    except AttributeError:
      raise NotImplementedError('Cannot aggregate %s with a tensor', \
        (str(type(other)),))
    result = Tensor(rank=self.rank)
    # sequential processing
    for key in set(other.keys() + self.keys()):
      value = min(self.__getitem__(key),other[key])
      if value:
        # if the result is not zero, set it to the output tensor
        result[key] = value
    return result

  def __iadd__(self,other):
    return self.__add__(other)

  def __imul__(self,other):
    return self.__mul__(other)

  def __iand__(self,other):
    return self.__and__(other)

  def __ior__(self,other):
    return self.__or__(other)

  def normalise(self):
    # abs-sum normalisation of the tensor values
    result = Tensor(rank=self.rank)
    n = float(sum([math.fabs(x) for x in self.base_dict.values()]))
    for key in self.base_dict:
      result[key] = self.base_dict[key]/n
    return result

  def index(self):
    """
    @TODO - write up the documentation
    """

    # @TODO - do a bit of compl. analysis and profiling here - why so slow?

    # resetting the indices
    self.ridx, self.midx = {}, {}
    # contructing the row ID -> key index and dimension ID -> key element ->
    # set of row IDs indices in one pass through the base dictionary
    for rid, key in [(x,self.base_dict.keys()[x]) for x in \
    range(len(self.base_dict))]:
      # updating the row ID index
      self.ridx[rid] = key
      # adding the row ID to the key element value sets
      for (key_dim,key_elem) in [(x,key[x]) for x in range(len(key))]:
        if not key_dim in self.midx:
          self.midx[key_dim] = {}
        if not key_elem in self.midx[key_dim]:
          self.midx[key_dim][key_elem] = []
        self.midx[key_dim][key_elem].append(rid)
    # changing the filled end lists in the index to sets
    for key_dim in self.midx:
      for key_elem in self.midx[key_dim]:
        self.midx[key_dim][key_elem] = set(self.midx[key_dim][key_elem])

  def query_and(self,query):
    """
    @TODO - write up the documentation
    """

    if self.midx == {} and self.ridx == {}:
      raise AttributeError('Tensor index not computed, use index() first')
    # initialise the matching row IDs set
    row_ids = set(self.ridx.keys())
    # proceed through the query, intersecting the row IDs according to it
    for query_dim, query_elem in [(x,query[x]) for x in range(len(query))]:
      if query_dim not in self.midx:
        continue # weird, shouldn't happen, but have it here for sure
      if query_elem != None:
        if query_elem in self.midx[query_dim]:
          row_ids &= self.midx[query_dim][query_elem]
        else:
          # if an index element is not present, result is empty
          row_ids = set()
          break
    # generating the (key,value) tuples from the matching row IDs
    return [(self.ridx[x],self.base_dict[self.ridx[x]]) for x in row_ids]

  def query_or(self,query):
    """
    @TODO - write up the documentation
    """

    if self.midx == {} and self.ridx == {}:
      raise AttributeError('Tensor index not computed, use index() first')
    # initialise the matching row IDs set
    # @TODO - think about the correctness of the semantics in case of 
    #         all-None queries!
    row_ids = set()
    # proceed through the query, unioning the row IDs according to it
    for query_dim, query_elem in [(x,query[x]) for x in range(len(query))]:
      if query_dim not in self.midx:
        continue # weird, shouldn't happen, but have it here for sure
      if query_elem != None and query_elem in self.midx[query_dim]:
        row_ids |= self.midx[query_dim][query_elem]
    # generating the (key,value) tuples from the matching row IDs
    return [(self.ridx[x],self.base_dict[self.ridx[x]]) for x in row_ids]

  def query(self,query,qtype='AND'):
    """
    @TODO - write up the documentation
    """
    if qtype.lower() == 'and':
      return self.query_and(query)
    elif qtype.lower() == 'or':
      return self.query_or(query)
    else:
      raise NotImplementedError('Unknown query type %s, try AND or OR', \
        (qtype,))

  def matricise(self,pivot_dim):
    """
    Creates a matrix representation of the tensor, using the given dimension
    as a pivot. The result is a tensor of rank 2 with keys in the form 
    (dim_p,(dim_0,...,dim_(p-1),dim_(p+1),...,dim_rank)) and values 
    representing the corresponding original tensor values.
    """
    
    m = Tensor(rank=2)
    try:
      # iterable (multiple) pivot dimensions
      if max(pivot_dim) >= self.rank:
        raise NotImplementedError('Max. dimension of %s higher than rank %s',\
          (str(pivot_dim),str(self.rank)))
      for key, value in self.base_dict.items():
        col_ids, row_ids = [], []
        for i, key_elem in [(x,key[x]) for x in range(len(key))]:
          if i in pivot_dim:
            row_ids.append(key_elem)
          else:
            col_ids.append(key_elem)
        row_ids = tuple(row_ids)
        col_ids = tuple(col_ids)
        if len(col_ids) == 1:
          col_ids = col_ids[0]
        if len(row_ids) == 1:
          row_ids = row_ids[0]
        m[(row_ids,col_ids)] = value
    except TypeError:
      # single pivot dimension
      if pivot_dim >= self.rank:
        raise NotImplementedError('Dimension %s higher than rank %s',\
          (str(pivot_dim),str(self.rank)))
      for key, value in self.base_dict.items():
        col_id = tuple(key[:pivot_dim]+key[pivot_dim+1:])
        m[(key[pivot_dim],col_id)] = value
    return m

  def getSparseDict(self,col2row=True):
    # returns a sparse matrix in a simple dictionary representation that can be
    # directly used for retrieving whole rows (applicable only to matrices); 
    # optionally also an index mapping column IDs to set of rows that have a 
    # non-zero element in that column
    if self.rank != 2:
      raise NotImplemented('Not applicable for tensors of rank %s', \
        (self.rank,))
    dct, idx = {}, {}
    for (i,j), w in self.items():
      if not dct.has_key(i):
        dct[i] = {}
      dct[i][j] = w
      # updating the column 2 row index
      if col2row:
        if not idx.has_key(j):
          idx[j] = set()
        idx[j].add(i)
    if col2row:
      return (dct, idx)
    return dct

  def __str__(self):
    """
    Generates a string representation of the tensor in the form of a table
    mapping the keys to values.
    """

    return '\n'.join(['\t'.join([str(elem) for elem in key])+' -> '+\
      str(self.base_dict[key]) for key in self.base_dict])

  def tsv(self):
    """
    Generates a string with tab-separated values representing the tensor.
    """

    return '\n'.join(['\t'.join([str(elem) for elem in key]+\
      [str(self.base_dict[key])]) for key in self.base_dict])

  def to_file(self,filename):
    """
    Exporting a lexicon to a filename or file-like object (tab-separated 
    values).
    """

    try:
      # assuming a file object
      filename.write(self.tsv())
      filename.flush()
    except AttributeError:
      # assuming a filename
      try:
        f = open(filename,'w')
        f.write(self.tsv())
        f.close()
      except:
        # if neither file nor filename, proceed with empty lines
        sys.stderr.write('W (exporting a tensor) - cannot export to: %s\n',\
          str(filename))

  def from_file(self,filename):
    """
    Importing a tensor from a filename or a file-like object (tab-separated
    values).
    """

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
        sys.stderr.write('W (importing a tensor) - cannot import from: %s\n',\
          str(filename))
    for line in lines:
      try:
        key_val = line.split('\t')[:self.rank+1]
        key = tuple([int(x) for x in key_val[:-1]])
        val = float(key_val[-1])
        self.base_dict[key] = val
      except:
        sys.stderr.write('W (importing a tensor) - fishy line:\n%s' % (line,))

if __name__ == "__main__":
  # @TODO - add some testing stuff?
  pass
