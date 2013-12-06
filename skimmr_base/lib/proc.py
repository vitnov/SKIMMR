"""
proc.py - a processing module implementing the following functionalities:
- analyser for computing similar items within particular statement corpus 
  tensor perspectives

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

import math

class Analyser:
  """
  Basic class for matrix perspective analysis, offering the following services:
  - clustering of the input matrix rows 
  - learning of rules from the perspective and its compressed counterpart
  """

  def __init__(self,store,ptype,compute=True,mem=True,trace=False,\
  log_name='analyser.log'):
    """
    Initialising the class with an input matrix to be analysed.
    """

    self.trace = trace
    self.logger = open(log_name,'a')
    # the store to access all the underlying data
    self.store = store
    #self.max_bulk = store.max_bulk
    # the type of the perspective to be analysed by this class
    self.ptype = ptype
    # the matrix handler of the perspective, computed from scratch by default
    self.matrix = self.store.perspectives[self.ptype]
    if compute:
      self.matrix = self.store.computePerspective(self.ptype)
    self.sparse = None
    self.rmaps = None
    self.cmaps = None
    self.col2row = None
    # get an in-memory representation of the matrix
    if mem:
      if self.trace:
        print 'DEBUG - getting the sparse in-memory matrix'
      self.sparse, self.col2row = self.matrix.getSparseDict()
      # @TODO - possibly get back to the SciPy sparse if necessary
      #self.sparse, self.rmaps, self.cmaps = self.matrix.getSparse('CSR')
      #if self.trace:
      #  print 'DEBUG - finished - now computing the similar stuff'
      #  print 'DEBUG - sparse dimensions :', self.sparse.shape
      #  print 'DEBUG - size of the matrix:', len(self.matrix)

  def __del__(self):
    """
    Clean up and close stuff.
    """
  
    self.logger.close()

  def getMostSpecificTerms(self,limit=10):
    """
    Returns labels of the most specific vectors (i.e., the vectors that have 
    at least limit non-zero feature values).
    """

    specific = [x for x in self.matrix if len(self.matrix[x]) >= limit]
    return [self.store.convert((x,)) for x in self.matrix \
      if len(self.matrix[x]) >= limit]

  def similarTo(self,entity,top=100,lexicalised=False,minsim=0.001,\
  sims2src={}):
    """
    Generates a list of (similar_entity,similarity) tuples for an input entity.
    sims2src is for storage of mapping pairs of similar things (or rather 
    their IDs) to the statements that were used for computing their similarity.
    """
    
    # @NOTE - an implementation that makes use of a simple dictionary-based 
    #         sparse matrix representation and corresponding column to row 
    #         index

    entity_id = entity
    if isinstance(entity_id,str) or isinstance(entity_id,unicode):
      entity_id = self.store.convert((entity,))[0]
    if entity_id == None or not entity_id in self.sparse:
      return []
    # the row vector of the sparse matrix (a column_index:weight dictionary)
    row = self.sparse[entity_id]
    un = math.sqrt(sum([row[x]**2 for x in row]))
    # getting promising vectors for the similarity computation
    promising = set()
    for col in row:
      promising |= self.col2row[col]
    if self.trace:
      print 'DEBUG@similarTo() - entity vector size        :', len(row)
      print 'DEBUG@similarTo() - number of possibly similar:', len(promising)
    # structures for:
    # - (similarity,vector ID) tuples
    # - similar vector ID tuples mapped to source statements that were used
    #   to compute them
    #sim_vec, sims2src = [], {}
    sim_vec = []
    # going through all promising rows in the sparse matrix representation
    for v_id in promising:
      if v_id == entity_id:
        # don't process the same entity as similar
        continue
      # container for statements that lead to particular similarities
      statements_used = set()
      compared_row = self.sparse[v_id]
      # computing the actual similarity
      uv, vn = 0.0, 0.0
      for x in compared_row:
        if x in row:
          tmp = row[x]*compared_row[x]
          uv += tmp
          # updating the statements used information
          if self.ptype == 'LAxLIRA':
            statements_used.add((entity_id,x[0],x[1]))
            statements_used.add((v_id,x[0],x[1]))
          # @TODO - implement also for other types !!!
        vn += compared_row[x]**2
      vn = math.sqrt(vn)
      sim = float(uv)/(un*vn)
      if math.fabs(sim) >= minsim:
        # add only if similarity crosses the threshold (adding code 
        # translated from the sparse representation row index)
        sim_vec.append((sim,v_id))
        sims2src[(entity_id,v_id)] = statements_used
    if self.trace:
      print 'DEBUG@similarTo() - number of actually similar:', len(sim_vec)
#      print 'DEBUG@similarTo() - the results:\n', sim_vec
#      print 'DEBUG@similarTo() - the results (names converted):'
#      for s, x in sim_vec:
#        print (self.store.convert((x,)),s)
      print 'DEBUG@similarTo() - sorting and converting the results now'
    # getting the (similarity, row vector ID) tuples sorted
    sorted_tuples = sorted(sim_vec,key=lambda x: x[0])
    sorted_tuples.reverse()
    if not lexicalised:
      return [(x[1],x[0]) for x in sorted_tuples[:top]]
    else:
      return [(self.store.convert((x,))[0],s) for s,x in sorted_tuples[:top]]

if __name__ == "__main__":
  # @TODO - possibly add testing of the Analyser
  pass
