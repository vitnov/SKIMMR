"""
ifce.py - an interface module implementing the following functionalities:
- memory store index
- memory store query object
- memory store result object (includes visualisations of the results)
- simple HTTP server (exposing the querying via a web application)

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

import sys, os, whoosh, time, xml, json, gzip, pydot, BaseHTTPServer
from xml.etree.ElementTree import fromstring
from itertools import combinations
from math import log
from whoosh.qparser import QueryParser
from whoosh.index import open_dir, create_in
from whoosh.fields import Schema, TEXT, ID
from whoosh.analysis import StemmingAnalyzer
import util
from util import FuzzySet, norm_np, Tensor
from strg import Lexicon

# the key word for the universe variable
UNIVERSE_TERM = '__UNIVERSE__'

class MemStoreQueryResult:
  """
  Wrapper for the result of a MemStore index query for an in-memory storage
  of the result content (terms, statements and relevant provenances), 
  computation of various visualisations of the result and functions for 
  abbreviated pretty printing and full XML and PDF storage of the result and
  its visualisation.
  """

  def __init__(self,index,name,tuid_set,queried=set(),fname_prefix='result-',\
  vis_par={},min_w=0.5):
    # store index for generating the statements and provenances
    self.index = index
    # the TUIDs that were queried for (for filtering the relevant statements)
    self.queried = queried
    # result name and filenames for its storage
    self.name = name
    self.fname_prefix = fname_prefix
    self.term_filename = self.fname_prefix+'terms-'+self.name+'.xml'
    self.stmt_filename = self.fname_prefix+'stmts-'+self.name+'.xml'
    self.prov_filename = self.fname_prefix+'provs-'+self.name+'.xml'
    self.vis_filenames = {
      'TERMS' : self.fname_prefix+'term_vis-'+self.name+'.png',
      'STMTS' : self.fname_prefix+'stmt_vis-'+self.name+'.png',
      'PROVS' : self.fname_prefix+'prov_vis-'+self.name+'.png',
      'TERMS_MAP' : self.fname_prefix+'term_vis-'+self.name+'.map',
      'STMTS_MAP' : self.fname_prefix+'stmt_vis-'+self.name+'.map',
      'PROVS_MAP' : self.fname_prefix+'prov_vis-'+self.name+'.map',
      'TERMS_RAW' : self.fname_prefix+'term_vis-'+self.name+'.dot',
      'STMTS_RAW' : self.fname_prefix+'stmt_vis-'+self.name+'.dot',
      'PROVS_RAW' : self.fname_prefix+'prov_vis-'+self.name+'.dot'
    }
    # result content
    self.tuid_set = tuid_set   # fuzzy term ID set, basis of the result
    self.min_w = min_w
    # the result set cut according to the min_w parameter
    self.tuid_cut = FuzzySet()
    for tuid in self.tuid_set.cut(self.min_w):
      self.tuid_cut[tuid] = self.tuid_set[tuid]
    self.suid_set = FuzzySet() # fuzzy statement ID set
    self.puid_set = FuzzySet() # fuzzy provenance ID set
    self.suid_dict = {} # statement -> overall combined relevance weight
    self.tuid_dict = {} # term -> combined weight based on connected statements
    self.puid_dict = {} # provenance -> overall combined relevance weight
    self.prov_rels = {} # edges between provenances and their weights
    self.vis_dict = {   # pydot graphs for generating the visualisations
      'TERMS' : pydot.Dot('TERMS',graph_type='graph',size="1000"),
      'STMTS' : pydot.Dot('STMTS',graph_type='graph',size="1000"),
      'PROVS' : pydot.Dot('PROVS',graph_type='graph',size="1000")
    }
    self.vis_maps = {   # maps between visualisation node labels and names
      'TERMS' : [],
      'STMTS' : [],
      'PROVS' : []
    }
    # trying to get codes of relationships
    cooc_relcode, simr_relcode = -2, -1
    try:
      cooc_relcode = self.index.lexicon[util.COOC_RELNAME]
    except KeyError:
      sys.stderr.write('\nW@ifce.py - no co-occurrence relation present\n')
    try:
      simr_relcode = self.index.lexicon[util.SIMR_RELNAME]
    except KeyError:
      sys.stderr.write('\nW@ifce.py - no similarity relation present\n')
    # visualisation parameters
    self.vis_par = {
      'PROG' : 'dot',        # graph rendering program
      'NODE_SHAPE' : 'rectangle', # shape of the node
      'NODE_STYLE' : 'filled', # style of the node
      'BASE_WIDTH' : 0.25,    # base width of the node in inches
      'FIXED_SIZE' : 'false',   # nodes are fixed/variable size
      'NCOL_MAP' : {            # colors of different node types
        'PROV_ART' : '#FFCC33', # - article provenance
        'PROV_DAT' : '#FF9900', # - data provenance
        'TERM_TRM' : '#6699CC', # - term nodes in term results visualisation
        'TERM_STM' : '#6699CC'  # - term nodes in stmt results visualisation
      },
      'MAX_NLABLEN' : 50,      # maximum node label length (truncate longer)
      'MAX_ELABLEN' : 25,       # maximum edge label lentgh (truncate longer)
      'SCALE_BASE' : 10,       # log base for scaling node sizes
      'EDGE_COL' : {           # mapping of edge IDs to their colors
        cooc_relcode : 'blue', # co-occurrence relation
        simr_relcode : 'red'   # similarity relation
      }
    }
    if vis_par != {}:
      self.vis_par = vis_par

  def populate_dictionaries(self):
    # populates the statement and provenance relevance dictionaries

    #print 'DEBUG -- number of all result terms     :', len(self.tuid_set)
    #print 'DEBUG -- minimum result weight threshold:', self.min_w
    #print 'DEBUG -- number of filtered result terms:', len(self.tuid_cut)
    i = 0
    for tuid in self.tuid_cut:
      i += 1
      #print 'DEBUG -- processing result term:', self.index.lexicon[tuid]
      #print '  *', i, 'out of', len(self.tuid_cut)
      #print '  * weight:', self.tuid_cut[tuid]
      #print '  * candidate rel. statements:', len(self.index.tuid2suid[tuid])
      # the degree of membership of the term in the result
      tuid_weight = self.tuid_cut[tuid]
      rel_suid = 0
      for suid in self.index.tuid2suid[tuid]:
        # original statement
        s, p, o, suid_weight = self.index.suid2stmt[suid]
        if not (s in self.queried or o in self.queried):
          # don't process statements that are not related to the queried terms
          continue
        rel_suid += 1
        # updating the result statement dict with the combined tuid/suid weight
        if not suid in self.suid_dict:
          self.suid_dict[suid] = 0
        self.suid_dict[suid] += tuid_weight*suid_weight
        # updating the term weight based on statements connected to it
        if not tuid in self.tuid_dict:
          self.tuid_dict[tuid] = 0
        self.tuid_dict[tuid] += self.suid_dict[suid]
        # adding also the statement subject and object to the dictionary
        if s != tuid:
          if not s in self.tuid_dict:
            self.tuid_dict[s] = 0
          self.tuid_dict[s] += self.suid_dict[suid]
        if o != tuid:
          if not o in self.tuid_dict:
            self.tuid_dict[o] = 0
          self.tuid_dict[o] += self.suid_dict[suid]
        # updating the result provenance dict with the combined tuid/suid/puid
        # weight
        for puid, puid_weight in self.index.suid2prov[suid]:
          if not puid in self.puid_dict:
            self.puid_dict[puid] = 0
          self.puid_dict[puid] += tuid_weight*suid_weight*puid_weight
        # updating the self.prov_rels dictionary
        for (puid1,w1), (puid2,w2) in \
        combinations(self.index.suid2prov[suid],2):
          # aggregate value for the provenance-provenance relation
          w = tuid_weight*suid_weight*(w1+w2)/2
          if (puid2,puid1) in self.prov_rels:
            self.prov_rels[(puid2,puid1)] += w
          else:
            if not (puid1,puid2) in self.prov_rels:
              self.prov_rels[(puid1,puid2)] = 0.0
            self.prov_rels[(puid1,puid2)] += w
      #print '  * actual rel. statements:', rel_suid
    # generating the statement and provenance result fuzzy sets from the 
    # dictionaries
    self.suid_set = self._generate_fuzzy_set(self.suid_dict)
    self.puid_set = self._generate_fuzzy_set(self.puid_dict)

  def _generate_fuzzy_set(self,dct,agg=max):
    # generates a fuzzy set from a member->weight dictionary, normalising the 
    # weight values first by a constant computed by the supplied agg function
    # from the dictionary values (maximum by default)

    fset = FuzzySet()
    if len(dct) == 0:
      return fset
    norm_const = agg(dct.values())
    if norm_const <= 0:
      # making sure it's OK to divide by it meaningfully
      norm_const = 1.0
    for member, weight in dct.items():
      # cutting off the values out of [0,1] interval
      w = float(weight)/norm_const
      if w > 1:
        w = 1
      if w < 0:
        w = 0
      fset[member] = w
    return fset

  def generate_visualisations(self,max_n=50,max_e=250):
    # generate visualisations from the populated results

    self._gen_term_vis(self.vis_dict['TERMS'],max_n,max_e)
    self._gen_stmt_vis(self.vis_dict['STMTS'],max_n,max_e)
    self._gen_prov_vis(self.vis_dict['PROVS'],max_n,max_e)

  def _gen_term_vis(self,graph,max_nodes,max_edges,lex_labels=True):
    # generate the term-based visualisation of the results (up to max_nodes 
    # most relevant nodes in the graph)
    # if lex_labels is True, the true lexical labels are used for the nodes,
    # otherwise numbers are being used

    # getting the relevant term IDs
    tuids = [x[0] for x in self.tuid_cut.sort(reverse=True,limit=max_nodes)]
    # getting the scale factors for the size of each term node
    tuid2scale = dict([(x,self.tuid_dict[x]) for x in tuids])
    norm_const = 1.0
    if len(tuid2scale):
      norm_const = min(tuid2scale.values())
    for tuid in tuid2scale:
      tuid2scale[tuid] /= norm_const
    # constructing the graph nodes
    nodes, i = {}, 0
    for tuid in tuids:
      # setting the node label -> node name mapping
      node_name = self.index.lexicon[tuid]
      node_label = str(i)
      self.vis_maps['TERMS'].append((node_label,node_name))
      # setting the node size and deriving the fontsize from it
      node_width = log(self.vis_par['BASE_WIDTH']*tuid2scale[tuid],\
        self.vis_par['SCALE_BASE'])
      # enforcing a minimal size of the scaled nodes
      if node_width < 0.4:
        node_width = 0.4
      font_size = int(24*node_width) # 1/3 of the node, 72 points per inch
      # setting the node colour
      node_col = self.vis_par['NCOL_MAP']['TERM_TRM']
      # setting the node label either to the lexical name or to an ID
      label = node_label
      if lex_labels:
        label = node_name[:self.vis_par['MAX_NLABLEN']]
      # creating the node
      nodes[tuid] = pydot.Node(\
        label,\
        style=self.vis_par['NODE_STYLE'],\
        fillcolor=node_col,\
        shape=self.vis_par['NODE_SHAPE'],\
        width=str(node_width),\
        fontsize=str(font_size),\
        fixedsize=self.vis_par['FIXED_SIZE']\
      )
      i += 1
    # adding the graph nodes to the TERMS visualisation
    for node in nodes.values():
      self.vis_dict['TERMS'].add_node(node)
    # constructing the edges (limited by max_edges)
    l, num_edges = self.suid_set.items() , 0
    l.sort(key=lambda x: x[1],reverse=True)
    for suid, w in l:
      if num_edges > max_edges:
        break
      s, p, o, corp_w = self.index.suid2stmt[suid]
      # adding the edge if both arguments are present in the node set
      if s in nodes and o in nodes:
        # edge label, if different from related_to, observed_with (those are
        # distinguished by the red and blue colours, respectively)
        edge_label = ''
        if p not in self.vis_par['EDGE_COL']:
          # non-default edge type, add a specific label
          edge_label = self.index.lexicon[p]
        edge_label = edge_label[:self.vis_par['MAX_ELABLEN']]
        #edge = pydot.Edge(nodes[s],nodes[o],label=edge_label)
        edge_col = 'black' # default colour
        if p in self.vis_par['EDGE_COL']:
          edge_col = self.vis_par['EDGE_COL'][p]
        edge_wgh = str(int(w*10000))
        edge = None
        if len(edge_label):
          edge = pydot.Edge(nodes[s],nodes[o],color=edge_col,weight=edge_wgh,\
            label=edge_label)
        else:
          edge = pydot.Edge(nodes[s],nodes[o],color=edge_col,weight=edge_wgh)
        self.vis_dict['TERMS'].add_edge(edge)
        num_edges += 1
    #print 'DEBUG -- number of TERMS graph nodes:', \
    #  len(self.vis_dict['TERMS'].get_node_list())
    #print 'DEBUG -- number of TERMS graph edges:', \
    #  len(self.vis_dict['TERMS'].get_edge_list())

  def _gen_stmt_vis(self,graph,max_nodes,max_edges,lex_labels=True):
    # generate the more statement oriented visualisation of the results (up to 
    # max_nodes most relevant statement nodes in the graph), similarly to the
    # term visualisation, however, this time including all terms from the 
    # result statements, not only the terms in the term-result set
    # if lex_labels is True, the true lexical labels are used for the nodes,
    # otherwise numbers are being used

    # getting the relevant term IDs
    l = self.tuid_dict.items()
    l.sort(key=lambda x: x[1],reverse=True)
    tuids = [x[0] for x in l[:max_nodes]]
    # getting the scale factors for the size of each term node
    tuid2scale = dict([(x,self.tuid_dict[x]) for x in tuids])
    norm_const = 1.0
    if len(tuid2scale):
      norm_const = min(tuid2scale.values())
    for tuid in tuid2scale:
      tuid2scale[tuid] /= norm_const
    # constructing the graph nodes
    nodes, i = {}, 0
    for tuid in tuids:
      # setting the node label -> node name mapping
      node_name = self.index.lexicon[tuid]
      node_label = str(i)
      self.vis_maps['STMTS'].append((node_label,node_name))
      # setting the node size and deriving the fontsize from it
      node_width = log(self.vis_par['BASE_WIDTH']*tuid2scale[tuid],\
        self.vis_par['SCALE_BASE'])
      # enforcing a minimal size of the scaled nodes
      if node_width < 0.4:
        node_width = 0.4
      font_size = int(24*node_width) # 1/3 of the node, 72 points per inch
      # setting the node colour
      node_col = self.vis_par['NCOL_MAP']['TERM_STM']
      # setting the node label either to the lexical name or to an ID
      label = node_label
      if lex_labels:
        label = node_name[:self.vis_par['MAX_NLABLEN']]
      # creating the node
      nodes[tuid] = pydot.Node(\
        label,\
        style=self.vis_par['NODE_STYLE'],\
        fillcolor=node_col,\
        shape=self.vis_par['NODE_SHAPE'],\
        width=str(node_width),\
        fontsize=str(font_size),\
        fixedsize=self.vis_par['FIXED_SIZE']\
      )
      i += 1
    # adding the graph nodes to the TERMS visualisation
    for node in nodes.values():
      self.vis_dict['STMTS'].add_node(node)
    # constructing the edges (limited by max_edges)
    l, num_edges = self.suid_set.items(), 0
    l.sort(key=lambda x: x[1],reverse=True)
    for suid, w in l:
      if num_edges > max_edges:
        break
      s, p, o, corp_w = self.index.suid2stmt[suid]
      # adding the edge if both arguments are present in the node set
      if s in nodes and o in nodes:
        # edge label, if different from related_to, observed_with (those are
        # distinguished by the red and blue colours, respectively)
        edge_label = ''
        if p not in self.vis_par['EDGE_COL']:
          # non-default edge type, add a specific label
          edge_label = self.index.lexicon[p]
        edge_label = edge_label[:self.vis_par['MAX_ELABLEN']]
        #edge = pydot.Edge(nodes[s],nodes[o],label=edge_label)
        edge_col = 'black' # default colour
        if p in self.vis_par['EDGE_COL']:
          edge_col = self.vis_par['EDGE_COL'][p]
        edge_wgh = str(int(w*10000))
        edge = None
        if len(edge_label):
          edge = pydot.Edge(nodes[s],nodes[o],color=edge_col,weight=edge_wgh,\
            label=edge_label)
        else:
          edge = pydot.Edge(nodes[s],nodes[o],color=edge_col,weight=edge_wgh)
        self.vis_dict['STMTS'].add_edge(edge)
        num_edges += 1
    #print 'DEBUG -- number of STMTS graph nodes:', \
    #  len(self.vis_dict['STMTS'].get_node_list())
    #print 'DEBUG -- number of TERMS graph edges:', \
    #  len(self.vis_dict['STMTS'].get_edge_list())

  def _gen_prov_vis(self,graph,max_nodes,max_edges,lex_labels=True):
    # generate the provenance-based visualisation of the results (up to 
    # max_nodes most relevant nodes in the graph)
    # if lex_labels is True, the true lexical labels are used for the nodes,
    # otherwise numbers are being used

    # getting the relevant provenance IDs
    puids = [x[0] for x in self.puid_set.sort(reverse=True,limit=max_nodes)]
    # getting the scale factors for the size of each term node
    puid2scale = dict([(x,self.puid_dict[x]) for x in puids])
    norm_const = 1.0
    if len(puid2scale):
      norm_const = min(puid2scale.values())
    for puid in puid2scale:
      puid2scale[puid] /= norm_const
    # constructing the graph nodes
    nodes, i = {}, 0
    for puid in puids:
      # setting the node label -> node name mapping
      node_name = self.index.lexicon[puid]
      node_label = str(i)
      node_title = ''
      if puid in self.index.puid2meta and \
      self.index.puid2meta[puid].has_key('TITLE'):
        node_title = self.index.puid2meta[puid]['TITLE']
      self.vis_maps['PROVS'].append((node_label,node_name,node_title))
      # setting the node size and deriving the fontsize from it
      node_width = log(self.vis_par['BASE_WIDTH']*puid2scale[puid],\
        self.vis_par['SCALE_BASE'])
      # enforcing a minimal size of the scaled nodes
      if node_width < 0.4:
        node_width = 0.4
      font_size = int(24*node_width) # 1/3 of the node, 72 points per inch
      # setting the node colour - distinguish between data and article 
      # provenance by the fact that article provenance label is numeric,
      # while the data provenance is not
      node_col = self.vis_par['NCOL_MAP']['PROV_ART']
      if not node_name.split('_')[0].isdigit():
        node_col = self.vis_par['NCOL_MAP']['PROV_DAT']
      #print 'DEBUG -- provenance node name:', node_name
      #print 'DEBUG -- setting the provenance node colour to:', node_col
      # setting the node label either to the lexical name or to an ID
      label = node_label
      if lex_labels:
        label = node_name[:self.vis_par['MAX_NLABLEN']]
      # creating the node
      nodes[puid] = pydot.Node(\
        label,\
        style=self.vis_par['NODE_STYLE'],\
        fillcolor=node_col,\
        shape=self.vis_par['NODE_SHAPE'],\
        width=str(node_width),\
        fontsize=str(font_size),\
        fixedsize=self.vis_par['FIXED_SIZE']\
      )
      i += 1
    # adding the graph nodes to the PROVS visualisation
    for node in nodes.values():
      self.vis_dict['PROVS'].add_node(node)
    # computing normalised edge weights
    prov_rels_set = self._generate_fuzzy_set(self.prov_rels)
    # constructing the edges (limited by max_edge)
    l, num_edges = prov_rels_set.items(), 0
    l.sort(key=lambda x: x[1],reverse=True)
    for (puid1, puid2), w in l:
      if num_edges > max_edges:
        break
      if puid1 in nodes and puid2 in nodes:
        # adding the edge if both provenances are relevant nodes
        # label too messy, but may be added again
        #edge_label = \
        #  str(prov_rels_set[(puid1,puid2)])[:self.vis_par['MAX_ELABLEN']]
        #edge = pydot.Edge(nodes[puid1],nodes[puid2],label=edge_label)
        edge_wgh = str(int(w*10000))
        edge = pydot.Edge(nodes[puid1],nodes[puid2],weight=edge_wgh)
        self.vis_dict['PROVS'].add_edge(edge)
        num_edges += 1
    #print 'DEBUG -- number of PROVS graph nodes:', \
    #  len(self.vis_dict['PROVS'].get_node_list())
    #print 'DEBUG -- number of PROVS graph edges:', \
    #  len(self.vis_dict['PROVS'].get_edge_list())

  def node_info(self,graph_name,node_label):
    # provides additional information on a node in the visualisation graph

    if graph_name in ['TERMS', 'STMTS']:
      # checking for a term name corresponding to the queried label
      for label, name in self.vis_maps[graph_name]:
        if label == node_label:
          return 'NODE METADATA:\n'+'  * NAME: '+name
    elif graph_name == 'PROVS':
      # checking for a node with the requested label
      for label, name, title in self.vis_maps[graph_name]:
        if label == node_label:
          # generating a string from the provenance meta-data record
          puid = self.index.lexicon[name]
          #print 'DEBUG -- PUID:', puid
          #print 'DEBUG -- name:', name
          prov_meta = {}
          if puid in self.index.puid2meta:
            prov_meta = self.index.puid2meta[puid]
          #print 'DEBUG -- meta:', prov_meta
          lines = ['NODE METADATA:']
          for key, value in prov_meta.items():
            lines.append('  * '+str(key)+': '+str(value.encode('utf-8'))) #,\
            #  errors='replace')))
          return '\n'.join(lines)
    return 'Node '+node_label+' not found in graph '+graph_name+'...'

  def store(self,path):
    # storing all the result files to the specified path

    self._dump_term_xml(path)
    self._dump_stmt_xml(path)
    self._dump_prov_xml(path)
    # dumping the image graphs
    vis_path = os.path.join(path,self.vis_filenames['TERMS'])
    self.vis_dict['TERMS'].write_png(vis_path,prog=self.vis_par['PROG'])
    vis_path = os.path.join(path,self.vis_filenames['STMTS'])
    self.vis_dict['STMTS'].write_png(vis_path,prog=self.vis_par['PROG'])
    vis_path = os.path.join(path,self.vis_filenames['PROVS'])
    self.vis_dict['PROVS'].write_png(vis_path,prog=self.vis_par['PROG'])
    # dumping the raw graphs
    vis_path = os.path.join(path,self.vis_filenames['TERMS_RAW'])
    self.vis_dict['TERMS'].write(vis_path)
    vis_path = os.path.join(path,self.vis_filenames['STMTS_RAW'])
    self.vis_dict['STMTS'].write(vis_path)
    vis_path = os.path.join(path,self.vis_filenames['PROVS_RAW'])
    self.vis_dict['PROVS'].write(vis_path)
    # dumping the graph node name maps
    f = open(os.path.join(path,self.vis_filenames['TERMS_MAP']),'w')
    f.write('\n'.join(['\t'.join(x) for x in self.vis_maps['TERMS']]))
    f.close()
    f = open(os.path.join(path,self.vis_filenames['STMTS_MAP']),'w')
    f.write('\n'.join(['\t'.join(x) for x in self.vis_maps['STMTS']]))
    f.close()
    f = open(os.path.join(path,self.vis_filenames['PROVS_MAP']),'w')
    f.write('\n'.join(['\t'.join(x) for x in self.vis_maps['PROVS']]))
    f.close()

  def _xml_elem(self,tag,attrib={},text='',depth=0,children=[],indent_unit=2):
    # generates an XML element representation from the given tag, attrib 
    # dictionary and text, with depth controlling the indentation

    hf_space = ' '*(depth*indent_unit)    # header/footer space indent
    bd_space = ' '*((depth+1)*indent_unit) # body space indent
    attrib_strlist = []
    for key, value in attrib.items():
      attrib_strlist.append(str(key)+'="'+str(value)+'"')
    lines = [hf_space+'<'+tag+' '+' '.join(attrib_strlist)+'>']
    if len(text):
      lines += [bd_space+x for x in text.split('\n')]
    lines += children
    lines.append(hf_space+'</'+tag+'>')
    safe_lines = []
    for line in lines:
      try:
        safe_lines.append(line.encode('utf-8')) #,errors='replace'))
      except UnicodeDecodeError:
        sys.stderr.write('\nW @ _xml_elem(): problematic line, writing as:\n')
        sys.stderr.write(`line`+'\n')
        safe_lines.append(`line`)
    return '\n'.join(safe_lines)

  def _dump_term_xml(self,path):
    # dumps the term XML

    f = open(os.path.join(path,self.term_filename),'w')
    f.write('<?xml version="1.0" ?>\n<xml>\n')
    i = 0
    elements = []
    for tuid, w in self.tuid_cut.sort(reverse=True):
      i += 1
      # generating the child term element
      term_elem = self._xml_elem('term',text=self.index.lexicon[tuid],depth=2)
      # the result element attributes
      attrib = {'rank':i, 'weight':w}
      res_elem = self._xml_elem('result',attrib=attrib,children=[term_elem],\
        depth=1)
      elements.append(res_elem)
    f.write('\n'.join(elements))
    f.write('\n</xml>')
    f.close()

  def _dump_stmt_xml(self,path):
    # dumps the term XML

    f = open(os.path.join(path,self.stmt_filename),'w')
    f.write('<?xml version="1.0" ?>\n<xml>\n')
    i = 0
    elements = []
    for suid, w in self.suid_set.sort(reverse=True):
      i += 1
      # generating the child elements
      s, p, o, corp_w = self.index.suid2stmt[suid]
      arg1_elem = self._xml_elem('argument',attrib={'pos':'L'},\
        text=self.index.lexicon[s],depth=2)
      rel_elem = self._xml_elem('relation',text=self.index.lexicon[p],depth=2)
      arg2_elem = self._xml_elem('argument',attrib={'pos':'R'},\
        text=self.index.lexicon[o],depth=2)
      # the result element attributes
      attrib = {'rank':i, 'weight':w}
      children = [arg1_elem,rel_elem,arg2_elem]
      res_elem = self._xml_elem('result',attrib=attrib,children=children,\
        depth=1)
      elements.append(res_elem)
    f.write('\n'.join(elements))
    f.write('\n</xml>')
    f.close()

  def _dump_prov_xml(self,path):
    # dumps the term XML

    f = open(os.path.join(path,self.prov_filename),'w')
    f.write('<?xml version="1.0" ?>\n<xml>\n')
    i = 0
    elements = []
    for puid, w in self.puid_set.sort(reverse=True):
      i += 1
      prov_meta = {}
      if puid in self.index.puid2meta:
        prov_meta = self.index.puid2meta[puid]
      # generating the children elements
      children = []
      for key, value in prov_meta.items():
        children.append(self._xml_elem(key,text=value,depth=2))
      # the result element attributes
      attrib = {'rank':i, 'weight':w}
      res_elem = self._xml_elem('result',attrib=attrib,children=children,\
        depth=1)
      elements.append(res_elem)
    f.write('\n'.join(elements))
    f.write('\n</xml>')
    f.close()

  def pretty_print(self,limit=10):
    # prepare a pretty print string with an abbreviated version of the result
    # (up to limit items from term, statement and provenance sets)

    lines = [str(limit)+' MOST RELEVANT TERMS:']
    i = 0
    for tuid, w in self.tuid_cut.sort(reverse=True,limit=limit):
      i += 1
      lines.append('RANK '+str(i)+'.')
      lines.append('  * term  : '+self.index.lexicon[tuid])
      lines.append('  * weight: '+str(w))
    lines += [str(limit)+' MOST RELEVANT STATEMENTS:']
    i = 0
    for suid, w in self.suid_set.sort(reverse=True,limit=limit):
      i += 1
      s, p, o, corp_w = self.index.suid2stmt[suid]
      lines.append('RANK '+str(i)+'.')
      lines.append('  * argument: '+self.index.lexicon[s])
      lines.append('  * relation: '+self.index.lexicon[p])
      lines.append('  * argument: '+self.index.lexicon[o])
      lines.append('  * weight  : '+str(w))
    lines += [str(limit)+' MOST RELEVANT PROVENANCE SOURCES:']
    i = 0
    for puid, w in self.puid_set.sort(reverse=True,limit=limit):
      i += 1
      prov_meta = {}
      if puid in self.index.puid2meta:
        prov_meta = self.index.puid2meta[puid]
      lines.append('RANK '+str(i)+'.')
      for key, value in prov_meta.items():
        lines.append('  * '+str(key)+': '+str(value.encode('utf-8'))) #,\
        #  errors='replace')))
      lines.append('  * weight  : '+str(w))
    return '\n'.join(lines)

class MemStoreQuery:
  """
  Wrapper for a MemStore query object parsed from an XML string or file passed
  to the contructor.
  """

  def __init__(self,xml_input,fname,log_file=None):
    # getting a name from the filename for external identification and result
    # storage later on
    self.name = os.path.splitext(os.path.split(fname)[-1])[0]
    # the string with the XML query representation
    xml_string = ''
    self.log_file = log_file
    if isinstance(xml_input,file):
      # interpreting as a file and reading the string from it
      xml_input.seek(0)
      xml_string = xml_input.read()
    elif isinstance(xml_input,str) or isinstance(xml_input,unicode):
      if os.path.isfile(xml_input):
        # interpreting the argument as a filename
        xml_string = open(xml_input,'r').read()
      else:
        # interpreting the argument as the actual XML
        xml_string = xml_input
    # attempting to parse the XML
    root = None
    try:
      root = fromstring(xml_string)
    except xml.etree.ElementTree.ParseError:
      if self.log_file:
        self.log_file.write('\nW @ MemStoreQuery(): invalid XML string\n')
      else:
        sys.stderr.write('\nW @ MemStoreQuery(): invalid XML string\n')
    # class variable for global management and assignment of the IDs to the
    # parsed nodes in the XML graph
    self._parse_node_id = 0
    # generating the query actions from the root if successfully parsed
    self.query_actions = []
    if root != None:
      self.query_actions = self._process_root(root)

  def _process_root(self,root):
    # generating a sequence of actions to be executed in order to evaluate the
    # query

    # getting all paths leading from the top qnode element to the leafs
    paths = []
    self._parse_node_id = 0
    for node in root: 
      if node.tag == 'qnode':
        # processing the root qnode
        self._get_paths(node,0,[(node,0,0)],paths)
      elif node.tag == 'qterm':
        # setting the path to a single term(inal) node in the root position
        paths = [[(node,0,0)]]
        # make sure that only one terminal node is processed if it is a root
        break
    # processing the XML nodes in the paths, generating the action items
    action_items, path_no = {}, 0
    for path in paths:
      # update the position from which the dependencies should be generated
      i = 0
      # process the nodes in each path, updating the action_items dictionary
      # entries and the dependency sets in each entry
      path_no += 1
      for node, node_id, depth in path:
        i += 1
        if node_id not in action_items:
          # generating the action item representation from the node
          action_items[node_id] = self._gen_action_item(node,node_id)
        # updating the dependencies
        action_items[node_id]['DEP'] |= set([x[1] for x in path[i:]])
    # sorting the generated action items according to their dependencies
    # adding leafs first
    sorted_action_items = [x for x in action_items.values() \
      if len(x['DEP']) == 0]
    # marking leaf NIDs as processed
    processed = set([x['NID'] for x in sorted_action_items])
    # processing the rest of the items iteratively
    while len(processed) < len(action_items):
      for node_id in action_items:
        # adding new stuff if not processed before, but has already met deps
        if not node_id in processed and \
        action_items[node_id]['DEP'] <= processed:
          sorted_action_items.append(action_items[node_id])
          processed.add(node_id)
    return sorted_action_items

  def _get_paths(self,qnode,depth,pred,paths):
    # process an XML query tree element, getting all paths leading from it to 
    # the leafs recursively

    # update the current depth
    current_depth = depth + 1
    for child in qnode: #.findall('qnode'):
      # update the node_id
      self._parse_node_id += 1
      # update the predecessors
      current_pred = pred+[(child,self._parse_node_id,current_depth)]
      if child.tag == 'qnode':
        # proceed deeper
        self._get_paths(child,current_depth,current_pred,paths)
      if child.tag == 'qterm':
        # add the leaf to the path as the final element
        paths.append(current_pred)

  def _gen_action_item(self,node,node_id):
    # convert a node into an action item dictionary record
 
    # initialising the dictionary
    action_item = {
      'NID' : node_id,
      'EXP' : [],
      'RES' : [],
      'ACT' : node.get('operation',default='EVALUATION'),
      'DEP' : set()
    }
    # updating the dictionary with expressions and restrictions, if present
    for expression in node.findall('expression'):
      text = expression.text
      text = text.strip()
      text = text.encode('utf-8') #,errors='replace')
      action_item['EXP'].append(text)
    for restriction in node.findall('restriction'):
      text = restriction.text
      text = text.strip()
      text = text.encode('utf-8') #,errors='replace')
      action_item['RES'].append(text)
    return action_item

  def pretty_print(self):
    # generating a string for pretty printing of the query representation

    term_lines = ['TERMINAL QUERY NODES FOR DIRECT EVALUATION:']
    node_lines = ['THE OVERALL QUERY STRUCTURE:']
    comb_nodes, added_already = {}, set()
    for action_item in self.query_actions:
      if action_item['ACT'] == 'EVALUATION':
        # terminal action item for direct evaluation
        term_lines.append('  * NODE ID     : ' + str(action_item['NID']))
        term_lines.append('    - QUERY TERMS : ' + ' AND '.join(['"'+x+'"' \
          for x in action_item['EXP']]))
        term_lines.append('    - RESTRICTIONS: ' + ' AND '.join(['"'+x+'"' \
          for x in action_item['RES']]))
        # for later generation of the query structure
        comb_nodes[action_item['NID']] = str(action_item['NID'])
      else:
        # set combination node for further line generation
        direct_deps = \
          (set(comb_nodes.keys()) & action_item['DEP']) - added_already
        added_already |= direct_deps
        l = ['('+comb_nodes[x]+')' for x in direct_deps]
        comb_nodes[action_item['NID']] = action_item['ACT']+' '+' '.join(l)
    # getting the string representation of the top query_action item
    top_id = self.query_actions[-1]['NID']
    node_lines.append(comb_nodes[top_id])
    return '\n'.join(['QUERY NAME: '+self.name]+term_lines+node_lines)

class MemStoreIndex:
  """
  Wrapper for indexing and querying of the MemStore pre-computed instances.
  """

  def __init__(self,store_path,trace=False,log_filename=None):
    start_all = time.time()
    # setting up the trace and logging related stuff
    self.trace = trace
    self.log_filename = log_filename
    self.log = sys.stderr
    if self.trace or self.log_filename:
      try:
        self.log = open(self.log_filename,'a')
      except: #IOError, TypeError:
        sys.stderr.write('\nW @ MemStoreIndex(): problem with the log file: '+\
          str(self.log_filename)+', using stderr for logging!\n')
    # starting loading the idex
    print '*** Initialising the store index from:', store_path
    # the path to the store itself
    self.store_path = store_path
    # the path to the store index files
    self.path = os.path.join(store_path,'index')
    self.db = None
    self.cursor = None
    # the index database and cursor for accessing it
    #print '- loading the database'
    #start = time.time()
    #self.db, self.cursor = self._load_db()
    #print '  ... loaded in', time.time() - start, 'seconds'
    # the provenance metadata dictionary
    print '- loading the provenance metadata'
    start = time.time()
    self.puid2meta = self._load_puid2meta()
    print '  ... loaded in', time.time() - start, 'seconds'
    # the store lexicon
    print '- loading the lexicon'
    start = time.time()
    self.lexicon = self._load_lexicon()
    print '  ... loaded in', time.time() - start, 'seconds'
    # generating the universe set
    print '- generating the lexical universe set'
    start = time.time()
    self.universe = FuzzySet([(x,1.0) for x in self.lexicon.int2lex.keys()])
    print '  ... generated in', time.time() - start, 'seconds'
    # the dictionary mapping types to their instances
    print '- loading the types/instances index'
    start = time.time()
    self.types2instances = self._load_types2instances()
    print '  ... loaded in', time.time() - start, 'seconds'
    # the fulltext index objects for getting query term IDs
    print '- loading the full-text index'
    start = time.time()
    self.full_text, self.qterm_parser = self._load_fulltext()
    print '  ... loaded in', time.time() - start, 'seconds'
    # loading the provenances dictionary into memory if a serialisation exist
    print '- loading the statement provenance index'
    start = time.time()
    self.suid2prov = self._load_suid2prov()
    print '  ... loaded in', time.time() - start, 'seconds'
    # loading the suids dictionary into memory if a serialisation exist
    print '- loading the statement ID and term -> statement ID indices'
    start = time.time()
    self.suid2stmt, self.tuid2suid = self._load_suid2stmt()
    print '  ... loaded in', time.time() - start, 'seconds'
    # loading the termsets dictionary into memory if a serialisation exist
    print '- loading the term sets index'
    start = time.time()
    self.tuid2relt = self._load_tuid2relt()
    print '  ... loaded in', time.time() - start, 'seconds'
    print '*** Store index loaded in', time.time() - start_all, 'seconds'
    # @TODO - figure out how to determine the real size more precisely
    #self_size = sys.getsizeof(self,-1)
    #if self_size > 0:
    #  print '*** Size of the index in memory (in bytes):', self_size
    #else:
    #  print '*** Size of the index in memory (in bytes): N/A'

  def __del__(self):
    # closing log file if not stderr
    if self.log != sys.stderr:
      self.log.close()

  # the auxiliary loading functions

  def _load_db(self):
    db = None
    cursor = None
    if os.path.exists(os.path.join(self.path,'index.db')):
      db = sqlite3.connect(os.path.join(self.path,'index.db'))
      cursor = db.cursor()
    else:
      sys.stderr.write('\nW @ MemStoreIndex() - DB cannot be loaded!\n')
    return db, cursor

  def _load_puid2meta(self):
    puid2meta = {}
    if os.path.exists(os.path.join(self.path,'metadata.json')):
      f = open(os.path.join(self.path,'metadata.json'),'r')
      dct = json.load(f)
      f.close()
      # making sure the keys are integer
      for key, value in dct.items():
        puid2meta[int(key)] = value
    else:
      sys.stderr.write('\nW @ MemStoreIndex() - puid2meta cannot be loaded!\n')
    return puid2meta

  def _load_lexicon(self):
    lexicon = Lexicon()
    if os.path.exists(os.path.join(self.store_path,'lexicon.tsv.gz')):
      lex_fn = os.path.join(self.store_path,'lexicon.tsv.gz')
      lex_f = gzip.open(lex_fn,'rb')
      lexicon.from_file(lex_f)
      lex_f.close()
    else:
      sys.stderr.write('\nW @ MemStoreIndex() - lexicon cannot be loaded!\n')
    return lexicon

  def _load_types2instances(self):
    types2instances = {}
    if os.path.exists(os.path.join(self.path,'types2instances.json')):
      f = open(os.path.join(self.path,'types2instances.json'),'r')
      dct = json.load(f)
      f.close()
      # making sure that all is integer and making sets out of the values
      for key, value in dct.items():
        types2instances[int(key)] = set([int(x) for x in value])
    else:
      sys.stderr.write('\nW @ MemStoreIndex() - type2inst cannot be loaded!\n')
    return types2instances

  def _load_fulltext(self):
    full_text, qterm_parser = None, None
    if os.path.exists(os.path.join(self.path,'fulltext')):
      full_text = open_dir(os.path.join(self.path,'fulltext'))
      qterm_parser = QueryParser('content',full_text.schema)
    else:
      sys.stderr.write('\nW @ MemStoreIndex() - fulltext cannot be loaded!\n')
    return full_text, qterm_parser

  def _load_suid2prov(self):
    suid2prov = {}
    if os.path.exists(os.path.join(self.path,'provenances.tsv.gz')):
      fn = os.path.join(self.path,'provenances.tsv.gz')
      f = gzip.open(fn,'rb')
      for line in f.read().split('\n'):
        spl = line.split('\t')
        if len(spl) != 3:
          continue
        key, value = int(spl[0]), (int(spl[1]),float(spl[2]))
        if not key in suid2prov:
          suid2prov[key] = []
        suid2prov[key].append(value)
      f.close()
    else:
      sys.stderr.write('\nW @ MemStoreIndex() - prov. cannot be loaded!\n')
    return suid2prov

  def _load_suid2stmt(self):
    suid2stmt, tuid2suid = {}, {}
    if os.path.exists(os.path.join(self.path,'suids.tsv.gz')):
      fn = os.path.join(self.path,'suids.tsv.gz')
      f = gzip.open(fn,'rb')
      for line in f.read().split('\n'):
        spl = line.split('\t')
        if len(spl) != 5:
          continue
        # updating the statement ID -> statement mapping
        suid = int(spl[0])
        statement = (int(spl[1]),int(spl[2]),int(spl[3]),float(spl[4]))
        suid2stmt[suid] = statement
        # updating the term ID -> statement ID mapping record for the subject
        if not statement[0] in tuid2suid:
          tuid2suid[statement[0]] = []
        tuid2suid[statement[0]].append(suid)
        # updating the term ID -> statement ID mapping record for the object
        if not statement[2] in tuid2suid:
          tuid2suid[statement[2]] = []
        tuid2suid[statement[2]].append(suid)
      f.close()
    else:
      sys.stderr.write('\nW @ MemStoreIndex() - suids cannot be loaded!\n')
    return suid2stmt, tuid2suid

  def _load_tuid2relt(self):
    tuid2relt = {}
    if os.path.exists(os.path.join(self.path,'termsets.tsv.gz')):
      fn = os.path.join(self.path,'termsets.tsv.gz')
      f = gzip.open(fn,'rb')
      for line in f.read().split('\n'):
        spl = line.split('\t')
        if len(spl) != 3:
          continue
        key, value = int(spl[0]), (int(spl[1]),float(spl[2]))
        if not key in tuid2relt:
          tuid2relt[key] = []
        tuid2relt[key].append(value)
      f.close()
    else:
      sys.stderr.write('\nW @ MemStoreIndex() - suids cannot be loaded!\n')
    return tuid2relt

  # the actual index and interface functions

  def _qterm_indices(self,qterm,expand_types=True):
    # attempt to retrieve indices corresponding a single query term via a 
    # fulltext index on all the stored terms (normalising the query term)
    ft_query, identifiers = self.qterm_parser.parse(unicode(norm_np(qterm))),[]
    try:
      searcher = self.full_text.searcher()
      identifiers = [int(x['identifier']) for x in searcher.search(ft_query)]
    finally:
      searcher.close()
    if not expand_types:
      # returning raw set of non-expanded identifiers
      return identifiers
    # expanding the identifiers - any type ID contributes with its instances
    id_set = set(identifiers)
    for identifier in identifiers:
      if identifier in self.types2instances:
        id_set |= self.types2instances[identifier]
    return list(id_set)

  def parse_xml_query(self,xml,fname):
    # parses the query in XML (string or file) and returns a query object 

    return MemStoreQuery(xml,fname)

  def evaluate(self,query,min_w=0.5,max_n=50,max_e=250):
    # evaluates a query object and returns a result object; the number of terms
    # being taken into account in the result is limited by the minimal weight
    # parameter - only those with weight higher than or equal to it are 
    # processed

    # dictionary mapping query node IDs to the particular result sets, 
    # store for direct dependencies added and a set of the TUIDs idenfitied
    # in the query (for filtering out the statements later on)
    res_dict, added_already, queried = {}, set(), set()
    for action_item in query.query_actions:
      if action_item['ACT'] == 'EVALUATION':
        # evaluating terminal action item
        res_set = FuzzySet()
        # updating the fuzzy result set by all expression hits
        for term in action_item['EXP']:
          # set to universe if a special term encountered
          if term.upper() == UNIVERSE_TERM.upper():
            res_set = self.universe
            break
          for idx in self._qterm_indices(term):
            queried.add(idx)
            try:
              res_set = res_set | FuzzySet([x for x in self.tuid2relt[idx]])
            except KeyError:
              sys.stderr.write('\nW @ MemStoreIndex.evaluate(): term '+\
              'missing in the base relation index: '+self.lexicon[idx]+'\n')
        # restricting the fuzzy result set by all restriction hits
        for term in action_item['RES']:
          # set to universe if a special term encountered
          if term.upper() == UNIVERSE_TERM.upper():
            res_set = self.universe
            break
          for idx in self._qterm_indices(term):
            try:
              res_set = res_set & FuzzySet([x for x in self.tuid2relt[idx]])
            except KeyError:
              sys.stderr.write('\nW @ MemStoreIndex.evaluate(): term '+\
              'missing in the base relation index: '+self.lexicon[idx]+'\n')
        # setting the result dictionary entry to the resulting fuzzy set
        res_dict[action_item['NID']] = res_set
      else:
        # evaluating combined node action item 
        # generating direct dependencies first
        direct_deps = \
          (set(res_dict.keys()) & action_item['DEP']) - added_already
        added_already |= direct_deps
        # combining the previously computed direct dependencies according
        # to the node's action
        res_set = FuzzySet()
        if len(direct_deps) != 2:
          self.log.write('\nW @ MemStoreIndex.evaluate() - wrong number of '+\
            'direct dependencies\n')
        else:
          a = res_dict[list(direct_deps)[0]]
          b = res_dict[list(direct_deps)[1]]
          if action_item['ACT'] == 'MINUS':
            # making sure we've got the order of the arguments right (nothing
            # else than universe is not valid for MINUS)
            if a == self.universe:
              res_set = a - b
            elif b == self.universe:
              res_set = b - a
          elif action_item['ACT'] == 'UNION':
            res_set = a | b
          elif action_item['ACT'] == 'INTERSECTION':
            res_set = a & b
        res_dict[action_item['NID']] = res_set
    tuid_set = FuzzySet()
    try:
      tuid_set = res_dict[query.query_actions[-1]['NID']]
    except IndexError:
      sys.stderr.write('\nW @ MemStoreIndex.evaluate() - empty result\n')
    # generate a result object from the set of top-most combined result set
    return self._generate_result(tuid_set,query.name,queried,min_w=min_w,\
      max_n=max_n,max_e=max_e)

  def _generate_result(self,tuid_set,query_name,queried,min_w=0.5,max_n=50,\
  max_e=250):
    # generate a full-fledged result object from the given fuzzy term set

    result = MemStoreQueryResult(self,query_name,tuid_set,queried,min_w=min_w)
    result.populate_dictionaries()
    result.generate_visualisations(max_n=max_n,max_e=max_e)
    return result

# stuff required for building the MemStore indices

def open_index(path):
  # creating and/or opening the fulltext index
  schema = Schema(\
    identifier=ID(stored=True),\
    content=TEXT(analyzer=StemmingAnalyzer())\
  )
  ix = None
  try:
    # trying to create index if fresh
    ix = create_in(path,schema)
  except:
    exceptionType, exceptionValue, exceptionTraceback = sys.exc_info()
    msg = 'creating an existing fulltext index?\nException details:\n'+\
      '  exception type : '+`exceptionType`+'\n'+\
      '  exception value: '+`exceptionValue`+'\n'+\
      '  index directory: '+path+'\n'
    sys.stderr.write('\nW @ open_index(): '+msg)
  # opening and returning the index
  ix = open_dir(path)
  return ix

def update_index(ix,text2index):
  # updating index using a dictionary of text keys mapped to their identifiers
  writer = ix.writer()
  for text, index in text2index.items():
    try:
      writer.add_document(identifier=unicode(index),content=unicode(text))
    except UnicodeDecodeError:
      sys.stderr.write('\nW @ update_index(): unicode problems, skipping\n'+\
        '  problematic text: '+text+'\n')
  writer.commit()

def load_lex(fname):
  # loading the lexicon
  l = Lexicon()
  f = gzip.open(fname,'rb')
  l.from_file(f)
  f.close()
  return l

def load_corpus(fname):
  # loading the lexicon
  c = Tensor(rank=3)
  f = gzip.open(fname,'rb')
  c.from_file(f)
  f.close()
  return c

def load_src(fname):
  # loading the lexicon
  src = Tensor(rank=4)
  f = gzip.open(fname,'rb')
  src.from_file(f)
  f.close()
  return src

def load_suids(fname):
  # load the mapping of the statements to their IDs
  dct = {}
  for line in gzip.open(fname,'r').read().split('\n'):
    spl = line.split('\t')
    if len(spl) != 5:
      continue
    dct[(int(spl[1]),int(spl[2]),int(spl[3]))] = (int(spl[0]), float(spl[4]))
  return dct

def gen_cooc_suid2puid(sources,stmt2suid):
  # generating the mapping from extracted statements to provenances and their
  # weights
  dct = {}
  for s, p, o, prov in sources:
    w = sources[(s,p,o,prov)]
    suid = stmt2suid[(s,p,o)][0]
    if not suid in dct:
      dct[suid] = []
    dct[suid].append((prov,w))
  return dct

def gen_sim_suid2puid(stmt2suid,suid2puid,f_out,simrel_id):
  # process the stmt2suid, creating the dictionary mapping subjects to 
  # (predicate,object) tuples, and also generating a list of similarity
  # relationship statements together with their SUIDs and weights
  print '  - building the auxiliary dictionaries'
  s2po, sim_stmts = {}, {}
  for s,p,o in stmt2suid:
    if not s in s2po:
      s2po[s] = set()
    s2po[s].add((p,o))
    if p == simrel_id:
      sim_stmts[(s,o)] = stmt2suid[(s,p,o)]
  # process all the similarity statements, determining the co-occurrence
  # statements that led to them as an intersection of the (predicate,object)
  # tuple sets corresponding to the similar arguments
  print '  - processing the similarity statements'
  i, missing, processed = 0, 0, 0
  for s, o in sim_stmts:
    i += 1
    print '    ...', i, 'out of', len(sim_stmts)
    sim_suid, sim_w = sim_stmts[(s,o)]
    puid2weight = {}
    # processing the shared statements
    for p_prov, o_prov in s2po[s] & s2po[o]:
      prov_suid1 = stmt2suid[(s, p_prov, o_prov)][0]
      prov_suid2 = stmt2suid[(o, p_prov, o_prov)][0]
      l = []
      if prov_suid1 in suid2puid:
        l += suid2puid[prov_suid1]
      if prov_suid2 in suid2puid:
        l += suid2puid[prov_suid2]
      for puid, w in l:
        if not puid in puid2weight:
          puid2weight[puid] = []
        puid2weight[puid].append(w)
    if not len(puid2weight):
      missing += 1
    for puid in puid2weight:
      # computing the actual provenance weight as a product of the maximum
      # provenance weight of related co-occurrence statement and the similarity
      # value
      prov_w = max(puid2weight[puid])*sim_w
      # writing the provenance line to the out-file
      f_out.write('\n'+'\t'.join([str(sim_suid),str(puid),str(prov_w)]))
      processed += 1
  return missing, processed

if __name__ == "__main__":
  # setting the paths to the store and index
  store_path = os.getcwd()
  if len(sys.argv) > 1:
    store_path = os.path.abspath(sys.argv[1])
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
  lexicon = load_lex(lexicon_path)
  print '  ... updating/creating the fulltext index at:', fulltext_path
  # opening/creating the fulltext index
  ftext = open_index(fulltext_path)
  text2index = {}
  # getting the text, identifier pairs from the lexicon
  print '  - generating the (text, identifier) tuples'
  for text, index in lexicon.lex2int.items():
    text2index[text] = str(index)
  print '  - using the tuples to fill the fulltext index'
  update_index(ftext,text2index)
  print '*** Creating the corpus indices'
  print '  ... loading the corpus from:', corpus_path
  corpus = load_corpus(corpus_path)
  i, suid_lines, term_dict = 0, [], {}
  print '  ... generating the CSV representations from the corpus'
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
  print '  ... storing the CSV file:', os.path.join(index_path,'suids.tsv.gz')
  f = gzip.open(os.path.join(index_path,'suids.tsv.gz'),'wb')
  f.write('\n'.join(suid_lines))
  f.close()
  print '  ... storing the CSV file:', \
    os.path.join(index_path,'termsets.tsv.gz')
  term_lines = []
  for t1, rel_set in term_dict.items():
    for t2, w in rel_set:
      term_lines.append('\t'.join([str(x) for x in [t1,t2,w]]))
  f = gzip.open(os.path.join(index_path,'termsets.tsv.gz'),'wb')
  f.write('\n'.join(term_lines))
  f.close()
  print '*** Creating the provenance index'
  print '  ... loading the sources from:', sources_path
  sources = load_src(sources_path)
  print '  ... loading the statement -> SUID mapping from:', \
    os.path.join(index_path,'suids.tsv.gz')
  stmt2suid = load_suids(os.path.join(index_path,'suids.tsv.gz'))
  print '  ... generating the SUID->PUID mapping for co-occurrence statements'
  # SUID -> PUID dictionary for the SUID -> PUID similarity statements later on
  suid2puid = gen_cooc_suid2puid(sources,stmt2suid)
  # compressed file for the provenance CSV
  f_out = gzip.open(os.path.join(index_path,'provenances.tsv.gz'),'wb')
  print '  ... storing the SUID->PUID mapping for co-occurrence statements'
  lines = []
  for suid in suid2puid:
    for puid, w in suid2puid[suid]:
      lines.append('\t'.join([str(suid),str(puid),str(w)]))
  f_out.write('\n'.join(lines))
  print '  ... generating/storing the SUID->PUID mapping for sim. statements'
  simrel_id = lexicon['related_to']
  missing, processed = gen_sim_suid2puid(stmt2suid,suid2puid,f_out,simrel_id)
  print '  ... finished'
  print '  - missing sim. provenance info     :', missing
  print '  - generated sim. provenance entries:', processed
  f_out.close()
