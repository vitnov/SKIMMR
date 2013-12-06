#!/usr/bin/python
"""
srvr_gt.py - a SKIMMR server, exposing the skim-reading functionalities via a 
self-contained HTTP server

Guide to execution:

python srvr_gt.py [ADDRESS:PORT] [PATH]

where ADDRESS, PORT are the HTTP address and port on which the server is
supposed to be launched (defaults to 127.0.0.1, 8008), and PATH is a path to 
the working directory with all the necessary server data (default to current 
working directory).

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

import sys, os, BaseHTTPServer, SocketServer, cgi, urlparse, tempfile, re,\
  hashlib, threading, datetime, shutil, urllib, traceback, time
# just for testing purposes
LIB_PATH = '/home/vitnov/Work/devel/eureeka-lite/skimmr_gt/skimmr_gt'
try:
  from skimmr_gt.ifce import MemStoreIndex, MemStoreQuery
  from skimmr_gt.util import dir_size
except ImportError:
  sys.path.append(LIB_PATH)
  from ifce import MemStoreIndex, MemStoreQuery
  from util import dir_size

# dictionary for generating the HTML of the max nodes select element, taking 
# the pre-selection into account
MAXN_SEL_KEYS = ['BEGTAG','10','25','50','100','ENDTAG'] # preferred key order
MAXN_SEL = {
'BEGTAG' : 
  {
    'DEF' : ' '*16 + '<select name="maxn">',
    'PRE' : ' '*16 + '<select name="maxn">'
  },
'10' : 
  {
    'DEF' : ' '*18 + '<option value="10">10</option>',
    'PRE' : ' '*18 + '<option value="10" selected="selected">10</option>'
  },
'25' : 
  {
    'DEF' : ' '*18 + '<option value="25">25</option>',
    'PRE' : ' '*18 + '<option value="25" selected="selected">25</option>'
  },
'50' : 
  {
    'DEF' : ' '*18 + '<option value="50">50</option>',
    'PRE' : ' '*18 + '<option value="50" selected="selected">50</option>'
  },
'100' : 
  {
    'DEF' : ' '*18 + '<option value="100">100</option>',
    'PRE' : ' '*18 + '<option value="100" selected="selected">100</option>'
  },
'ENDTAG' : 
  {
    'DEF' : ' '*16 + '</select>',
    'PRE' : ' '*16 + '</select>'
  }
}

# dictionary for generating the HTML of the max edges select element, taking 
# the pre-selection into account
MAXE_SEL_KEYS = ['BEGTAG','50','100','200','500','ENDTAG'] # preferred key order
MAXE_SEL = {
'BEGTAG' : 
  {
    'DEF' : ' '*16 + '<select name="maxe">',
    'PRE' : ' '*16 + '<select name="maxe">'
  },
'50' : 
  {
    'DEF' : ' '*18 + '<option value="50">50</option>',
    'PRE' : ' '*18 + '<option value="50" selected="selected">50</option>'
  },
'100' : 
  {
    'DEF' : ' '*18 + '<option value="100">100</option>',
    'PRE' : ' '*18 + '<option value="100" selected="selected">100</option>'
  },
'200' : 
  {
    'DEF' : ' '*18 + '<option value="200">200</option>',
    'PRE' : ' '*18 + '<option value="200" selected="selected">200</option>'
  },
'500' : 
  {
    'DEF' : ' '*18 + '<option value="500">500</option>',
    'PRE' : ' '*18 + '<option value="500" selected="selected">500</option>'
  },
'ENDTAG' : 
  {
    'DEF' : ' '*16 + '</select>',
    'PRE' : ' '*16 + '</select>'
  }
}
# mappging between file extensions and corresponding HTTP header types
EXT2HTTP_TYPE = {
  '.png' : 'image/png',
  '.gif' : 'image/gif',
  '.jpg' : 'image/jpeg',
  '.jpeg' : 'image/jpeg',
  '.tiff' : 'image/tiff',
  '.htm' : 'text/html',
  '.html' : 'text/html',
  '.tmp' : 'text/html',
  '.txt' : 'text/plain',
  '.css' : 'text/css',
  '.mpg' : 'video/mpeg',
  '.mpeg' : 'video/mpeg',
  '.avi' : 'video/avi'
}
# URL where the whole thing is running
BASE_URL = 'http://127.0.0.1:8008'
# root path
ROOT_PATH = os.getcwd()
# path to the original text files
TEXT_PATH = os.path.join(ROOT_PATH,'text')
# path to the global data directory
DATA_PATH = os.path.join(ROOT_PATH,'data')
# path to the store/index to be loaded
STORE_PATH = os.path.join(DATA_PATH,'stre')
# path to the HTML templates for the content rendering
HTML_PATH = os.path.join(DATA_PATH,'html')
# result HTML template
RESULT_TEMP = ''
try:
  RESULT_TEMP = open(os.path.join(HTML_PATH,'results.tmp'),'r').read()
except:
  exceptionType, exceptionValue, exceptionTraceback = sys.exc_info()
  msg = 'could not open result template...\nException details:\n'+\
    '  exception type : '+`exceptionType`+'\n'+\
    '  exception value: '+`exceptionValue`+'\n'+\
    '  path attempted: '+os.path.join(HTML_PATH,'results.tmp')+'\n'
  sys.stderr.write('\nW @ srvr.py main: '+msg)
# path to the user sub-dreictories (passwords and individual dynamic output)
USERS_PATH = os.path.join(DATA_PATH,'usrs')
# path to the cache directory with pre-computed PNG graph files
CACHE_PATH = os.path.join(DATA_PATH,'cche')
# file with a list of default cache entries, not to be deleted
CACHE_DEF = 'default.dirs'
# maximum allowed cache size (in B, 512 MB by default)
CACHE_LIMIT = 8*(2**30)
# fraction of the cache limit to be purged if necessary
CACHE_PURGF = 0.2
# dictionary mapping user names to the hashes of their passwords
PASSWD = {}
try:
  PASSWD = dict([tuple(line.split('\t')[:2]) for line in \
    open(os.path.join(USERS_PATH,'passwd'),'r').read().split('\n') if \
    len(line.strip()) > 0 and len(line.split('\t')) >= 2])
except:
  exceptionType, exceptionValue, exceptionTraceback = sys.exc_info()
  msg = 'could not open password info...\nException details:\n'+\
    '  exception type : '+`exceptionType`+'\n'+\
    '  exception value: '+`exceptionValue`+'\n'+\
    '  path attempted: '+os.path.join(USERS_PATH,'passwd')+'\n'
  sys.stderr.write('\nW @ srvr.py main: '+msg)
# the global index variable to be used by the request handler
INDEX = None
try:
  INDEX = MemStoreIndex(STORE_PATH,trace=True)
except:
  exceptionType, exceptionValue, exceptionTraceback = sys.exc_info()
  msg = 'could not open the index...\nException details:\n'+\
    '  exception type : '+`exceptionType`+'\n'+\
    '  exception value: '+`exceptionValue`+'\n'+\
    '  path attempted: '+STORE_PATH+'\n'
  sys.stderr.write('\nW @ srvr.py main: '+msg)
# minimum weight to be taken into account for the result generation
THRESHOLD = 0.25
# threading lock for protecting shared writeable resources
# @TODO - check if it works as intended...
LOCK = threading.Lock()
# maximum length of the query history displayed to the user
HIST_LEN = 100
# PubMed URL prefix for querying PubMed for related articles
# @TODO - uncomment for the biomedical SKIMMR version
#PUBMED_PREF = 'http://www.ncbi.nlm.nih.gov/pubmed?term='
# Google URL prefix for entity term look-up
GOOGLE_PREF = 'http://www.google.com/#q='
# mapping from action form values to action IDs to be logged
ACTIONS = {
  'query' : 'SKIM',
  'google_lookup' : 'LKUP',
  'source_lookup' : 'READ'
}
# forbidden queries
FORBIDDEN = set()
# extension of textual source files
TEXT_EXT = '.par'

# function for resetting the paths according to a new root path
def reset_paths(path):
  global ROOT_PATH, TEXT_PATH, DATA_PATH, STORE_PATH, HTML_PATH, RESULT_TEMP,\
    USERS_PATH, CACHE_PATH
  ROOT_PATH = path
  TEXT_PATH = os.path.join(ROOT_PATH,'text')
  DATA_PATH = os.path.join(ROOT_PATH,'data')
  STORE_PATH = os.path.join(DATA_PATH,'stre')
  HTML_PATH = os.path.join(DATA_PATH,'html')
  try:
    RESULT_TEMP = open(os.path.join(HTML_PATH,'results.tmp'),'r').read()
  except:
    exceptionType, exceptionValue, exceptionTraceback = sys.exc_info()
    msg = 'could not open result template...\nException details:\n'+\
      '  exception type : '+`exceptionType`+'\n'+\
      '  exception value: '+`exceptionValue`+'\n'+\
      '  path attempted: '+os.path.join(HTML_PATH,'results.tmp')+'\n'
    sys.stderr.write('\nW @ srvr.py main: '+msg)
  USERS_PATH = os.path.join(DATA_PATH,'usrs')
  CACHE_PATH = os.path.join(DATA_PATH,'cche')
  try:
    PASSWD = dict([tuple(line.split('\t')[:2]) for line in \
      open(os.path.join(USERS_PATH,'passwd'),'r').read().split('\n') if \
      len(line.strip()) > 0 and len(line.split('\t')) >= 2])
  except:
    exceptionType, exceptionValue, exceptionTraceback = sys.exc_info()
    msg = 'could not open password info...\nException details:\n'+\
      '  exception type : '+`exceptionType`+'\n'+\
      '  exception value: '+`exceptionValue`+'\n'+\
      '  path attempted: '+os.path.join(USERS_PATH,'passwd')+'\n'
    sys.stderr.write('\nW @ srvr.py main: '+msg)
  try:
    INDEX = MemStoreIndex(STORE_PATH,trace=True)
  except:
    exceptionType, exceptionValue, exceptionTraceback = sys.exc_info()
    msg = 'could not open the index...\nException details:\n'+\
      '  exception type : '+`exceptionType`+'\n'+\
      '  exception value: '+`exceptionValue`+'\n'+\
      '  path attempted: '+STORE_PATH+'\n'
    sys.stderr.write('\nW @ srvr.py main: '+msg)

class ThreadingSimpleServer(SocketServer.ThreadingMixIn,\
BaseHTTPServer.HTTPServer):
  """
  Simple multi-threading wrapper for the custom request handler.
  """

  pass

class RequestHandler(BaseHTTPServer.BaseHTTPRequestHandler):
  """
  Class handling the HTTP requests in the HTTP server wrapper for the 
  MemStoreIndex.
  """

  def parse_input(self,maxlen=0):
    # parsing the input of POST queries
    ctype = self.headers.get('content-type')
    if not ctype:
      return {}
    ctype, pdict = cgi.parse_header(ctype)
    if ctype == 'multipart/form-data':
      return cgi.parse_multipart(self.rfile,pdict)
    elif ctype == 'application/x-www-form-urlencoded':
      clength = int(self.headers.get('Content-length'))
      if maxlen and clength > maxlen:
        raise ValueError, 'Maximum content length exceeded'
      return urlparse.parse_qs(self.rfile.read(clength),1)
    else:
      return {}

  def format_main(self,params={}):
    # formatting the main (init) screen

    # getting the HTML template
    html = open(os.path.join(HTML_PATH,'main.tmp'),'r').read()
    return html % (BASE_URL,params['usrn'])

  def format_about(self,params={}):
    # format the about HTML page

    # getting the HTML template
    html = open(os.path.join(HTML_PATH,'about.tmp'),'r').read()
    return html

  def format_trial(self,params={}):
    # format the about HTML page

    # getting the HTML template
    html = open(os.path.join(HTML_PATH,'trial.tmp'),'r').read()
    return html

  def format_login(self,params={}):
    # format the login screen

    # getting the HTML template
    html = open(os.path.join(HTML_PATH,'login.tmp'),'r').read()
    return html % (params['status'],BASE_URL)

  def format_register(self,params={}):
    # format the user registration / passwd change screen

    # getting the HTML template
    html = open(os.path.join(HTML_PATH,'register.tmp'),'r').read()
    return html % (params['status'],BASE_URL)

  def format_results(self,params={}):
    # formatting the results page

    # getting the results HTML template
    html = open(os.path.join(HTML_PATH,'results.tmp'),'r').read()
    # generating pre-selected max. nodes and edges parts of the HTML form
    node_sel = []
    for key in MAXN_SEL_KEYS:
      if 'maxn' in params and key == params['maxn']:
        node_sel.append(MAXN_SEL[key]['PRE'])
      else:
        node_sel.append(MAXN_SEL[key]['DEF'])
    edge_sel = []
    for key in MAXE_SEL_KEYS:
      if 'maxe' in params and key == params['maxe']:
        edge_sel.append(MAXE_SEL[key]['PRE'])
      else:
        edge_sel.append(MAXE_SEL[key]['DEF'])
    # generating the link queries from the input list
    terms = []
    if 'terms' in params:
      terms = [' '*16 + '<option value="%s">%s</option>' % (x,x) for x in \
        params['terms'] if len(x.strip()) > 0]
    # generating the history options from the input list
    history = []
    if 'hist' in params:
      history = [' '*16 + '<option value="%s">%s</option>' % (x,x) for x in \
        params['hist'] if len(x.strip()) > 0]
    # placing the generated strings to the overall HTML content
    return html % (BASE_URL,params['usrn'],'\n'.join(terms),\
      '\n'.join(history),'\n'.join(node_sel),'\n'.join(edge_sel),\
      params['usrq'],\
      params['rwidth'],params['rheight'],params['usrn'],\
      params['extframe'],\
      #params['gurl'],params['usrn'],params['usrq'],params['maxn'],\
      #params['maxe'],
      '\n'.join(terms),\
      BASE_URL,params['usrn'],params['usrq'],params['maxn'],params['maxe'],\
      BASE_URL,params['usrn'],params['usrq'],params['maxn'],params['maxe'],\
      params['fwidth'])

  def format_head(self,params={}):
    # formatting the head of every HTML page generated

    # getting the head HTML template
    html = open(os.path.join(HTML_PATH,'head.tmp'),'r').read()
    return html % BASE_URL

  def format_foot(self,params={}):
    # formatting the foot of every HTML page generated

    # getting the foot HTML template
    html = open(os.path.join(HTML_PATH,'foot.tmp'),'r').read()
    return html % BASE_URL

  def _from_cache(self,usr_name,cached):
    # getting the cached result files from cache

    shutil.copyfile(os.path.join(cached,'ent_net.png'),\
      os.path.join(USERS_PATH,usr_name,'ent_net.png'))
    shutil.copyfile(os.path.join(cached,'ent_net.dot'),\
      os.path.join(USERS_PATH,usr_name,'ent_net.dot'))
    shutil.copyfile(os.path.join(cached,'ent_lst.txt'),\
      os.path.join(USERS_PATH,usr_name,'ent_lst.txt'))
    shutil.copyfile(os.path.join(cached,'prv_lst.txt'),\
      os.path.join(USERS_PATH,usr_name,'prv_lst.txt'))
    shutil.copyfile(os.path.join(cached,'ewt_lst.txt'),\
      os.path.join(USERS_PATH,usr_name,'ewt_lst.txt'))

  def _to_cache(self,usr_name,cached):
    # storing the results to cache

    # purging the cache if too big
    # @TODO - do more clever and only partial purging (LFU)
    if dir_size(CACHE_PATH) > CACHE_LIMIT:
      print 'DEBUG - RequestHandler._to_cache() - purging the cache'
      print '  (size: %s, limit: %s)' % (str(dir_size(CACHE_PATH)),\
        str(CACHE_LIMIT))
      # a set of default cache entries, not to be deleted
      default_cache = set(open(os.path.join(CACHE_PATH,\
        CACHE_DEF),'r').read().split('\n'))
      for dir_name in set(os.listdir(CACHE_PATH)) - default_cache:
        shutil.rmtree(os.path.join(CACHE_PATH,dir_name))
    # creating the directory
    if not os.path.exists(cached):
      os.makedirs(cached)
    # copying the actual files
    shutil.copyfile(os.path.join(USERS_PATH,usr_name,'ent_net.png'),\
      os.path.join(cached,'ent_net.png'))
    shutil.copyfile(os.path.join(USERS_PATH,usr_name,'ent_net.dot'),\
      os.path.join(cached,'ent_net.dot'))
    shutil.copyfile(os.path.join(USERS_PATH,usr_name,'ent_lst.txt'),\
      os.path.join(cached,'ent_lst.txt'))
    shutil.copyfile(os.path.join(USERS_PATH,usr_name,'prv_lst.txt'),\
      os.path.join(cached,'prv_lst.txt'))
    shutil.copyfile(os.path.join(USERS_PATH,usr_name,'ewt_lst.txt'),\
      os.path.join(cached,'ewt_lst.txt'))

  def evaluate(self,text_query,max_n=25,max_e=100,usr_name='test',form={}):
    path = os.path.join(USERS_PATH,usr_name)
    fname = text_query.replace(' ','_')+'.tmp'
    tq = text_query.lower()
    if text_query in FORBIDDEN or tq in FORBIDDEN:
      tq = ''
    lines = [
      '<?xml version="1.0" ?>',
      '<xml>', 
      '  <qterm operation="EVALUATION">',
      '    <expression>',
      '      '+tq,
      '    </expression>',
      '  </qterm>',
      '</xml>'
    ]
    # checking the cache
    query_hash = hashlib.sha224(tq+\
      '_maxn=%s-maxe=%s'%(max_n,max_e)).hexdigest()
    cached = os.path.join(CACHE_PATH,query_hash)
    if os.path.exists(cached):
      # present in hash, no need to evaluate the query
      self._from_cache(usr_name,cached)
    else:
      query_obj = MemStoreQuery('\n'.join(lines),fname)
      query_res = INDEX.evaluate(query_obj,min_w=THRESHOLD,max_n=max_n,\
        max_e=max_e)
      # dumping the select result data
      # base entity and provenance dump filenames
      ent_base = os.path.join(path,'ent_net')
      # dumping the PNG visualisations 
      try:
        query_res.vis_dict['STMTS'].write_png(ent_base+'.png',\
          prog=query_res.vis_par['PROG'])
      except:
        # error in graph file rendering
        shutil.copyfile(os.path.join(HTML_PATH,'render_error.png'),\
          ent_base+'.png')
        # printing the exception details out
        exceptionType, exceptionValue, exceptionTraceback = sys.exc_info()
        msg = 'something went wrong in graph rendering...\n'+\
          'Exception details:\n'+\
          '  exception type : '+`exceptionType`+'\n'+\
          '  exception value: '+`exceptionValue`+'\n'
        sys.stderr.write('\nE @ RequestHandler.evaluate(): '+msg)
        sys.stderr.write('  exception traceback printout:\n')
        traceback.print_tb(exceptionTraceback)
      # dumping the .dot sources
      query_res.vis_dict['STMTS'].write(ent_base+'.dot')
      # dumping the list of result terms 
      res_terms = [x[1] for x in query_res.vis_maps['STMTS']]
      f = open(os.path.join(path,'ent_lst.txt'),'w')
      f.write('\n'.join(res_terms))
      f.close()
      # dumping the list of result provenances (just the IDs, no fragments)
      res_provs = set([x[1].split('_')[0] for x in query_res.vis_maps['PROVS']])
      f = open(os.path.join(path,'prv_lst.txt'),'w')
      f.write('\n'.join(res_provs))
      f.close()
      # dumping the entity,weight tuple list from the result
      ewt_lst = [str(ent_id)+','+str(ent_w) for ent_id, ent_w in \
        query_res.tuid_cut.items()] # (entity ID,relevance weight) list
      f = open(os.path.join(path,'ewt_lst.txt'),'w')
      f.write('\n'.join(ewt_lst))
      f.close()
      # copying the stuff to cache
      self._to_cache(usr_name,cached)
    # dumping the entity graph snippet
    html_temp = open(os.path.join(HTML_PATH,'ent_net.tmp'),'r').read()
    f = open(os.path.join(path,'ent_net.htm'),'w')
    f.write(html_temp % (usr_name,usr_name))
    f.close()

  def do_GET(self):
   # process static GET requests

   file_ext = os.path.splitext(self.path)[-1].lower()
   if file_ext == '':
     # empty request, render the login screen
     self.send_response(200)
     self.send_header('Content-type','text/html')
     self.end_headers()
     html = open(os.path.join(HTML_PATH,'login.tmp'),'r').read()
     content = '\n'.join([self.format_head(),html % ('',BASE_URL),\
       self.format_foot()])
     self.wfile.write(content)
   elif file_ext in EXT2HTTP_TYPE:
     # HTML or image file will be served if it exists
     local_path = urllib.unquote(self.path.strip('/'))
     # making sure the path separators are OK w.r.t. the local file system
     local_path = local_path.replace('/',os.sep)
     ## fixing possible redundant directory prefixing due to relative paths
     #local_path_split = local_path.split(os.sep)
     #if len(local_path_split) >= 2 and \
     #local_path_split[0] == local_path_split[1]:
     #  local_path = os.sep.join(local_path_split[1:])
     # processing the file according to its re-constructed global path
     global_path = os.path.join(os.getcwd(),local_path)
     if os.path.exists(global_path):
       self.send_response(200)
       self.send_header('Content-type',EXT2HTTP_TYPE[file_ext])
       self.end_headers()
       if file_ext == '.tmp':
         # processing an HTML template
         fname = global_path.split('/')[-1]
         body = ''
         if fname == 'login.tmp':
           body = self.format_login({'status':''})
         elif fname == 'about.tmp':
           body = self.format_about()
         elif fname == 'trial.tmp':
           body = self.format_trial()
         elif fname == 'register.tmp':
           body = self.format_register({'status':''})
         content = '\n'.join([self.format_head(),body,self.format_foot()])
         self.wfile.write(content)
       else:
         # serving the file as is otherwise
         self.wfile.write(open(global_path,'r').read())
     else:
       self.send_response(404)
   else:
     # unsupported media type
     self.send_response(415)

  def do_POST(self):
    # processes POST form data dynamically, generating query results

    form = self.parse_input()
    action, html = '', ''
    if 'action' in form:
      action = form['action'][0]
    if action == 'query':
      html = self.process_query(form)
    elif action == 'login':
      html = self.process_login(form)
    elif action == 'register':
      html = self.process_registration(form)
    elif action == 'google_lookup':
      html = self.process_google_lookup(form)
    elif action == 'source_lookup':
      html = self.process_source_lookup(form)
    elif action == 'feedback':
      html = self.process_feedback(form)
    # generating response according to the action identified and HTML generated
    if len(action) == 0:
      # unknown action
      self.send_response(400)
    else:
      # recognised and processed action
      self.send_response(200)
      self.send_header('Content-type','text/html')
      self.end_headers()
      self.wfile.write(html)

  def process_login(self,form):
    # processing login information

    usrn, passwd = '', ''
    if 'usrn' in form:
      usrn = form['usrn'][0]
    if 'passwd' in form:
      passwd = form['passwd'][0]
    passwd_hash = hashlib.sha224(passwd).hexdigest()
    params = {}
    if usrn in PASSWD and PASSWD[usrn] == passwd_hash:
      # correct password for an existing user, proceed to the main screen and
      # reset/archive the possible previous session history
      shist_fname = os.path.join(USERS_PATH,usrn,'session_history.txt')
      if os.path.exists(shist_fname):
        os.rename(shist_fname,os.path.splitext(shist_fname)[0]+\
          '-archived_at_%s.txt' % \
          str(datetime.datetime.now()).split('.')[0].replace(' ','_'))
      params['usrn'] = usrn
      return '\n'.join([self.format_head(params),self.format_main(params),\
        self.format_foot(params)])
    else:
      # login unsuccesful, regenerate the login screen
      params['status'] = '<div style="color:red">Login incorrect</div>'
      return '\n'.join([self.format_head(params),self.format_login(params),\
        self.format_foot(params)])

  def process_registration(self,form):
    # processing user registration

    global PASSWD
    params = {}
    usrn, old_passwd, new_passd = '', '', ''
    if 'usrn' in form:
      usrn = form['usrn'][0]
    if 'old_passwd' in form:
      old_passwd = form['old_passwd'][0]
    if 'new_passwd' in form:
      new_passwd = form['new_passwd'][0]
    old_passwd_hash = hashlib.sha224(old_passwd).hexdigest()
    new_passwd_hash = hashlib.sha224(new_passwd).hexdigest()
    if usrn in PASSWD:
      # changing the password
      if old_passwd_hash == PASSWD[usrn]:
        # matching old passwd
        LOCK.acquire()
        try:
          PASSWD[usrn] = new_passwd_hash
          passwd_f = open(os.path.join(USERS_PATH,'passwd'),'w')
          passwd_f.write('\n'.join([x[0]+'\t'+x[1] for x in PASSWD.items()]))
          passwd_f.close()
        finally:
          LOCK.release()
        params['status'] = '<div style="color:green">Change of password successful, proceed to <a href="/data/html/login.tmp">login</a>.</div>'
      else:
        # old passwd mismatch
        params['status'] = '<div style="color:red">Wrong password or user name.</div>'
    else:
      # registering the user
      # updating the PASSWD variable and file
      LOCK.acquire()
      try:
        PASSWD[usrn] = new_passwd_hash
        passwd_f = open(os.path.join(USERS_PATH,'passwd'),'w')
        passwd_f.write('\n'.join([x[0]+'\t'+x[1] for x in PASSWD.items()]))
        passwd_f.close()
      finally:
        LOCK.release()
      # creating the user's directory if not created before
      usr_dir = os.path.join(USERS_PATH,usrn)
      if os.path.exists(usr_dir):
        # something weird
        params['status'] = '<div style="color:red">Unable to register the user name:</div> '+usrn
      else:
        # creating the user directory, setting the status to OK
        os.mkdir(usr_dir)
        params['status'] = '<div style="color:green">Registration successful, proceed to <a href="/data/html/login.tmp">login</a>.</div>'
        # dumping the user profile
        f = open(os.path.join(usr_dir,'profile.txt'),'w')
        lines = ['User name:\t'+usrn]
        if 'occupation' in form:
          lines.append('Occupation:\t'+';'.join(form['occupation']))
        if 'education' in form:
          lines.append('Education:\t'+';'.join(form['education']))
        if 'field' in form:
          lines.append('Field:\t'+';'.join(form['field']))
        if 'additional' in form:
          lines.append('Additional info:\t'+\
            self._clean_text(form['additional'][0]))
        f.write('\n'.join(lines))
        f.close()
    return '\n'.join([self.format_head(params),\
      self.format_register(params),self.format_foot(params)])

  def process_query(self,form):
    # processing a user query

    # getting the information from the form:
    # - the max. nodes, max. edges values
    maxn, maxe = 25, 100
    if 'maxn' in form:
      maxn = form['maxn'][0]
    if 'maxe' in form:
      maxe = form['maxe'][0]
    # - one of the pre-defined options
    defq = ''
    if 'defq' in form:
      if form['defq'][0] != 'None':
        defq = form['defq'][0]
    # - the atrbitrary user query
    usrq = ''
    if 'usrq' in form:
      usrq = form['usrq'][0]
    # - a query from the history list
    hist = ''
    if 'hist' in form:
      if form['hist'][0] != 'None':
        hist = form['hist'][0]
    # the name of the user that generated the current POST query
    usrn = 'test'
    if 'usrn' in form:
      usrn = form['usrn'][0]
    query = defq
    if len(hist) > 0:
      query = hist
    if len(usrq) > 0:
      query = usrq # user query is most prominent
    # evaluating the query using the global MemStoreIndex instance and storing
    # the results to the ./data directory, updating also the history files
    print 'DEBUG - RequestHandler.process_query(): evaluating:', \
      query
    start = time.time()
    self.evaluate(query,int(maxn),int(maxe),usrn,form)
    end = time.time()
    print 'DEBUG - RequestHandler.process_query(): evaluation finished in', \
      end-start, 'seconds'
    # updating the history
    self.update_history(form,alt_query=query)
    # getting the list of recent (skimming-only) history queries
    hist_list = [x.split('\t')[2].strip() for x in \
      open(os.path.join(USERS_PATH,usrn,'history.txt'),\
      'r').read().split('\n') if len(x.strip()) > 0 and \
       x.split('\t')[1].strip() == 'SKIM'][-HIST_LEN:]
    hist_list.reverse()
    # setting the HTML formatting parameters
    params = {
      'maxn' : maxn, # maximum number of nodes
      'maxe' : maxe, # maximum number of edges
      'terms' : [x.strip() for x in open(os.path.join(USERS_PATH,usrn,\
        'ent_lst.txt'),'r').read().split('\n')], # result terms
      'hist' : hist_list, # history terms
      'rwidth' : '96%',   # width of the entity result iframes
      'rheight' : '600px',# height of all the result iframes
      'fwidth' : '96%',   # width of the result feedback table
      'usrn' : usrn,   # name of the current user
      'usrq' : query, # current query
      'gurl' : 'http://www.google.com', # Google URL for the form action
      'extframe' : '' # related stuff, empty here
    }
    # returning formatted HTML
    return '\n'.join([self.format_head(params),self.format_results(params),\
      self.format_foot(params)])

  def process_feedback(self,form):
    # processing the explicitly submitted feedback on results

    # the max. nodes, max. edges values
    maxn, maxe = 25, 100
    if 'maxn' in form:
      maxn = form['maxn'][0]
    if 'maxe' in form:
      maxe = form['maxe'][0]
    # user name, user query
    usrn, usrq = 'test', ''
    if 'usrn' in form:
      usrn = form['usrn'][0]
    if 'usrq' in form:
      usrq = form['usrq'][0]
    # updating the user's feedback file
    self.update_feedback(form)
    # getting the list of recent (skimming-only) history queries
    hist_list = [x.split('\t')[2].strip() for x in \
      open(os.path.join(USERS_PATH,usrn,'history.txt'),\
      'r').read().split('\n') if len(x.strip()) > 0 and \
       x.split('\t')[1].strip() == 'SKIM'][-HIST_LEN:]
    hist_list.reverse()
    # the parameters for formatting the refreshed results page body
    params = {
      'maxn' : maxn, # maximum number of nodes
      'maxe' : maxe, # maximum number of edges
      'terms' : [x.strip() for x in open(os.path.join(USERS_PATH,usrn,\
        'ent_lst.txt'),'r').read().split('\n')], # result terms
      'hist' : hist_list, # history terms
      'rwidth' : '96%',   # width of the entity result iframes
      'rheight' : '600px',# height of all the result iframes
      'fwidth' : '96%',   # width of the result feedback table
      'usrn' : usrn,   # name of the current user
      'gurl' : 'http://www.google.com', # Google URL for the form action
      'extframe' : '', # HMTML snippet - empty this time
      'usrq' : usrq # current query
    }
    # returning formatted HTML
    return '\n'.join([self.format_head(params),self.format_results(params),\
      self.format_foot(params)])

  def process_google_lookup(self,form):
    # generating the list of PubMed IDs relevant to the current stage of the 
    # browsing session

    # the max. nodes, max. edges values
    maxn, maxe = 25, 100
    if 'maxn' in form:
      maxn = form['maxn'][0]
    if 'maxe' in form:
      maxe = form['maxe'][0]
    # user name, user query
    usrn, usrq = 'test', ''
    if 'usrn' in form:
      usrn = form['usrn'][0]
    if 'usrq' in form:
      usrq = form['usrq'][0]
    # entity the user is looking up
    entity = ''
    if 'entity' in form:
      entity = form['entity'][0]
    # getting the list of recent (skimming-only) history queries
    hist_list = [x.split('\t')[2].strip() for x in \
      open(os.path.join(USERS_PATH,usrn,'history.txt'),\
      'r').read().split('\n') if len(x.strip()) > 0 and \
       x.split('\t')[1].strip() == 'SKIM'][-HIST_LEN:]
    hist_list.reverse()
    # update history
    self.update_history(form)
    # generate a Google link snippet
    url = GOOGLE_PREF + urllib.quote(entity)
    google_snip = """
    <h2><a href="%s">Web pages</a> related to: <i>%s</i></h2>
    <iframe width="%s" height="600px" frameborder="1" scrolling="yes" src="%s">
      Google reference could not be displayed - the iframe HTML feature
      not supported by your browser.
    </iframe>
    <br/><br/>
    """ % (url,entity,'96%',url)
    # the parameters for formatting the refreshed results page body
    params = {
      'maxn' : maxn, # maximum number of nodes
      'maxe' : maxe, # maximum number of edges
      'terms' : [x.strip() for x in open(os.path.join(USERS_PATH,usrn,\
        'ent_lst.txt'),'r').read().split('\n')], # result terms
      'hist' : hist_list, # history terms
      'rwidth' : '96%',   # width of the entity result iframes
      'rheight' : '600px',# height of all the result iframes
      'fwidth' : '96%',   # width of the result feedback table
      'usrn' : usrn,   # name of the current user
      'gurl' : url, # Google URL for the form action
      'extframe' : google_snip, # HTML snippet with a Google info
      'usrq' : usrq # current query
    }
    # returning formatted HTML
    return '\n'.join([self.format_head(params),self.format_results(params),\
      self.format_foot(params)])

  def process_source_lookup(self,form):
    # generating the HTML summary of the current sources

    # the max. nodes, max. edges values
    maxn, maxe = 25, 100
    if 'maxn' in form:
      maxn = form['maxn'][0]
    if 'maxe' in form:
      maxe = form['maxe'][0]
    # maximum provenances, user name, user query
    maxa, usrn, usrq = 'all', 'test', ''
    if 'maxa' in form:
      maxa = form['maxa'][0]
    if 'usrn' in form:
      usrn = form['usrn'][0]
    if 'usrq' in form:
      usrq = form['usrq'][0]
    # getting the list of recent (skimming-only) history queries
    hist_list = [x.split('\t')[2].strip() for x in \
      open(os.path.join(USERS_PATH,usrn,'history.txt'),\
      'r').read().split('\n') if len(x.strip()) > 0 and \
       x.split('\t')[1].strip() == 'SKIM'][-HIST_LEN:]
    hist_list.reverse()
    print 'DEBUG - RequestHandler.process_query(): computing provenance:', \
      usrq
    start = time.time()
    terms = [x.strip() for x in open(os.path.join(USERS_PATH,usrn,\
      'ent_lst.txt'),'r').read().split('\n')]
    f = open(os.path.join(USERS_PATH,usrn,'prv_htm.htm'),'w')
    f.write(self.generate_prvhtm(usrn,maxa,terms))
    f.close()
    end = time.time()
    print 'DEBUG - RequestHandler.process_query(): prov. comp. finished in', \
      end-start, 'seconds'
    # update history
    self.update_history(form)
    # external frame snippet
    src_snip = """
    <h2>Source paragraphs related to: <i>%s</i></h2>
    <iframe width="%s" height="600px" frameborder="1" scrolling="yes" src="%s">
      Provenance sources could not be displayed - the iframe HTML feature
      not supported by your browser.
    </iframe>
    <br/><br/>
    """ % (usrq,'96%','/data/usrs/'+usrn+'/prv_htm.htm')
    # the parameters for formatting the refreshed results page body
    params = {
      'maxn' : maxn, # maximum number of nodes
      'maxe' : maxe, # maximum number of edges
      'terms' : terms, # result terms
      'hist' : hist_list, # history terms
      'rwidth' : '96%',   # width of the entity result iframes
      'rheight' : '600px',# height of all the result iframes
      'fwidth' : '96%',   # width of the result feedback table
      'usrn' : usrn,   # name of the current user
      'gurl' : 'http://www.google.com', # Google URL for the form action
      'extframe' : src_snip, # HTML snippet with the provenance info
      'usrq' : usrq # current query
    }
    # returning formatted HTML
    return '\n'.join([self.format_head(params),self.format_results(params),\
      self.format_foot(params)])

  def generate_prvhtm(self,usrn,maxa,terms):
    # generates an HTML string representing the requested number of source 
    # paragraphs

    # query result term IDs to their weights
    tuid2weight = dict([(int(line.split(',')[0]),float(line.split(',')[1])) \
      for line in open(os.path.join(USERS_PATH,usrn,'ewt_lst.txt'),\
      'r').read().split('\n') if len(line.split(',')) == 2])
    # provenances of the terms to their aggregate weight
    prov2weight = {}
    for tuid in tuid2weight:
      tuid_w = tuid2weight[tuid]
      for suid in INDEX.tuid2suid[tuid]:
        for puid, puid_w in INDEX.suid2prov[suid]:
          # getting lexical provenance representation
          prov_lexform = INDEX.lexicon[puid]
          #prov, chunk = prov_lexform.split('_')[0], prov_lexform.split('_')[-1]
          #if not prov.isdigit() or int(chunk) > 0:
          #  # @TODO - perhaps do the abstract-only filtering more cleverly
          #  continue # no PubMed abstract ID
          if prov_lexform not in prov2weight:
            prov2weight[prov_lexform] = 0.0
          prov2weight[prov_lexform] += tuid_w*puid_w
    # lexicalised provenances
    srt_prv = prov2weight.items()
    # sorting the provenances according to their frequency
    srt_prv.sort(key=lambda x: x[1], reverse=True)
    # cutting of the list possibly
    limit = 0
    try:
      limit = int(maxa)
    except ValueError:
      pass
    if limit > 0:
      srt_prv = srt_prv[:limit]
    # the internal provenance HTML frame initialisation
    html = ['<!DOCTYPE html>']
    html += ['<html>','<head>']
    html += ['<link rel="stylesheet" type="text/css"'+\
      'href="/data/html/style.css" media="screen"/>','</style>','</head>','',\
      '<body>']
    # list of the query-relevant terms
    #query_terms = [INDEX.lexicon[x] for x in tuid2weight]
    # generating a chunk of HTML for each provenance source
    i = 1
    for prov, weight in srt_prv:
      text = open(os.path.join(TEXT_PATH,prov+TEXT_EXT),'r').read()
      html += ['  '+str(i)+': <i>'+prov+'</i><br/>']
      html += ['  <p><small>'+self._prep_text(text,terms)+\
        '</small></p><br/>']
      i += 1
    # finishing off the HTML generation with a footer
    html += ['</body>','</html>']
    return '\n'.join(html)

  def _prep_text(self,text,query_terms):
    # adapting the text to emphasise the query terms in bold
    core_text = self._clean_text(text)
    chunks = []
    regexp = '|'.join([x.replace('_',' ').replace('  ',' ') \
      for x in query_terms])
    print 'DEBUG - _prep_text(): RE pattern for query results (raw):', regexp
    regexp = self._clean_regexp(regexp)
    print 'DEBUG - _prep_text(): RE pattern for query results (clean):', regexp
    pattern = re.compile(regexp,flags=re.I)
    pos = 0
    match = pattern.search(core_text,pos)
    while match:
      # adding the stuff between the matches
      chunks.append(core_text[pos:match.start()])
      # adding the match in HTML bold
      chunks.append('<b>'+core_text[match.start():match.end()]+'</b>')
      # getting the next match
      pos = match.end() + 1
      match = pattern.search(core_text,pos)
    if pos < len(core_text):
      chunks.append(core_text[pos:])
    return '<p>'+' '.join(chunks)+'</p>'

  def _clean_regexp(self,regexp):
    to_remove = ['(',')','*','+','.','\\']
    for c in to_remove:
      r = regexp.replace(c,'')
    return r

  def _clean_text(self,text):
    # possibly add more sophisticated cleaning if needed

    result = text.replace('\t',' ')
    result = result.replace('\n',' ')
    result = result.replace('\r',' ')
    result = result.replace('  ',' ')
    return result

  def update_history(self,form,alt_query=''):
    # extracts the user feedback on the current screen from the form and stores
    # it together with the corresponding query and a time-stamp
    # the file is in a simple tab-separated value format, where the fields are
    # as follows:
    # 1. time stamp
    # 2. action type (SKIM or READ)
    # 3. current query
    # 4. maximum nodes
    # 5. maximum edges
    # 6. maximum publications to look-up

    # time-stamp in the beginning of the record
    line = [str(datetime.datetime.now())]
    # user name, maximum number of nodes, edges and provenances
    usrq, maxn, maxe, maxa = '', 'Unknown', 'Unknown', 'Unknown'
    usrn = 'test'
    if 'usrn' in form:
      usrn = form['usrn'][0]
    if 'usrq' in form:
      usrq = form['usrq'][0]
    if len(usrq) == 0:
      if len(alt_query) > 0:
        usrq = alt_query
      else:
        usrq = 'Unknown'
    if 'maxn' in form:
      maxn = form['maxn'][0]
    if 'maxe' in form:
      maxe = form['maxe'][0]
    if 'maxa' in form:
      maxa = form['maxa'][0]
    action = 'Unknown'
    if 'action' in form and form['action'][0] in ACTIONS:
      action = ACTIONS[form['action'][0]]
    line += [action,usrq,maxn,maxe,maxa]
    f = open(os.path.join(USERS_PATH,usrn,'history.txt'),'a')
    f.write('\t'.join(line)+'\n')
    f.close()

  def update_feedback(self,form):
    # records explicit user feedback on a particular result in the following 
    # tab-separated format:
    # 1. time stamp
    # 2. current query
    # 3. maximum nodes
    # 4. maximum edges
    # 5. maximum publications to look-up
    # 6. pre-defined entity feedback
    # 7. detailed entity feedback
    # 8. pre-defined provenance feedback
    # 9. detailed provenance feedback

    # time-stamp in the beginning of the record
    line = [str(datetime.datetime.now())]
    # user name, maximum number of nodes, edges and provenances
    usrn,usrq,maxn,maxe,maxa = 'test','Unknown','Unknown','Unknown','Unknown'
    if 'usrn' in form:
      usrn = form['usrn'][0]
    if 'usrq' in form:
      usrq = form['usrq'][0]
    if 'maxn' in form:
      maxn = form['maxn'][0]
    if 'maxe' in form:
      maxe = form['maxe'][0]
    if 'maxa' in form:
      maxa = form['maxa'][0]
    line += [usrq,maxn,maxe,maxa]
    # entity feedback fields
    fb_ent, fb_ent_det = 'None','None'
    # provenance feedback fields
    fb_prv, fb_prv_det = 'None','None'
    if 'fb_ent' in form:
      fb_ent = ';'.join(form['fb_ent'])
    if 'fb_ent_det' in form:
      fb_ent_det = self._clean_text(form['fb_ent_det'][0])
      if len(fb_ent_det.strip()) == 0:
        fb_ent_det = 'None'
    if 'fb_prv' in form:
      fb_prv = ';'.join(form['fb_prv'])
    if 'fb_prv_det' in form:
      fb_prv_det = self._clean_text(form['fb_prv_det'][0])
      if len(fb_prv_det.strip()) == 0:
        fb_prv_det = 'None'
    # updating the line container with the feedback values
    line += [fb_ent, fb_ent_det, fb_prv, fb_prv_det]
    # appending the feedback data to the user's feedback file
    f = open(os.path.join(USERS_PATH,usrn,'feedback.txt'),'a')
    f.write('\t'.join(line)+'\n')
    f.close()

class Server:
  """
  Simple server class, wrapping the MemStoreIndex-based request handler.
  """

  def __init__(self,address='',port=8008,trace=False,\
  log_filename='memstore-server.log'):
    # service variables
    self.trace = trace
    self.log_filename = log_filename
    # initialising the HTTP server
    server_address = ('', 8080)
    self.address = address
    self.port = port
    server_address = (self.address,self.port)
    # non-threading alternative
    #self.http_server = BaseHTTPServer.HTTPServer(server_address,RequestHandler)
    # threading alternative
    self.http_server = ThreadingSimpleServer(server_address,RequestHandler)

  def run(self):
    # different code from the multithreading example, try the former one first
    #try:
    #  print '*** HTTP server started...'
    #  while 1:
    #    sys.stdout.flush()
    #    server.handle_request()
    #except KeyboardInterrupt:
    #  '\n*** Shutting the HTTP server down...'
    #  self.http_server.socket.close()
    # classic code, maybe doesn't work for the multithreading, but try at least
    try:
      print '*** HTTP server started...'
      self.http_server.serve_forever()
    except KeyboardInterrupt:
      print '\n*** Shutting the HTTP server down...'
      self.http_server.socket.close()

if __name__ == "__main__":
  # default address and port
  address, port = '127.0.0.1', 8008
  # checking for a possible address:port OR path argument
  if len(sys.argv) > 1:
    arg = sys.argv[1]
    if len(arg.split(':')) == 2:
      # interpreting as an address:port argument
      address, port = arg.split(':')
      port = int(port)
      # updating the base URL of the application
      BASE_URL = 'http://'+address+':'+str(port)
    elif os.path.exists(os.path.absval(arg)):
      # interpreting as an alternative root path
      ROOT_PATH = os.path.absval(os.path.absval(arg))
      # resetting all the path-dependent stuff
      reset_paths(ROOT_PATH)
  # checking for a possible root path argument
  if len(sys.argv) > 2:
    # use other path then default
    ROOT_PATH = os.path.absval(sys.argv[2])
    # resetting all the path-dependent stuff
    reset_paths(ROOT_PATH)
  # launch the server
  server = Server(address=address,port=port,trace=True)
  server.run()

