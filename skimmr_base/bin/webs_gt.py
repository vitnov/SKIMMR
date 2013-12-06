#!/usr/bin/python
"""
webs_gt.py - a SKIMMR web service, accepting queries as strings and 
returning JSON strings representing the resulting graphs

Guide to execution:

python webs_gt.py [ADDRESS:PORT] [PATH]

where ADDRESS, PORT are the HTTP address and port on which the server is
supposed to be launched (defaults to 127.0.0.1, 8008), and PATH is a path to 
the working directory with all the necessary server data (default to current 
working directory).

Copyright (C) 2013 Vit Novacek (vit.novacek@deri.org), Digital Enterprise
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
TEST = True # change to false if not in the development version
LIB_PATH = '/home/vitnov/Work/devel/eureeka-lite/skimmr_base/lib'
#try:
#  from skimmr_gt.ifce import MemStoreIndex, MemStoreQuery
#  from skimmr_gt.util import dir_size
#except ImportError:
#  sys.path.append(LIB_PATH)
#  from ifce import MemStoreIndex, MemStoreQuery
#  from util import dir_size

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
# the global index variable to be used by the request handler
INDEX = None
# minimum weight to be taken into account for the result generation
THRESHOLD = 0.25
# threading lock for protecting shared writeable resources
# @TODO - check if it works as intended...
LOCK = threading.Lock()

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

  def evaluate(self,text_query,res_type='stmt',max_n=25,max_e=100):
    # auxiliary variables
    fname = text_query.replace(' ','_')+'.tmp'
    tq = text_query.lower()
    # constructing the XML version of the query
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
    # evaluating the query in the store
    query_obj = MemStoreQuery('\n'.join(lines),fname)
    query_res = INDEX.evaluate(query_obj,min_w=THRESHOLD,max_n=max_n,\
      max_e=max_e)
    # getting the JSON representation of the result (statements or provenances)
    return query_res.generate_json(res_type=res_type)

  def do_GET(self):
    # process GET requests

    # getting the dictionary with the input params
    pdict = urlparse.parse_qs(urlparse.urlsplit(self.path).query)
    # setting the default parameter values
    query_string, res_type = '', 'stmt'
    maxn, maxe = 50, 200
    # checking for the user-submitted values
    if 'query' in pdict:
      query_string = pdict['query'][0]
    if 'maxn' in pdict:
      try:
        maxn = int(pdict['maxn'][0])
      except ValueError:
        sys.stderr.write('W@do_GET(): invalid size request: '+\
          pdict['maxn'][0]+'\n')
    if 'maxe' in pdict:
      try:
        maxe = int(pdict['maxe'][0])
      except ValueError:
        sys.stderr.write('W@do_GET(): invalid size request: '+\
          pdict['maxe'][0]+'\n')
    if 'rtype' in pdict:
      res_type = pdict['rtype'][0]
    # evaluating the query
    result = self.evaluate(query_string,res_type,maxn,maxe)
    # sending responses, dumping the JSON result out
    self.send_response(200)
    self.send_header('Content-type','application/json')
    self.end_headers()
    self.wfile.write(result)

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
  # sorting out imports according to the TEST variable value
  if TEST:
    sys.path.append(LIB_PATH)
    from ifce import MemStoreIndex, MemStoreQuery
    from util import dir_size
  else:
    from skimmr_gt.ifce import MemStoreIndex, MemStoreQuery
    from skimmr_gt.util import dir_size
  # loading the main index
  try:
    INDEX = MemStoreIndex(STORE_PATH,trace=True)
  except:
    exceptionType, exceptionValue, exceptionTraceback = sys.exc_info()
    msg = 'could not open the index...\nException details:\n'+\
      '  exception type : '+`exceptionType`+'\n'+\
      '  exception value: '+`exceptionValue`+'\n'+\
      '  path attempted: '+STORE_PATH+'\n'
    sys.stderr.write('\nW @ srvr.py main: '+msg)
  # setting the default address and port
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
  # launch the server
  server = Server(address=address,port=port,trace=True)
  server.run()

