"""
extr.py - a SKIMMR extraction module implementing the following functionalities:
- conversion of raw text files (natural language) into tuples containing
  co-occurence statements extracted from the text, plus their certainty 
  weights and provenance

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

import os, nltk, sys, math, time
import util
from multiprocessing import Queue, cpu_count, Lock
from itertools import combinations
from nltk.chunk.regexp import RegexpParser
from nltk.tree import Tree
from nltk.stem.wordnet import WordNetLemmatizer

#CHUNK_GRAMMAR = """
#NP:
#  {<DT|JJ>}
#  }<[\.VI].*>+{
#  <.*>}{<DT>
#  <DT|JJ>{}<NN.*>
#"""

# @TODO - experiment with the grammars a bit perhaps

# with cardinals
#NP_GRAMMAR_SIMPLE = """
#NP: {<CD>?<JJ.*>*(<N.*>|<JJ.*>)+}
#"""
#
#NP_GRAMMAR_COMPOUND = """
#NP: {<CD>?<JJ.*>*(<N.*>|<JJ.*>)+((<IN>|<TO>)?<CD>?<JJ.*>*(<N.*>|<JJ.*>)+)*((<CC>|,)<CD>?<JJ.*>*(<N.*>|<JJ.*>)+((<IN>|<TO>)?<CD>?<JJ.*>*(<N.*>|<JJ.*>)+)*)*}
#"""

# no cardinals inlcuded
NP_GRAMMAR_SIMPLE = """
NP: {<JJ.*>*(<N.*>|<JJ.*>)+}
"""

NP_GRAMMAR_COMPOUND = """
NP: {<JJ.*>*(<N.*>|<JJ.*>)+((<IN>|<TO>)?<JJ.*>*(<N.*>|<JJ.*>)+)*((<CC>|,)<JJ.*>*(<N.*>|<JJ.*>)+((<IN>|<TO>)?<JJ.*>*(<N.*>|<JJ.*>)+)*)*}
"""

parser_cmp = RegexpParser(NP_GRAMMAR_COMPOUND)
parser_smp = RegexpParser(NP_GRAMMAR_SIMPLE)

# word separator in multi-term expressions
SEP = '_'

STOPLIST_STR = """
a
a's
able
about
above
according
accordingly
across
actually
after
afterwards
again
against
ain't
all
allow
allows
almost
alone
along
already
also
although
always
am
among
amongst
an
and
another
any
anybody
anyhow
anyone
anything
anyway
anyways
anywhere
apart
appear
appreciate
appropriate
are
aren't
around
as
aside
ask
asking
associated
at
available
away
awfully
b
be
became
because
become
becomes
becoming
been
before
beforehand
behind
being
believe
below
beside
besides
best
better
between
beyond
both
brief
but
by
c
c'mon
c's
came
can
can't
cannot
cant
cause
causes
certain
certainly
changes
clearly
co
com
come
comes
concerning
consequently
consider
considering
contain
containing
contains
corresponding
could
couldn't
course
currently
d
definitely
described
despite
did
didn't
different
do
does
doesn't
doing
don't
done
down
downwards
during
e
each
edu
eg
eight
either
else
elsewhere
enough
entirely
especially
et
etc
even
ever
every
everybody
everyone
everything
everywhere
ex
exactly
example
except
f
far
few
fifth
first
five
followed
following
follows
for
former
formerly
forth
four
from
further
furthermore
g
get
gets
getting
given
gives
go
goes
going
gone
got
gotten
greetings
h
had
hadn't
happens
hardly
has
hasn't
have
haven't
having
he
he's
hello
help
hence
her
here
here's
hereafter
hereby
herein
hereupon
hers
herself
hi
him
himself
his
hither
hopefully
how
howbeit
however
i
i'd
i'll
i'm
i've
ie
if
ignored
immediate
in
inasmuch
inc
indeed
indicate
indicated
indicates
inner
insofar
instead
into
inward
is
isn't
it
it'd
it'll
it's
its
itself
j
just
k
keep
keeps
kept
know
knows
known
l
last
lately
later
latter
latterly
least
less
lest
let
let's
like
liked
likely
little
look
looking
looks
ltd
m
mainly
many
may
maybe
me
mean
meanwhile
merely
might
more
moreover
most
mostly
much
must
my
myself
n
name
namely
nd
near
nearly
necessary
need
needs
neither
never
nevertheless
new
next
nine
no
nobody
non
none
noone
nor
normally
not
nothing
novel
now
nowhere
o
obviously
of
off
often
oh
ok
okay
old
on
once
one
ones
only
onto
or
other
others
otherwise
ought
our
ours
ourselves
out
outside
over
overall
own
p
particular
particularly
per
perhaps
placed
please
plus
possible
presumably
probably
provides
q
que
quite
qv
r
rather
rd
re
really
reasonably
regarding
regardless
regards
relatively
respectively
right
s
said
same
saw
say
saying
says
second
secondly
see
seeing
seem
seemed
seeming
seems
seen
self
selves
sensible
sent
serious
seriously
seven
several
shall
she
should
shouldn't
since
six
so
some
somebody
somehow
someone
something
sometime
sometimes
somewhat
somewhere
soon
sorry
specified
specify
specifying
still
sub
such
sup
sure
t
t's
take
taken
tell
tends
th
than
thank
thanks
thanx
that
that's
thats
the
their
theirs
them
themselves
then
thence
there
there's
thereafter
thereby
therefore
therein
theres
thereupon
these
they
they'd
they'll
they're
they've
think
third
this
thorough
thoroughly
those
though
three
through
throughout
thru
thus
to
together
too
took
toward
towards
tried
tries
truly
try
trying
twice
two
u
un
under
unfortunately
unless
unlikely
until
unto
up
upon
us
use
used
useful
uses
using
usually
uucp
v
value
various
very
via
viz
vs
w
want
wants
was
wasn't
way
we
we'd
we'll
we're
we've
welcome
well
went
were
weren't
what
what's
whatever
when
whence
whenever
where
where's
whereafter
whereas
whereby
wherein
whereupon
wherever
whether
which
while
whither
who
who's
whoever
whole
whom
whose
why
will
willing
wish
with
within
without
won't
wonder
would
would
wouldn't
x
y
yes
yet
you
you'd
you'll
you're
you've
your
yours
yourself
yourselves
z
zero
"""

STOPLIST = set([x.strip() for x in STOPLIST_STR.split()])

###############################################################################
## AUXILIARY FUNCTIONS
###############################################################################

def reset_cmp(grammar):
  # use for resetting the compound noun phrase parser with an external grammar
  parser_cmp = RegexpParser(grammar)

def reset_smp(grammar):
  # use for resetting the simple noun phrase parser with an external grammar
  parser_smp = RegexpParser(grammar)

###############################################################################
## LOWER-LEVEL FUNCTIONS (not supposed to be called directly by users)
###############################################################################

def get_cooc(chunk_trees,stoplist=True):
  triples, simple_trees = [], []
  lmtzr = WordNetLemmatizer()
  for t in chunk_trees:
    entities = []
    for chunk in t[:]:
      if isinstance(chunk,Tree) and chunk.node == 'NP':
        # getting a tree for later processing of triples from the simple noun 
        # phrases (if present)
        simple_trees.append(parser_smp.parse(chunk.leaves()))
        words = []
        for word, tag in chunk[:]:
          # stem/discard elements and construct an argument
          if (stoplist and word in STOPLIST) or \
          (len([x for x in word if x.isalnum()]) == 0):
            # do not process stopwords for simple trees, do not process purely 
            # non alphanumeric characters
            continue
          if tag.startswith('N'):
            words.append(lmtzr.lemmatize(word,'n'))
          elif tag.startswith('J'):
            words.append(lmtzr.lemmatize(word,'a'))
          else:
            words.append(word)
        if len(words) > 0:
          entities.append(SEP.join(words))
    for e1, e2 in combinations(entities,2):
      triples.append((e1,util.COOC_RELNAME,e2))
      triples.append((e2,util.COOC_RELNAME,e1))
  return triples, simple_trees

def parse_pos(fname):
  """
  Parses a POS-tagged file and return a dictionary of sentence number -> list 
  of (token,POS-tag) tuples.
  """

  dct = {}
  sent_no, sent_list = 0.0, []
  for line in open(fname,'r').read().split('\n'):
    if line.startswith('SENTENCE') and len(line.split('\t')) == 2:
      if len(sent_list) > 0:
        # dumping the current sentence to the dictionary
        dct[sent_no] = sent_list
      sent_list = []
      sent_no = float(line.split('\t')[1])
    elif len(line.split('\t')) == 3:
      sent_list.append((line.split('\t')[1],line.split('\t')[2]))
  # dumping the last sentence to the dictionary
  if len(sent_list) > 0:
    dct[sent_no] = sent_list
  return dct

def text2cooc(dct,filename,add_verbs=True):
  """
  Processes the input dictionary of sentence number -> list of (token,POS-tag) 
  tuples by the chunkers and generates/stores the term->sentence numbers 
  mapping for further processing by co-occurrence statement builder.
  """

  term2sentno, lmtzr = {}, WordNetLemmatizer()
  # process all sentences
  for sent_no in dct:
    if add_verbs:
      # updating the inverse occurrence index with verbs
      for token, tag in dct[sent_no]:
        if tag.startswith('VB'):
          v = lmtzr.lemmatize(token,'v').lower()
          if v not in STOPLIST:
            if v not in term2sentno:
              term2sentno[v] = set()
            term2sentno[v].add(sent_no)
    # trying to parse the sentence into a top-level chunk tree
    try:
      tree = parser_cmp.parse(dct[sent_no])
    except ValueError:
      sys.stderr.write('\nW @ text2cooc - funny sentence, ommitting:\n'+\
        str(dct[sent_no])+'\n')
      continue
    # getting the top-level tree triples and decomposing the NPs
    cmp_triples, simple_trees = get_cooc([tree],stoplist=False)
    smp_triples, discard = get_cooc(simple_trees,stoplist=True)
    # updating the inverse occurrence index with NPs 
    for s, p, o in cmp_triples + smp_triples:
      if s.lower() not in term2sentno:
        term2sentno[s.lower()] = set()
      if o.lower() not in term2sentno:
        term2sentno[o.lower()] = set()
      term2sentno[s.lower()].add(sent_no)
      term2sentno[o.lower()].add(sent_no)
  # store the dictionary
  f = open(filename,'w')
  # homogeneous, but possibly faulty due to Unicode errrors
  #f.write('\n'.join([key.encode('utf-8')+'\t'+','.join([str(x) for x in \
  #  value]) for key,value in term2sentno.items()]))
  # finer-grained exception handling
  for key, value in term2sentno.items():
    try:
      f.write(key.encode('utf-8')+'\t'+','.join([str(x) for x in value])+'\n')
    except UnicodeDecodeError:
      f.write(`key`+'\t'+','.join([str(x) for x in value])+'\n')
  f.close()
  return term2sentno

def text2postag(fname):
  """
  Tokenizes the text in the input file name and generates a file with 
  vertical representation of the POS-tagged text in the following form:

  SENTENCE\t1
  1\ttoken_1\tPOS_1
  2\ttoken_2\tPOS_2
  3\ttoken_3\tPOS_3
  ...
  n1\ttoken_n1\tPOS_n1
  SENTENCE\t2
  ...
  """

  # input text
  text = open(fname,'r').read()
  # output vertical
  pos_vert = []
  # going through the text, updating the vertical
  sent_no = 0
  for sentence in nltk.sent_tokenize(text):
    pos_vert.append('SENTENCE'+'\t'+str(sent_no))
    word_no = 0
    for word, tag in nltk.pos_tag(nltk.word_tokenize(sentence)):
      pos_vert.append('\t'.join([str(word_no),word,tag]))
      word_no += 1
    sent_no += 1
  # dumping the vertical
  outf = open(os.path.splitext(fname)[0]+'.pos','w')
  outf.write('\n'.join(pos_vert))
  outf.close()

def processor_cooc(identifier,job,lock,args):
  # basic job of the parallel co-occurrence extraction, dispatching one file
  add_verbs = args[0]
  dct, fname = job
  return text2cooc(dct,fname,add_verbs=add_verbs)

def processor_pos(identifier,job,lock,args):
  # basic job of the parallel POS tagging, dispatching one file
  return text2postag(job)

###############################################################################
## HIGHER-LEVEL FUNCTIONS FOLLOW
###############################################################################

def split_pars(path,wlimit=2000000):
  # splitting the texts in the path into paragraphs; if there are more words 
  # than wlimit, the paragraph list gets truncated to a size <= wlimit

  wsize = 0
  for fname in os.listdir(path):
    if os.path.splitext(fname)[-1].lower() != '.txt':
      continue
    text = open(os.path.join(path,fname),'r').read()
    par, pars, i = [], {}, 0
    for line in text.split('\n'):
      if len(line.strip()) == 0 and len(par) > 0:
        # empty line, flushing the paragraph
        pars[i] = ' '.join(par)
        wsize += sum([len(par_line.split()) for par_line in par])
        par = []
        if wsize > wlimit:
          break
        i += 1
      else:
        par.append(line)
    if len(par) > 0:
      # flushing the last paragraph
      pars[i] = ' '.join(par)
      wsize += sum([len(par_line.split()) for par_line in par])
    # dumping the paragraphs
    for par_id in pars:
      par_fname = os.path.join(path,\
        os.path.splitext(os.path.split(fname)[-1])[0]+'_'+str(par_id)+'.par')
      f = open(par_fname,'w')
      f.write(pars[par_id])
      f.close()
    if wsize > wlimit:
      break

def postag_texts(path,procn=cpu_count()):
  # filling the queue of jobs (POS-dictionaries and output filenames)
  jobs = Queue()
  for fname in os.listdir(path):
    if os.path.splitext(fname)[-1].lower() != '.par':
      continue
    jobs.put(os.path.join(path,fname))
  lock = Lock()
  # executing the POS tagging in parallel
  util.parex(jobs,processor_pos,lock,(),procn=procn,store_results=False)

def extract_cooc(path,procn=cpu_count(),add_verbs=True):
  # filling the queue of jobs (POS-dictionaries and output filenames)
  jobs = Queue()
  for fname in os.listdir(path):
    if os.path.splitext(fname)[-1].lower() != '.pos':
      continue
    dct = parse_pos(os.path.join(path,fname))
    filename = os.path.join(path,os.path.splitext(fname)[0]+'.t2s')
    jobs.put((dct,filename))
  lock = Lock()
  # executing the COOC extraction in parallel
  args = (add_verbs,)
  util.parex(jobs,processor_cooc,lock,args,procn=procn,store_results=False)

def gen_src(path,output=util.SRCSTM_FNAME,dist_thres=5,weight_thres=1.0/3,\
max_stmt=3000000):
  """
  Generates a lexical form of the source tensor in the tabular statement form.
  This can then be directly imported into the store.
  """

  lines = []
  fnames = [fname for fname in os.listdir(path) \
    if os.path.splitext(fname)[-1].lower() == '.t2s']
  i = 0
  for fname in fnames:
    i += 1
    print '  ... processing file', i, 'out of', len(fnames)
    # loading the term -> sentence number mapping
    terms2sentno = {}
    for line in open(os.path.join(path,fname),'r').read().split('\n'):
      if len(line.split('\t')) != 2:
        continue
      terms2sentno[line.split('\t')[0]] = [float(x) for x in \
        line.split('\t')[1].split(',')]
    print '  - number of terms to combine:', len(terms2sentno)
    # computing the co-occurence weights from the loaded mapping
    for t1, t2 in combinations(terms2sentno.keys(),2):
      w = 0.0
      for pos1 in terms2sentno[t1]:
        for pos2 in terms2sentno[t2]:
          d = math.fabs(pos1-pos2)
          if d < dist_thres:
            w += 1.0/(1.0+math.fabs(pos1-pos2))
      # updating the line list with the co-occurrence statement
      if w > weight_thres:
        lines.append('\t'.join([t1,util.COOC_RELNAME,t2,\
          os.path.splitext(fname)[0],str(w)]))
  if max_stmt <= 0:
    # storing all the statements to the output file
    f = open(os.path.join(path,output),'w')
    errors = 0
    for line in lines:
      try:
        f.write(line.encode('ascii','ignore')+'\n')
      except UnicodeEncodeError:
        errors += 1
        sys.err.write('W@exst_bm.py - Unicode error, omitting statement: %s\n'%\
          line)
    f.close()
    print '  ... number of Unicode errors:', errors
    f.close()
  else:
    # storing only the top-max_stmt relevant statements to the output file
    w2stmts = {}
    for line in lines:
      if len(line.split('\t')) != 5:
        continue
      s, p, o, d, w = line.split('\t')
      w = float(w)
      if not w in w2stmts:
        w2stmts[w] = []
      w2stmts[w].append(line)
    keys = w2stmts.keys()
    keys.sort(reverse=True)
    new_lines = []
    for key in keys:
      for line in w2stmts[key]:
        new_lines.append(line)
        if len(new_lines) >= max_stmt:
          break
      if len(new_lines) >= max_stmt:
        break
    f = open(os.path.join(path,output),'w')
    errors = 0
    for line in new_lines:
      try:
        f.write(line.encode('ascii',errors='replace')+'\n')
      except:
        errors += 1
        sys.stderr.write('W@exst_bm.py - Unicode error, omitting statement: %s\n'%\
          line)
    f.close()
    print '  ... number of Unicode errors:', errors

if __name__ == "__main__":
  path = os.getcwd()
  if len(sys.argv) > 1:
    path = os.path.abspath(sys.argv[1])
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
  start = time.time()
  gen_src(path)
  end = time.time()
  print '...finished in %s seconds' % (str(end-start),)
