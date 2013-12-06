SKIMMR
======

SKIMMR - a research prototype for machine-aided skim reading (includes back-end code for generating a graph-like knowledge base from texts and a standalone HTTP server-based UI). There is a biomedical and general text version of the prototype.

The codebase consists of three sub-branches:
- skimmr_base - basic code shared by the two different versions
- skimmr_bm - biomedical version
- skimmr_gt - version for general texts
The two versions of SKIMMR are commented in the following dedicated sections.

==============================
 SKIMMR_BM - a Brief Overview
==============================

-----------------------------------------------------
 Contact vit.novacek@deri.org for more detailed info
-----------------------------------------------------

0. ABSTRACT AND TABLE OF CONTENTS
=================================

This document provides basic information about SKIMMR (a tool for 
machine-aided skim reading), in particular about its SKIMMR_BM package version
that focuses on biomedical texts. 

The document contains three sections:

1. ABOUT - overview of the tool and its functionalities

2. INSTALLATION - basic instructions on how to install SKIMMR

3. USING SKIMMR - basic instruction on how to use it after installation

1. ABOUT
========

SKIMMR is a research prototype aimed at helping users to navigate through 
large amounts of textual data efficiently. This is done by extending the 
traditional paradigm of searching and browsing of text collections. SKIMMR 
lets the user skim texts by navigating a network of concepts and relations 
explicitly or implicitly present in them. The concepts and their relations 
have been extracted and inferred from the textual content using novel machine 
reading techniques that power the SKIMMR back-end.

The interconnected `skimming networks' provide a high-level overview of the 
domain covered by the texts, and let the user quickly discover interesting 
pieces of information. This process also largely reduces the burden of 
sieving through lots of irrelevant resources, which is often the down side 
of using the standard search engines. When the users identify interesting 
information within the high level overview, they can continue reading the 
related textual resources in detail. 

This particular version of SKIMMR (SKIMMR_BM) focuses on biomedical articles 
available on PubMed. The knowledge base exposed by the SKIMMR interface is 
extracted from the PubMed abstracts (see the SKIMMR back-end documentation for 
details). Once the users finish skimming, i.e., navigating the network of 
concepts and relations extracted from the PubMed articles, they can easily 
browse and read the related publications. This is done using an embedded 
window with PubMed results focused on the articles most relevant to the 
concepts discovered when skimming the data. Specific examples that illustrate 
the way SKIMMR works are provided in the following section. 

2. INSTALLATION
===============

Probably the easiest way of installing SKIMMR_BM is using easy_install:

	*easy_install skimmr_bm*

Check the documentation at http://peak.telecommunity.com/DevCenter/EasyInstall
for more detailed info on easy_install and setuptools.

Should you prefer to download and install the package manually, fetch the
SKIMMR_BM distribution archive file first. After unpacking it, switch to the 
generated directory and execute the following command:

	*python setup.py install*

If you want to install the package locally (for the current user only), use 
the following:

	*python setup.py install --user*

Check the documentation at http://docs.python.org/2/distutils/index.html for 
more detailed options.

3. USING SKIMMR
===============

After downloading and installing the SKIMMR package, it can be readily used
in a basic manner through the scripts provided. These are:

- dwnl_bm.py - download of the PubMed abstracts to be processed

- exst_bm.py - extraction of co-occurrence statements from texts

- crkb_bm.py - creation of a knowledge base and its population by semantic 
               similarity relations

- ixkb_bm.py - indexing of the knowledge base for efficient querying

- prep_bm.py - preparation of the sub-folder structure and resources in the 
               working directory necessary in order to launch the SKIMMR server

- srvr_bm.py - launching the SKIMMR server and UI

The scripts are located in the *bin* subdirectory of the installation package.
Alternatively, you can copy them from wherever your system puts Python package
binaries (check the documentation of your operating system and your local 
Python implementation).

The typical way of using these scripts is summarised in the following sections.
Note that there are other ways how to launch the scripts - you can check the 
documentation in the script source code for details.

3.1 Creating the working directory
----------------------------------

First of all, SKIMMR requires a place to store and process its data. Create a
directory for that somewhere (let us assume it is called *skimmr* in the 
following). Switch to that directory then and copy all the SKIMMR_BM scripts 
there. After that, run

	*python prep_bm.py*

in there. This will generate three sub-directories, *data*, *lingpipe* and 
*text*, as well as a couple of files and directories deeper in the *data* one. 
By default, the LingPipe text mining software is downloaded by this script,
as it is a preferred method for the biomedical knowledge extraction later
on. You can skip this, however, you need to copy the LingPipe software into
the *lingpipe* directory yourselves then, or use a general knowledge extraction
pipe-line built into the SKIMMR system (see the detailed options documented
in the script sources). 

3.2 Downloading abstracts from PubMed
-------------------------------------

SKIMMR_BM is specifically meant to process textual content available via the
PubMed repository. After placing one or more files with the *.pmid* extension
into the *text* folder in the *skimmr* directory, the tool can automatically 
download all the abstracts corresponding to the PubMed identifiers provided
in the *.pmid* files (divided by any sequence of white space characters or
commas). This is done by running the following

	*python dwnl_bm.py E-MAIL*

where E-MAIL is your e-mail (used for the purposes of the Entrez API used by 
SKIMMR to fetch the article data from PubMed).

3.3 Processing the texts
------------------------

If you have not downloaded PubMed abstracts based on their IDs in the previous
step, copy the locally stored abstracts files you want to process to the 
*text* folder in the *skimmr* directory. Plain text files (in ASCII or Unicode 
format) are supported, with the *.txt* extensions. Note that the filenames 
should be the PubMed IDs of the corresponding articles (plus the *.txt* 
extension), otherwise the source article look-up in SKIMMR later on will not
produce any meaningful results.

After you have all the texts in place, run

	*python exst_bm.py*

in the *skimmr* directory. This will chop up the texts into paragraphs and
extract the co-occurrence statements from them. There is a limit imposed
on the number of produced statements in the exst.py script, dynamically
computed from the available memory (or set to 750,000 if the psutil package
is not available on your system). You can change that when using the SKIMMR
library functions directly. 

3.4 Creating the knowledge base
-------------------------------

After generating the co-occurrence statements in the previous step, you can
create the knowledge base from them using 

	*python crkb_bm.py create*

which will generate a couple of knowledge base persistence files in the *stre*
sub-folder of the *data* directory in the *skimmr* root folder.

3.5 Computing similarities
--------------------------

When the knowledge base has been generated, you can augment it by computing
semantic similarity relationships between the terms that are more frequent 
than average:

	*python crkb_bm.py compsim*

This will update the knowledge base persistence files accordingly. Note that 
this step may take up to several hours for larger knowledge bases!

3.6 Indexing the knowledge base
-------------------------------

Before you can expose the processed content via a SKIMMR web interface, you
have to index the knowledge base. This is done by running

	*python ixkb_bm.py*

which will generate a couple of index files in the knowledge base persistence
sub-directory.

3.7 Launching and using the server
----------------------------------

At last, you can launch the SKIMMR server by

	*python srvr_bm.py*

This will start the server at localhost (127.0.0.1) and port 8008. You can 
specify alternative addresses and ports by running the server as

	*python srvr_bm.py ADDRESS:PORT*

Also, you can specify an alternative store to be loaded by the server (useful
if you want to examine multiple stores you have previously generated):

	*python srvr_bm.py [ADDRESS:PORT] [FOLDER]*

where FOLDER is a path to the store you want to load.

After the SKIMMR server has been started, you can point you browser to the 
corresponding address and port and start using the tool as indicated in the
*About* web-page accessible from the SKIMMR interface (just follow the link 
in the bottom of every page in the SKIMMR web interface).


==============================
SKIMMR_GT - a Brief Overview
==============================

-----------------------------------------------------
 Contact vit.novacek@deri.org for more detailed info
-----------------------------------------------------

0. ABSTRACT AND TABLE OF CONTENTS
=================================

This document provides basic information about SKIMMR (a tool for 
machine-aided skim reading), in particular about its SKIMMR_GT package version
that focuses on general texts. 

The document contains three sections:

1. ABOUT - overview of the tool and its functionalities

2. INSTALLATION - basic instructions on how to install SKIMMR

3. USING SKIMMR - basic instruction on how to use it after installation


1. ABOUT
========

SKIMMR is a research prototype aimed at helping users to navigate through 
large amounts of textual data efficiently. This is done by extending the 
traditional paradigm of searching and browsing of text collections. SKIMMR 
lets the user skim texts by navigating a network of concepts and relations 
explicitly or implicitly present in them. The concepts and their relations 
have been extracted and inferred from the textual content using novel machine 
reading techniques that power the SKIMMR back-end.

The interconnected `skimming networks' provide a high-level overview of the 
domain covered by the texts, and let the user quickly discover interesting 
pieces of information. This process also largely reduces the burden of 
sieving through lots of irrelevant resources, which is often the down side 
of using the standard search engines. When the users identify interesting 
information within the high level overview, they can continue reading the 
related textual resources in detail. 

2. INSTALLATION
===============

Probably the easiest way of installing SKIMMR is using easy_install:

	*easy_install skimmr_gt*

Check the documentation at http://peak.telecommunity.com/DevCenter/EasyInstall
for more detailed info on easy_install and setuptools.

Should you prefer to download and install the package manually, fetch the
SKIMMR distribution archive file first. After unpacking it, switch to the 
generated directory and execute the following command:

	*python setup.py install*

If you want to install the package locally (for the current user only), use 
the following:

	*python setup.py install --user*

Check the documentation at http://docs.python.org/2/distutils/index.html for 
more detailed options.

3. USING SKIMMR
===============

After downloading and installing the SKIMMR package, it can be readily used
in a basic manner through the scripts provided. These are:

- exst_gt.py - extraction of co-occurrence statements from texts

- crkb_gt.py - creation of a knowledge base and its population by semantic 
               similarity relations

- ixkb_gt.py - indexing of the knowledge base for efficient querying

- prep_gt.py - preparation of the sub-folder structure and resources in the 
               working directory necessary in order to launch the SKIMMR server

- srvr_gt.py - launching the SKIMMR server and UI

The scripts are located in the *bin* subdirectory of the installation package.
Alternatively, you can copy them from wherever your system puts Python package
binaries (check the documentation of your local operating system and Python
implementation).

The typical way of using these scripts is summarised in the following sections.
Note that there are other ways how to launch the scripts - you can check the 
documentation in the script source code for details.

3.1 Creating the working directory
----------------------------------

First of all, SKIMMR requires a place to store and process its data. Create a
directory for that somewhere (let us assume it is called *skimmr* in the 
following). Switch to that directory then and copy all the SKIMMR scripts 
there. After that, run

	*python prep_gt.py*

in there. This will generate two sub-directories, *data* and *text*, as well
as a couple of files and directories deeper in the *data* one. You are all set
for loading the texts you want to process into SKIMMR then.

3.2 Processing the texts
------------------------

Copy the text files you want to process to the *text* folder in the *skimmr*
directory. Plain text files (in ASCII or Unicode format) are supported, with
the *.txt* extensions. It is advisable to use meaningful and unique filenames
for the text files, as they will be used later on for assigning the provenance
identifiers to the original text data.

After you have all the texts in place, run

	*python exst_gt.py*

in the *skimmr* directory. This will chop up the texts into paragraphs and
extract the co-occurrence statements from them. There is a limit imposed
on the number of produced statements in the exst.py script, dynamically
computed from the available memory (or set to 750,000 if the psutil package
is not available on your system). You can change that when using the SKIMMR
library functions directly. 

3.3 Creating the knowledge base
-------------------------------

After generating the co-occurrence statements in the previous step, you can
create the knowledge base from them using 

	*python crkb_gt.py create*

which will generate a couple of knowledge base persistence files in the *stre*
sub-folder of the *data* directory in the *skimmr* root folder.

3.4 Computing similarities
--------------------------

When the knowledge base has been generated, you can augment it by computing
semantic similarity relationships between the terms that are more frequent 
than average:

	*python crkb_gt.py compsim*

This will update the knowledge base persistence files accordingly. Note that 
this step may take up to several hours for larger knowledge bases!

3.5 Indexing the knowledge base
-------------------------------

Before you can expose the processed content via a SKIMMR web interface, you
have to index the knowledge base. This is done by running

	*python ixkb_gt.py*

which will generate a couple of index files in the knowledge base persistence
sub-directory.

3.6 Launching and using the server
----------------------------------

At last, you can launch the SKIMMR server by

	*python srvr_gt.py*

This will start the server at localhost (127.0.0.1) and port 8008. You can 
specify alternative addresses and ports by running the server as

	*python srvr_gt.py ADDRESS:PORT*

Also, you can specify an alternative store to be loaded by the server (useful
if you want to examine multiple stores you have previously generated):

	*python srvr_gt.py [ADDRESS:PORT] [FOLDER]*

where FOLDER is a path to the store you want to load.

After the SKIMMR server has been started, you can point you browse to the 
corresponding address and port and start using the tool as indicated in the
*About* web-page accessible from the SKIMMR interface (just follow the link 
in the bottom of every page in the SKIMMR web interface).
