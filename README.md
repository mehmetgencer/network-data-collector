network-data-collector
======================

Data-set collection utilities for social network research. These utilities are aimed to simplify the task of collecting virtual community data such as mail-groups (nia NNTP), and exporting the data to common formats suitable for use in social network analysis software.

Currently, a single utility "collectorNNTP.py" is provided for collecting data via news server interface (NNTP). 
This utility defaults to gmane server, which archives numerous software developer mail-groups.

The collector script can be stopped and continues data collection from where its left off when restarted.

Installation and use
---------------------
Works for Python3. Requires dateutil package.

Run the program without arguments to get some help:

    $ python3 collectorNNTP.py

The program must be invoked by providing a project name:

    $ python3 collectorNNTP.py proj1

It will then create directory ~/network-data-collector  for persisting the collected data as well as the program state (in case it is interrupted and re-started). It then uses yserial library (included with this distribution) to persist data into
this directory with the given project name. Thus you can work on multiple projects simultaneously. 

When a project is first created, the program will walk you through its setup, by asking which NNTP server to use (defaults
to gmane server), username/password (leave empty if not used), and then optionally retrieves and presents a list of all available
groups on the server. At that stage you can enter one or more mail-group names, each on one line, ending with an empty line.
If you use multiple mail-groups in a single data set, they will be treated as a single data set, but you may have the option
of including mailgroup data in some of the data export alternatives.

    Starting setup of your project... 
    server (default: news.gmane.org) :
    port (default: 119) :
    username (default: ) :
    password (default: ) :
    Following groups were selected for download from this server:
     ...
    Will get a list of mailgroups at server. Press ENTER to continue, or enter 'no' to skip:no
     ...
    Enter the group names you want to collect, finish with an empty line
    Enter group name:gmane.politics.activism.buildacarfreecity
    Enter group name:
    Setup is completed.

Once the setup is complete you can start ollecting data:

    $ python3 collectorNNTP.py proj1 collect

Finally, you can export the dataset in one of supported formats, e.g. for Gephi GEXF format:

    $ python3 collectorNNTP.py proj1 dumpGEXF outfile.gexf
