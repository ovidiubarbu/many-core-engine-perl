# Many-core Engine for Perl #

MCE for Perl helps enable a new level of performance by maximizing all available cores.

https://metacpan.org/pod/MCE

![http://many-core-engine-perl.googlecode.com/svn/wiki/img/MCE.gif](http://many-core-engine-perl.googlecode.com/svn/wiki/img/MCE.gif)

# Description #

MCE spawns a pool of workers and therefore does not fork a new process per each element of data. Instead, MCE follows a bank queuing model. Imagine the line being the data and bank-tellers the parallel workers. MCE enhances that model by adding the ability to chunk the next n elements from the input stream to the next available worker.

![http://many-core-engine-perl.googlecode.com/svn/wiki/img/Bank_Queuing_Model.gif](http://many-core-engine-perl.googlecode.com/svn/wiki/img/Bank_Queuing_Model.gif)

# Examples #

MCE comes with various examples showing real-world use case scenarios on parallelizing something as small as cat (try with -n) to greping for patterns and word count aggregation. An implementation of the wide finder is also provided utilizing MCE.

https://metacpan.org/pod/MCE::Examples

Make your code run faster with Perl's secret turbo module; by David Farrell.

http://perltricks.com/article/61/2014/1/21/Make-your-code-run-faster-with-Perl-s-secret-turbo-module

A Mojolicious and MCE demonstration (go towards the end of the article).

http://perltricks.com/article/73/2014/2/28/The-Perl-Nerd-Merit-Badge-Contest-results

The mce\_mojolicious.pl code; by Justin Hawkins.

https://gist.github.com/tardisx/9088819

MCE includes a dual chunk-level {file, list} wrapper script for grep binaries.

https://metacpan.org/source/MARIOROY/MCE-1.600/bin/mce_grep

The mce-sandbox has been finalized on Github (Sandboxing with Perl + MCE + Inline::C).

https://github.com/marioroy/mce-sandbox

# Supported OS and maximum workers allowed #

MCE spawns child processes using zero threads. The inclusion of threads or the forks module tells MCE to use threads instead.

![http://many-core-engine-perl.googlecode.com/svn/wiki/img/Supported_OS.gif](http://many-core-engine-perl.googlecode.com/svn/wiki/img/Supported_OS.gif)