#!/usr/bin/env perl
###############################################################################
## ----------------------------------------------------------------------------
## Wide Finder script done with Many-core Engine for Perl.
##
## http://www.tbray.org/ongoing/When/200x/2007/09/20/Wide-Finder
## http://www.tbray.org/ongoing/When/200x/2007/10/30/WF-Results
## http://www.tbray.org/ongoing/When/200x/2007/11/12/WF-Conclusions
##
## Heh, I'm 5 years late :). I only came to know of Wide Finder about two
## months ago. MCE does quite well for this use case. One doesn't have to
## worry about a worker crossing into another worker's boundary as can be
## seen with the fastest solution utilizing MMAP IO.
##
## Try testing 8, 16, 32 workers on a "code cache" file under Linux. The MMAP
## progressively slows down as number of workers increases. The reason why is
## that all workers utilizing MMAP IO are performing disk IO simultaneously
## causing an increase in random IO access which increases latency.
##
## That's not the case with MCE. MCE follows the bank-queuing model plus
## chunking utilizing a single queue depth. This helps enable "sustained"
## sequential IO. MCE can be seen 2x ~ 3x faster when processing a file
## not residing inside the OS file system cache.
##
## Results for a 384 MB logfile (2 million lines) utilizing 8 workers:
##
## Sys::Mmap (Cold Cache).........:  28.531s  (Read IO is random)
## Sys::Mmap (Warm Cache).........:   0.156s
##
## Many-core Engine (Cold Cache)..:  13.341s  (Read IO is sequential)
## Many-core Engine (Warm Cache)..:   0.157s
##
## MCE, with its Chunking and Slurp IO capability, is beneficial for the
## "wide-data" usage pattern of all sorts.
##
## Default behaviour is to spawn off child processes.
## Pass --use_threads or --use_forks to use threads instead.
##
## usage: wide_finder.pl logfile
##
###############################################################################

use strict;
use warnings;

use Cwd qw( abs_path );

our ($prog_name, $prog_dir, $base_dir);
my  ($use_threads, $use_forks);

BEGIN {
   $ENV{PATH} = "/usr/bin:/bin:/usr/sbin:/sbin:$ENV{PATH}";

   $prog_name = $0;                    ## prog name
   $prog_name =~ s{^.*[\\/]}{}g;
   $prog_dir  = abs_path($0);          ## prog dir
   $prog_dir  =~ s{[\\/][^\\/]*$}{};
   $base_dir  = $prog_dir;             ## base dir
   $base_dir  =~ s{[\\/][^\\/]*$}{};

   unshift @INC, "$base_dir/lib";

   for (@ARGV) {
      $use_threads = 1 if ($_ eq '--use_threads');
      $use_forks   = 1 if ($_ eq '--use_forks');
   }

   if (($use_threads ? 1 : 0) + ($use_forks ? 1 : 0) == 1) {
      if ($use_threads) {
         local $@; local $SIG{__DIE__} = sub { };
         eval {
            require threads; threads->import();
            require threads::shared; threads::shared->import();
         };
         if ($@) {
            print STDERR "This Perl lacks threads && threads::shared modules\n";
            exit 3;
         }
      }
      elsif ($use_forks) {
         local $@; local $SIG{__DIE__} = sub { };
         eval {
            require forks; forks->import();
            require forks::shared; forks::shared->import();
         };
         if ($@) {
            print STDERR "This Perl lacks forks && forks::shared modules\n";
            exit 3;
         }
      }
   }
}

use Time::HiRes qw( time );
use MCE;

###############################################################################
## ----------------------------------------------------------------------------
## Display usage and exit.
##
###############################################################################

sub usage {

   print <<"::_USAGE_BLOCK_END_::";

NAME
   $prog_name -- "wide finder" reporting tool

SYNOPSIS
   $prog_name logfile

DESCRIPTION
   The $prog_name script is an implementation of the "wide finder"
   benchmark by Tim Bray at:

   http://www.tbray.org/ongoing/When/200x/2007/09/20/Wide-Finder
   http://www.tbray.org/ongoing/When/200x/2007/10/30/WF-Results
   http://www.tbray.org/ongoing/When/200x/2007/11/12/WF-Conclusions
  
   The following options are available:

   --chunk_size CHUNK_SIZE
          Specify chunk size for MCE             Default: 2000000

   --max_workers MAX_WORKERS
          Specify number of workers for MCE      Default: 8

   --use_threads or --use_forks
          Script will include threads & threads::shared/forks & forks::shared
          modules -- workers are threads versus being child processes

EXAMPLES

   $prog_name --chunk_size 100000 --max_workers 6 logfile
   $prog_name --max_workers 8 logfile

::_USAGE_BLOCK_END_::

   exit 1
}

###############################################################################
## ----------------------------------------------------------------------------
## Define defaults and process command-line arguments.
##
###############################################################################

my $flag = sub { 1; };
my $isOk = sub { (@ARGV == 0 or $ARGV[0] =~ /^-/) ? usage() : shift @ARGV; };

my $chunk_size  = 2000000;
my $max_workers = 8;
my $skip_args   = 0;

my $logfile;

while ( my $arg = shift @ARGV ) {
   unless ($skip_args) {
      $chunk_size  = $isOk->() and next if ($arg eq '--chunk_size');
      $max_workers = $isOk->() and next if ($arg eq '--max_workers');
      $use_threads = $flag->() and next if ($arg eq '--use_threads');
      $use_forks   = $flag->() and next if ($arg eq '--use_forks');
      $skip_args   = $flag->() and next if ($arg eq '--');

      usage() if ($arg =~ /^-/);
   }

   $logfile = $arg;
}

usage() if (($use_threads ? 1 : 0) + ($use_forks ? 1 : 0) == 2);
usage() unless (defined $logfile);

unless (-e $logfile) {
   print "$prog_name: $logfile: No such file or directory\n";
   exit 2;
}
if (-d $logfile) {
   print "$prog_name: $logfile: Is a directory\n";
   exit 1;
}

###############################################################################
## ----------------------------------------------------------------------------
## Parallelize via MCE. Slurp IO is enabled for raw processing.
##
###############################################################################

my %total = ();

## Callback function for storing sub-totals.

sub store_result {
   my ($hash_ref) = @_;
   for (keys %$hash_ref) {
      $total{$_} += $hash_ref->{$_};
   }
}

## Compute via MCE. Think of user_begin, user_func, user_end like the awk
## scripting language: awk 'BEGIN { ... } { ... } END { ... }'. All workers
## submit their totals after processing all chunks.

my $mce = MCE->new(
   max_workers => $max_workers,
   chunk_size  => $chunk_size,
   use_slurpio => 1,

   user_begin => sub {
      my $self = shift;
      $self->{wk_hash} = {};
   },
   user_func => sub {
      my ($self, $chunk_ref, $chunk_id) = @_;
      $self->{wk_hash}{$1}++ while (
         $$chunk_ref =~
            m{GET /ongoing/When/\d\d\dx/(\d\d\d\d/\d\d/\d\d/[^ .]+) }g
      );
   },
   user_end => sub {
      my $self = shift;
      $self->do('store_result', $self->{wk_hash});
   }
);

$mce->spawn();

my $start = time();
$mce->process($logfile);
my $end = time();

$mce->shutdown();

## Display results.

my $n = 0;

for (sort { $total{$b} <=> $total{$a} } keys %total) {
    print "$total{$_}\t$_\n";
    last if ++$n >= 10;
}

printf "\n## Compute time: %0.03f\n\n",  $end - $start;

