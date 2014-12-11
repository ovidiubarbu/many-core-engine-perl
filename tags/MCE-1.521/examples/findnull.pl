#!/usr/bin/env perl
###############################################################################
## ----------------------------------------------------------------------------
## This script will report line numbers containing null values.
## Matches on regular expressions:  /\|\|/, /\|\t\s*\|/, or /\| \s*\|/
## Null value findings are reported to STDERR.
##
## Another use case of MCE chunking away at a use case scenario in which
## one wants to determine if any lines contain null fields, excluding
## the first and last fields. Think of a large file with very long lines.
##
## MCE does very well and runs much faster than the binary egrep can.
## Slurp IO with MCE is extremely fast. So, no matter how many workers
## you give to the problem, only a single worker slurps the next chunk
## at a given time. You get "sustained" sequential IO plus the workers
## for parallel processing.
##
## usage: findnull.pl [-l] datafile
##        findnull.pl wc.pl
##
###############################################################################

use strict;
use warnings;

use Cwd 'abs_path';  ## Remove taintedness from path
use lib ($_) = (abs_path().'/../lib') =~ /(.*)/;

my $prog_name = $0; $prog_name =~ s{^.*[\\/]}{}g;

sub INIT {
   ## Provide file globbing support under Windows similar to Unix.
   @ARGV = <@ARGV> if ($^O eq 'MSWin32');
}

use MCE;

###############################################################################
## ----------------------------------------------------------------------------
## Display usage and exit.
##
###############################################################################

sub usage {

   print <<"::_USAGE_BLOCK_END_::";

NAME
   $prog_name -- report line numbers containing null values

SYNOPSIS
   $prog_name [-l] file

DESCRIPTION
   The $prog_name script displays the line number containing null value(s).
   A null value is a match on /\\|\\|/ or /\\|\\s+\\|/.

   The following options are available:

   --max-workers MAX_WORKERS
          Specify number of workers for MCE   -- default: 8

   --chunk-size CHUNK_SIZE
          Specify chunk size for MCE          -- default: 2M

   -l     Display the number of lines for the file

EXIT STATUS
   The $prog_name utility exits 0 on success, and >0 if an error occurs.

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

my $chunk_size  = 2097152;  ## 2M
my $max_workers = 8;
my $skip_args   = 0;

my $l_flag = 0;
my $file   = ();

while ( my $arg = shift @ARGV ) {
   unless ($skip_args) {
      $l_flag      = $flag->() and next if ($arg eq '-l');

      $skip_args   = $flag->() and next if ($arg eq '--');
      $max_workers = $isOk->() and next if ($arg =~ /^--max[-_]workers$/);
      $chunk_size  = $isOk->() and next if ($arg =~ /^--chunk[-_]size$/);

      if ($arg =~ /^--max[-_]workers=(.+)/) {
         $max_workers = $1;
         next;
      }
      if ($arg =~ /^--chunk[-_]size=(.+)/) {
         $chunk_size = $1;
         next;
      }

      usage() if ($arg =~ /^-/);
   }

   $file = $arg;
}

usage() unless (defined $file);

unless (-e $file) {
   print "$prog_name: $file: No such file or directory\n";
   exit 2;
}
if (-d $file) {
   print "$prog_name: $file: Is a directory\n";
   exit 1;
}

###############################################################################
## ----------------------------------------------------------------------------
## Configure regex variables and define user_func for MCE.
##
###############################################################################

## It's actually faster to check them separately versus combining them
## into one regex delimited by |.

my @patterns = ('\|\|', '\|\t\s*\|', '\| \s*\|');
my $re = '(?:' . join('|', @patterns) . ')';

$re = qr/$re/;

sub user_func {

   my ($mce, $chunk_ref, $chunk_id) = @_;
   my ($found_match, $line_count, @lines);

   ## Check each one individually -- it's faster than doing (?:...|...|...)

   for (0 .. @patterns - 1) {
      if ($$chunk_ref =~ /$patterns[$_]/) {
         $found_match = 1;
         last;
      }
   }

   ## Slurp IO is enabled. $chunk_ref points to the raw scalar chunk.
   ## Each worker receives a chunk relatively fast. Worker processes
   ## chunk from memory.

   open my $_MEM_FH, '<', $chunk_ref;
   binmode $_MEM_FH;

   if ($found_match) {
      while (<$_MEM_FH>) {
         push @lines, $. if (/$re/);
      }
   }
   else {
      1 while (<$_MEM_FH>);
   }

   ## All workers report the last line count read irregardless on
   ## whether matching lines were found. The reason is that other
   ## workers may find matching lines and the callback function
   ## needs to accurately report line numbers.

   $line_count = $.;
   close $_MEM_FH;

   my %result = (
      'found_match' => $found_match, 'line_count' => $line_count,
      'lines' => \@lines
   );

   MCE->gather(\%result, $chunk_id);

   return;
}

###############################################################################
## ----------------------------------------------------------------------------
## Report line numbers containing null values.
##
###############################################################################

## Make an output iterator for gather. Output order is preserved.

my $total_lines = 0;

sub output_iterator {
   my %tmp; my $order_id = 1;

   return sub {
      my ($result, $chunk_id) = @_;
      $tmp{$chunk_id} = $result;

      while (1) {
         last unless exists $tmp{$order_id};
         my $r = $tmp{$order_id}; my $e;

         if ($r->{found_match}) {
            for (@{ $r->{lines} }) {
               $e = "NULL value at line " . ($_ + $total_lines) . " in $file";
               print STDERR "Warning: ", $e, "\n";
            }
         }

         $total_lines += $r->{line_count};
         delete $tmp{$order_id++};
      }
   };
}

## Configure MCE and run. Display total lines read if specified.

my $mce = MCE->new(
   chunk_size => $chunk_size, max_workers => $max_workers,
   input_data => $file, gather => output_iterator(),
   user_func => \&user_func, use_slurpio => 1
);

$mce->run();

print "$total_lines $file\n" if ($l_flag);

