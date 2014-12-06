#!/usr/bin/env perl
###############################################################################
## ----------------------------------------------------------------------------
## This script, similar to the forseq.pl example as far as usage goes, assigns
## input_data a closure (the iterator itself) by calling a factory function.
##
## usage: iterator.pl [ size ]
## usage: iterator.pl [ begin end [ step [ format ] ] ]
##
##   e.g. iterator.pl 10 20 2
##
## The format string is passed to sprintf (% is optional).
##
##   e.g. iterator.pl 20 30 0.2 %4.1f
##        iterator.pl 20 30 0.2  4.1f
##
###############################################################################

use strict;
use warnings;

use Cwd 'abs_path';  ## Remove taintedness from path
use lib ($_) = (abs_path().'/../lib') =~ /(.*)/;

my $prog_name = $0; $prog_name =~ s{^.*[\\/]}{}g;

use Time::HiRes qw(time);
use MCE;

my $s_begin  = shift || 3000;
my $s_end    = shift;
my $s_step   = shift || 1;
my $s_format = shift;

if ($s_begin !~ /\A\d*\.?\d*\z/) {
   print STDERR "usage: $prog_name [ size ]\n";
   print STDERR "usage: $prog_name [ begin end [ step [ format ] ] ]\n";
   exit;
}

$s_format =~ s/^%// if (defined $s_format);

unless (defined $s_end) {
   $s_end = $s_begin - 1; $s_begin = 0;
}

###############################################################################
## ----------------------------------------------------------------------------
## Input and output iterators using closures.
##
## A closure construction typically involves two functions: the closure itself;
## and a factory, the fuction that creates the closure.
##
###############################################################################

## Generates a sequence of numbers. The external variables ($n, $max, $step)
## are used for keeping state across successive calls to the closure. The
## iterator returns undef when $n exceeds max.

sub input_iterator {
   my ($n, $max, $step) = @_;

   return sub {
      return if $n > $max;

      my $current = $n;
      $n += $step;

      return $current;
   };
}

## Preserves output order. The external variables (%result_n, %result_d) are
## used for temporary storage for out-of-order results. The external variable
## ($order_id) is incremented after printing to STDOUT in orderly fashion.
##
## The external variables keep their state across successive calls to the
## closure.

sub output_iterator {
   my (%result_n, %result_d); my $order_id = 1;

   return sub {
      $result_n{ $_[2] } = $_[0];
      $result_d{ $_[2] } = $_[1];

      while (1) {
         last unless exists $result_d{$order_id};

         printf "n: %s sqrt(n): %f\n",
            $result_n{$order_id}, $result_d{$order_id};

         delete $result_n{$order_id};
         delete $result_d{$order_id};

         $order_id++;
      }

      return;
   };
}

###############################################################################
## ----------------------------------------------------------------------------
## Parallelize via MCE.
##
###############################################################################

my $mce = MCE->new(

   chunk_size => 1, max_workers => 3, gather => output_iterator(),

   user_func => sub {
      my ($mce, $chunk_ref, $chunk_id) = @_;

      if (defined $s_format) {
         my $n = sprintf "%${s_format}", $_;
         MCE->gather($n, sqrt($n), $chunk_id);
      }
      else {
         MCE->gather($_, sqrt($_), $chunk_id);
      }
   }

)->spawn;

my $start = time();

$mce->process( input_iterator($s_begin, $s_end, $s_step) );

my $end = time();

printf STDERR "\n## Compute time: %0.03f\n\n", $end - $start;

