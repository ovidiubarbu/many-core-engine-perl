#!/usr/bin/env perl
###############################################################################
## ----------------------------------------------------------------------------
## This example demonstrates the sqrt example from Parallel::Loops, with MCE.
## MCE does not fork a new child process for each @input_data.
##
## The number below indicates the size of @input_data which can be submitted
## and displayed in 1 second. Output was directed to /dev/null during testing.
##
## Parallel::Loops is based on Parallel::ForkManager.
##
## Parallel::Loops..:       600  Forking each @input is expensive
## MCE foreach......:    34,000  Sends result after each @input
## MCE forseq.......:    70,000  Loops through sequence of numbers
## MCE forchunk.....:   480,000  Chunking reduces overhead
##
## usage: forseq.pl [ size ]
## usage: forseq.pl [ begin end [ step [ format ] ] ]
##
##   For format arg, think as if passing to sprintf (without the %)
##   forseq.pl 20 30 0.2 4.1f
##
###############################################################################

use strict;
use warnings;

use Cwd 'abs_path'; ## Insert lib-path at the head of @INC.
use lib abs_path($0 =~ m{^(.*)[\\/]} && $1 || abs_path) . '/../lib';

use Time::HiRes qw(time);
use MCE;

my $prog_name = $0; $prog_name =~ s{^.*[\\/]}{}g;
my $s_begin   = shift || 3000;
my $s_end     = shift;
my $s_step    = shift || 1;
my $s_format  = shift;

if ($s_begin !~ /\A\d*\.?\d*\z/) {
   print {*STDERR} "usage: $prog_name [ size ]\n";
   print {*STDERR} "usage: $prog_name [ begin end [ step [ format ] ] ]\n";
   exit;
}

unless (defined $s_end) {
   $s_end = $s_begin - 1; $s_begin = 0;
}

###############################################################################
## ----------------------------------------------------------------------------
## Parallelize via MCE's forseq method.
##
###############################################################################

## Make an output iterator for gather. Output order is preserved.

sub preserve_order {
   my (%result_n, %result_d); my $order_id = 1;

   return sub {
      my ($chunk_id, $n, $data) = @_;

      $result_n{$chunk_id} = $n;
      $result_d{$chunk_id} = $data;

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

## Configure MCE.

my $mce = MCE->new(
   max_workers => 3, gather => preserve_order
);

my $seq = {
   begin => $s_begin, end => $s_end, step => $s_step, format => $s_format
};

my $start = time;

$mce->forseq( $seq, sub {
   my ($mce, $n, $chunk_id) = @_;
   MCE->gather($chunk_id, $n, sqrt($n));
});

my $end = time;

printf {*STDERR} "\n## Compute time: %0.03f\n\n", $end - $start;

