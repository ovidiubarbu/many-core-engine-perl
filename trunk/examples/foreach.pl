#!/usr/bin/env perl
###############################################################################
## ----------------------------------------------------------------------------
## This example demonstrates the sqrt example from Parallel::Loops.
## Parallel::Loops utilizes Parallel::ForkManager.
##
## Tested on Mac OS X 10.9.5; with Perl 5.16.2; 2.6 GHz Core i7;
## 1600 MHz RAM. The number indicates the size of input displayed
## in 1 second. Output was directed to >/dev/null during testing.
##
## Parallel::Loops:        800  Forking each @input is expensive
## MCE->foreach...:     70,000  Workers persist between each @input
## MCE->forseq....:    200,000  Uses sequence of numbers as input
## MCE->forchunk..:  1,000,000  IPC overhead is greatly reduced
##
## usage: foreach.pl [ size ]
##
###############################################################################

use strict;
use warnings;

use Cwd 'abs_path'; ## Insert lib-path at the head of @INC.
use lib abs_path($0 =~ m{^(.*)[\\/]} && $1 || abs_path) . '/../lib';

use Time::HiRes qw(time);
use MCE;

my $prog_name = $0; $prog_name =~ s{^.*[\\/]}{}g;
my $size = shift || 3000;

unless ($size =~ /\A\d+\z/) {
   print {*STDERR} "usage: $prog_name [ size ]\n";
   exit;
}

my @input_data = (0 .. $size - 1);

###############################################################################
## ----------------------------------------------------------------------------
## Parallelize via MCE's foreach method.
##
###############################################################################

## Make an output iterator for gather. Output order is preserved.

sub preserve_order {
   my %tmp; my $order_id = 1;

   return sub {
      my ($chunk_id, $data) = @_;
      $tmp{$chunk_id} = $data;

      while (1) {
         last unless exists $tmp{$order_id};

         printf "n: %d sqrt(n): %f\n",
            $input_data[$order_id - 1], $tmp{$order_id};

         delete $tmp{$order_id++};
      }

      return;
   };
}

## Configure MCE.

my $mce = MCE->new(
   max_workers => 3, gather => preserve_order
);

## Use $chunk_ref->[0] or $_ to retrieve the single element.

my $start = time;

$mce->foreach( \@input_data, sub {
   my ($mce, $chunk_ref, $chunk_id) = @_;
   MCE->gather($chunk_id, sqrt($chunk_ref->[0]));
});

my $end = time;

printf {*STDERR} "\n## Compute time: %0.03f\n\n", $end - $start;

