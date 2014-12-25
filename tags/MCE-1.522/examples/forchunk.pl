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
## usage: forchunk.pl [ size ]
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

my $chunk_size = 2500;

sub preserve_order {
   my %tmp; my $order_id = 1;

   return sub {
      my ($chunk_id, $data_ref) = @_;
      $tmp{$chunk_id} = $data_ref;

      while (1) {
         last unless exists $tmp{$order_id};
         my $i = ($order_id - 1) * $chunk_size;

         for ( @{ $tmp{$order_id} } ) {
            printf "n: %d sqrt(n): %f\n", $input_data[$i++], $_;
         }

         delete $tmp{$order_id++};
      }

      return;
   };
}

## Configure MCE.

my $mce = MCE->new(
   max_workers => 3, chunk_size => $chunk_size, gather => preserve_order
);

## Below, $chunk_ref is a reference to an array containing the next
## $chunk_size items from @input_data.

my $start = time;

$mce->forchunk( \@input_data, sub {
   my ($mce, $chunk_ref, $chunk_id) = @_;
   my @result;

   for ( @{ $chunk_ref } ) {
      push @result, sqrt($_);
   }

   MCE->gather($chunk_id, \@result);
});

my $end = time;

printf {*STDERR} "\n## Compute time: %0.03f\n\n", $end - $start;

