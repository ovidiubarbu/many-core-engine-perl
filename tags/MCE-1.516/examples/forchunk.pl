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
## usage: forchunk.pl [ size ]
##
###############################################################################

use strict;
use warnings;

use Cwd qw(abs_path);
use lib abs_path . "/../lib";

my $prog_name = $0; $prog_name =~ s{^.*[\\/]}{}g;

use Time::HiRes qw(time);
use MCE;

my $size = shift || 3000;

unless ($size =~ /\A\d+\z/) {
   print STDERR "usage: $prog_name [ size ]\n";
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

sub output_iterator {
   my %tmp; my $order_id = 1;

   return sub {
      $tmp{$_[1]} = $_[0];

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
   max_workers => 3, chunk_size => $chunk_size, gather => output_iterator()
);

## Below, $chunk_ref is a reference to an array containing the next
## $chunk_size items from @input_data.

my $start = time();

$mce->forchunk( \@input_data, sub {
   my ($mce, $chunk_ref, $chunk_id) = @_;
   my @result;

   for ( @{ $chunk_ref } ) {
      push @result, sqrt($_);
   }

   MCE->gather(\@result, $chunk_id);
});

my $end = time();

printf STDERR "\n## Compute time: %0.03f\n\n", $end - $start;

