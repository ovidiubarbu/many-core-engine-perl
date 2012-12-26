#!/usr/bin/env perl
###############################################################################
## ----------------------------------------------------------------------------
## This example demonstrates the sqrt example from Parallel::Loops, with MCE.
## MCE does not fork a new child process for each @input_data.
##
## The number below indicates the size of @input_data which can be submitted
## and displayed in 1 second. Output was directed to /dev/null during testing.
##
## Parallel::Loops..:       600  Forking each @input is expensive
## MCE foreach......:    18,000  Sends result after each @input
## MCE forchunk.....:   385,000  Chunking reduces overhead even more so
##
## usage: foreach.pl [size]
##
###############################################################################

use strict;
use warnings;

use FindBin;
use lib "$FindBin::Bin/../lib";

my $prog_name = $0; $prog_name =~ s{^.*[\\/]}{}g;

use Time::HiRes qw(time);
use MCE;

my $size = shift || 3000;

###############################################################################
## ----------------------------------------------------------------------------
## Parallelize via MCE's foreach method.
##
###############################################################################

my @input_data = (0 .. $size - 1);
my (%result, $start, $end);

my $max_workers = 3;
my $order_id    = 1;

## Callback function for displaying results. Output order is preserved.

sub display_result {

   my ($wk_result, $chunk_id) = @_;
   $result{$chunk_id} = $wk_result;

   while (1) {
      last unless exists $result{$order_id};

      printf "i: %d sqrt(i): %f\n",
         $input_data[$order_id - 1], $result{$order_id};

      delete $result{$order_id};
      $order_id++;
   }
}

## Compute via MCE.

my $mce = MCE->new(
   max_workers => $max_workers
);

$start = time();

## Worker calls code block passing a reference to an array containing
## one item. Use $chunk_ref->[0] to retrieve the single element.

$mce->foreach(\@input_data, sub {
   my ($self, $chunk_ref, $chunk_id) = @_;
   my $wk_result = sqrt($chunk_ref->[0]);
   $self->do('display_result', $wk_result, $chunk_id);
});

$end = time();

printf STDERR "\n## Compute time: %0.03f\n\n",  $end - $start;

