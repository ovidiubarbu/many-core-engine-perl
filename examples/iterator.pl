#!/usr/bin/env perl
###############################################################################
## ----------------------------------------------------------------------------
## Similar to forseq.pl as far as usage goes. However, this script uses an
## iterator factory for input_data.
##
## usage: iterator.pl [ size ]
## usage: iterator.pl [ begin end [ step [ format ] ] ]
##
## e.g. : iterator.pl 10 20 0.2 4.1f
##
## For the format arg, think of passing format to sprintf (% is optional).
##
##   iterator.pl 20 30 0.2 %4.1f
##   iterator.pl 20 30 0.2  4.1f
##
###############################################################################

use strict;
use warnings;

use Cwd qw(abs_path);
use lib abs_path . "/../lib";

my $prog_name = $0; $prog_name =~ s{^.*[\\/]}{}g;

use Time::HiRes qw(time);
use MCE::Loop;

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
###############################################################################

## Provides sequence of numbers.

sub input_iterator {
   my ($n, $max, $step) = @_;

   return sub {
      return if $n > $max;

      my $current = $n;
      $n += $step;

      return $current;
   };
}

## Preserves output order.

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
## Parallelize via MCE::Loop.
##
###############################################################################

MCE::Loop::init {
   max_workers => 3, chunk_size => 1, gather => output_iterator()
};

my $start = time();

mce_loop {
   my ($self, $chunk_ref, $chunk_id) = @_;

   if (defined $s_format) {
      my $n = sprintf "%${s_format}", $_;
      MCE->gather($n, sqrt($n), $chunk_id);
   }
   else {
      MCE->gather($_, sqrt($_), $chunk_id);
   }

} input_iterator($s_begin, $s_end, $s_step);

my $end = time();

printf STDERR "\n## Compute time: %0.03f\n\n", $end - $start;

