#!/usr/bin/env perl

##
## This script counts the number of primes between 2 and N. Specify 0 for the
## 3rd argument to output primes to STDOUT while running.
##
## Due to the chunking nature of the application, this script requires very
## little memory no matter how big N is. This is made possible by extracting
## primes from an imaginary list, described below. However, displaying primes
## between 2 and 25 billion and directed to a file requires 11 gigabytes
## on disk.
##
## This implementation utilizes 100% Perl code for the algorithm.
##
## Usage:
##   perl primes2_p.pl <N> <max_workers> <cnt_only>
##
##   perl primes2_p.pl 10000 8 0   ## Display prime numbers and total count
##   perl primes2_p.pl 10000 8 1   ## Count prime numbers only
##

use strict;
use warnings;

use Cwd qw(abs_path);
use lib abs_path . "/../../lib";

use Time::HiRes qw(time);

use MCE;

my $N           = @ARGV ? shift : 1000;          ## Default is 1000
my $max_workers = @ARGV ? shift :    8;          ## Default is    8
my $cnt_only    = @ARGV ? shift :    1;          ## Default is    1

if ($N !~ /^\d+$/ || $N < 2 || $N % 2) {
   die "error: $N must be an even integer greater than 1.\n";
}
if ($max_workers !~ /^\d+$/ || $max_workers < 1) {
   die "error: $max_workers must be an integer greater than 0.\n";
}

###############################################################################
 # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * #
###############################################################################

##
## Parallel sieve based on serial code from Xuedong Luo (Algorithm3).
##
##    A practical sieve algorithm for finding prime numbers
##    ACM Volume 32 Issue 3, March 1989, Pages 344-346
##    http://dl.acm.org/citation.cfm?doid=62065.62072
##
## Added logic to skip numbers before current chunk.
## Added logic to extract primes from an imaginary list.
##

sub practical_sieve {

   my ($N, $seq_n, $step_size, $chunk_id, $cnt_only) = @_;

   my @ret;

   my $from = $seq_n;
   my $to   = $from + $step_size - 1; $to = $N if ($to > $N);
   my $size = int(($to - $from) / 3);

   my ($c, $k, $t, $q, $M) = (0, 1, 2, int(sqrt($to)/3), int($to/3));
   my (@is_prime, $j, $ij, $d);

   my $n_offset = ($chunk_id - 1) * $step_size;
   my $j_offset = int($n_offset/3);

   ## Initialize
   $is_prime[0] = 0; $is_prime[$_] = 1 for (1 .. $size + 1);

   ## Clear out value if exceeds N
   $is_prime[$size + 1] = 0 if ($n_offset + (3 * ($size + 1) + 1) > $N);
   $is_prime[$size + 0] = 0 if ($n_offset + (3 * $size + 2) > $N);

   for my $i (1 .. $q) {
      $k  = 3 - $k;  $c = 4 * $k * $i + $c;  $j = $c;
      $ij = 2 * $i * (3 - $k) + 1;  $t = 4 * $k + $t;

      ## Skip numbers before current chunk
      if ($j < $j_offset) {
         $d  = int(($j_offset - $j) / ($t - $ij + $ij));
         $j += ($t - $ij + $ij) * $d if ($d > 0);

         while ($j < $j_offset) {
            $j  = $j + $ij;
            $ij = $t - $ij;
         }
      }

      ## Clear out composites
      while ($j <= $M) {
         $is_prime[$j - $j_offset] = 0;
         $j  = $j + $ij;
         $ij = $t - $ij;
      }
   }

   ## Count primes only, otherwise send back a list of primes for this chunk
   if ($cnt_only) {
      my $found = 0;

      $found++ if ($from <= 2);
      $found++ if ($from <= 3 && 3 <= $N);

      foreach (@is_prime) {
         $found++ if ($_);
      }

      push @ret, $found;
   }
   else {
      ##
      ## Think of an imaginary list containing sequence of numbers beginning
      ## with 5. The n_offset value is used to determine the starting offset
      ## position.
      ##
      ## Avoid all composites that have 2 or 3 as one of thier prime factors.
      ##
      ## { 0, 5, 7, 11, 13, ... 3i + 2, 3(i + 1) + 1, ..., N } (where i is odd)
      ##   0, 1, 2,  3,  4, ... list indices (0 is not used)
      ##

      push @ret, 2 if ($from <= 2);
      push @ret, 3 if ($from <= 3 && 3 <= $N);

      for (my $i = 1; $i <= $size; $i += 2) {
         push @ret, $n_offset + (3 * $i + 2)       if ($is_prime[ $i ]);
         push @ret, $n_offset + (3 * ($i + 1) + 1) if ($is_prime[$i+1]);
      }
   }

   return \@ret;
}

###############################################################################
 # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * #
###############################################################################

## Callback functions. These are called in a serial fashion. The cache is
## used to ensure output order when displaying prime numbers while running.

my $step_size = 18 * 15000;      ## Power of 18 recommended: (18/3/3) = 2

my $total = 0;
my $order_id = 1;
my %cache;

sub aggregate_total {

   my $found = $_[0];

   $total += $found;

   return;
}

sub display_primes {

   $cache{ $_[1] } = $_[0];

   while (1) {
      last unless (exists $cache{$order_id});

      if (length $cache{$order_id} > 1) {
         print $cache{$order_id};
      }
      delete $cache{$order_id};
      $order_id++;
   }

   return;
}

###############################################################################
 # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * #
###############################################################################

## MCE follows a bank-teller queuing model when distributing the sequence of
## numbers at step_size to workers. The user_func is called once per each step.
## Both user_begin and user_end are called once per worker for the duration of
## the run: <user_begin> <user_func> <user_func> ... <user_func> <user_end>

my $start = time();

my $mce = MCE->new(

   max_workers => $max_workers,
   sequence    => [1, $N, $step_size],

   user_begin  => sub {
      my ($self) = @_;
      $self->{total} = 0;
   },

   user_end    => sub {
      my ($self) = @_;
      $self->do('aggregate_total', $self->{total});
   },

   user_func   => sub {
      my ($self, $seq_n, $chunk_id) = @_;

      my $p = practical_sieve($N, $seq_n, $step_size, $chunk_id, $cnt_only);

      if ($cnt_only) {
         $self->{total} += $p->[0];
      } else {
         $self->{total} += scalar(@$p);
         $self->do("display_primes", join("\n", @$p)."\n", $chunk_id);
      }
   }

)->run;

my $end = time();

print  STDERR "\n## There are $total prime numbers between 2 and $N.\n";
printf STDERR "## Compute time: %0.03f secs\n\n", $end - $start;

