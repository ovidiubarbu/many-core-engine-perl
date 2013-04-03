#!/usr/bin/env perl
###############################################################################
 # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * #
###############################################################################

## Prime generating script utilizing MCE for parallel processing.
##
## Due to the chunking nature of the application, this script requires very
## little memory no matter how big N is. This is made possible by polling
## primes from an imaginary list, described below. However, displaying primes
## between 2 and 25 billion and directed to a file requires 11 gigabytes
## on disk.
##
## This implementation utilizes Inline C code for the algorithm.
##
## Usage:
##   perl primes2_c.pl <N> [ <max_workers> ] [ <cnt_only> ]
##
##   perl primes2_c.pl 10000 8 0   ## Display prime numbers and total count
##   perl primes2_c.pl 10000 8 1   ## Count prime numbers only
##
##   perl primes2_c.pl check 23
##   perl primes2_c.pl between 900 950 [ <max_workers> ] [ <cnt_only> ]
##
##   Exit with a status of 0 if prime number(s) were found, otherwise 2.

use strict;
use warnings;

use Cwd qw(abs_path);
use lib abs_path . "/../../lib";

use Time::HiRes qw(time);
use MCE;

## Parse command-line arguments

my ($FROM, $ADJ_FROM, $N, $ADJ_N, $max_workers, $cnt_only);

if (@ARGV && ($ARGV[0] eq '-check' || $ARGV[0] eq 'check')) {
   shift;
   $N    = @ARGV ? shift : 2;                    ## Default 2
   $FROM = $N;
}
elsif (@ARGV && ($ARGV[0] eq '-between' || $ARGV[0] eq 'between')) {
   shift;
   $FROM = @ARGV ? shift : 2;                    ## Default 2
   $N    = @ARGV ? shift : 1000;                 ## Default 1000

   die "FROM: $FROM must be a number greater than 1.\n"
      if ($FROM !~ /^\d+$/ || $FROM < 2);

   die "FROM: 9223372036854775807 is the maximum allowed.\n"
      if ($FROM > 9223372036854775807);
}
else {
   $FROM = 2;
   $N    = @ARGV ? shift : 1000;
}

$max_workers = @ARGV ? shift : 8;                ## Default 8
$cnt_only    = @ARGV ? shift : 1;                ## Default 1

## Inline C (64-bit) if failing when declaring (unsigned long long) for the
## function variable types. Therefore, the maximum allowed is signed long long.

die "N: $N must be a number equal_to or greater than $FROM.\n"
   if ($N !~ /^\d+$/ || $N < $FROM);

die "N: 9223372036854775807 is the maximum allowed.\n"
   if ($N > 9223372036854775807);

die "max_workers: $max_workers must be a number greater than 0.\n"
   if ($max_workers !~ /^\d+$/ || $max_workers < 1);

die "cnt_only: $cnt_only must be either 0 or 1.\n"
   if ($cnt_only !~ /^[01]$/);

## Ensure (power of 18) for the algorithm (the starting value is critical)

$ADJ_FROM  = $FROM - 18;
$ADJ_FROM  = $ADJ_FROM - ($ADJ_FROM % 18) if ($ADJ_FROM % 18);
$ADJ_FROM  = 1 if ($ADJ_FROM < 1);

$ADJ_FROM += 1 if ($ADJ_FROM % 2 == 0);

$ADJ_N     = ($N % 2) ? $N + 1 : $N;

###############################################################################
 # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * #
###############################################################################

## Parallel sieve based on serial code from Xuedong Luo (Algorithm3).
##
## :: A practical sieve algorithm for finding prime numbers
##    ACM Volume 32 Issue 3, March 1989, Pages 344-346
##    http://dl.acm.org/citation.cfm?doid=62065.62072
##
## :: Added logic to skip numbers before the current chunk.
## :: Added logic to poll primes from an imaginary list.

use Config;

use Inline C => Config => CCFLAGS => $Config{ccflags} . ' -std=c99';
use Inline C => <<'END_C';

#include <math.h>

AV * practical_sieve(

      unsigned long FROM, unsigned long ADJ_FROM, unsigned long ADJ_N,
      unsigned long seq_n, unsigned long step_size, unsigned long chunk_id,
      unsigned int cnt_only
) {

   AV * ret = newAV();

   unsigned long to   = seq_n + step_size - 1; if (to > ADJ_N) to = ADJ_N;
   unsigned int  size = (to - seq_n) / 3;

   unsigned int  k = 1, t = 2, ij;
   unsigned int  q = sqrt(to) / 3;
   unsigned long M = to / 3, c = 0, j, d;
   unsigned int  is_prime[size + 1];

   unsigned long n_offset = (chunk_id - 1) * step_size + (ADJ_FROM - 1);
   unsigned long j_offset = n_offset / 3;

   // Initialize

   is_prime[0] = 0;

   for (unsigned int i = 1; i <= size + 1; i++)
      is_prime[i] = 1;

   // Clear out values < FROM

   if (chunk_id == 1) {
      for (unsigned int i = 1; i <= size; i += 2) {
         if (n_offset + (3 * i + 2) >= FROM) break;
         is_prime[ i ] = 0;
         if (n_offset + (3 * (i + 1) + 1) >= FROM) break;
         is_prime[i+1] = 0;
      }
   }

   // Clear out values > ADJ_N

   if (to == ADJ_N) {
      if (n_offset + (3 * (size + 1) + 1) > ADJ_N) is_prime[size + 1] = 0;
      if (n_offset + (3 *  size + 2     ) > ADJ_N) is_prime[size + 0] = 0;
   }

   // Process chunk

   for (unsigned int i = 1; i <= q; i++) {
      k  = 3 - k;  c = 4 * k * i + c;  j = c;
      ij = 2 * i * (3 - k) + 1;  t = 4 * k + t;

      // Skip numbers before the current chunk

      if (j < j_offset) {
         d  = (j_offset - j) / (t - ij + ij);
         j += (t - ij + ij) * d;

         // This may loop 0, 1, or 2 times max

         while (j < j_offset) {
            j  = j + ij;
            ij = t - ij;
         }
      }

      // Clear out composites

      while (j <= M) {
         unsigned int index = (unsigned int) j - j_offset;
         is_prime[index] = 0;
         j  = j + ij;
         ij = t - ij;
      }
   }

   // Count primes only, otherwise send list of primes for this chunk

   if (cnt_only) {
      unsigned long found = 0;

      if (2 >= seq_n && 2 <= to && 2 >= FROM) found++;
      if (3 >= seq_n && 3 <= to && 3 >= FROM) found++;

      for (unsigned int i = 1; i <= size + 1; i++) {
         if (is_prime[i]) found++;
      }

      av_push(ret, newSVuv(found));
   }
   else {

      // Think of an imaginary list containing sequence of numbers. The
      // n_offset value is used to determine the starting offset position.
      //
      // Avoid all composites that have 2 or 3 as one of their prime factors.
      //
      // { 0, 5, 7, 11, 13, ... 3i + 2, 3(i + 1) + 1, ..., N } (where i is odd)
      //   0, 1, 2,  3,  4, ... list indices (0 is not used)

      if (2 >= seq_n && 2 <= to && 2 >= FROM) av_push(ret, newSVuv(2));
      if (3 >= seq_n && 3 <= to && 3 >= FROM) av_push(ret, newSVuv(3));

      for (unsigned int i = 1; i <= size; i += 2) {
         if (is_prime[ i ]) av_push(ret, newSVuv(n_offset + (3 * i + 2)));
         if (is_prime[i+1]) av_push(ret, newSVuv(n_offset + (3 * (i + 1) + 1)));
      }
   }

   return sv_2mortal(ret);
}

END_C

###############################################################################
 # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * #
###############################################################################

## Callback functions. These are called in a serial fashion. The cache is
## used to ensure output order when displaying prime numbers while running.

my $order_id = 1;
my $total = 0;
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

## Step size must be a power of 18: (18/3/3) = 2. Do not increase beyond this.

my $step_size = 18 * 15000;

$step_size += $step_size if ($FROM >= 1_000_000_000_000);        ## step  2x
$step_size += $step_size if ($FROM >= 10_000_000_000_000);       ## step  4x
$step_size += $step_size if ($FROM >= 100_000_000_000_000);      ## step  8x
$step_size += $step_size if ($FROM >= 1_000_000_000_000_000);    ## step 16x
$step_size += $step_size if ($FROM >= 10_000_000_000_000_000);   ## step 32x

## MCE follows a bank-teller queuing model when distributing the sequence of
## numbers at step_size to workers. The user_func is called once per each step.
## Both user_begin and user_end are called once per worker for the duration of
## the run: <user_begin> <user_func> <user_func> ... <user_func> <user_end>

my $mce = MCE->new(
   max_workers => (($FROM != $N) ? $max_workers : 1),
   sequence    => [ $ADJ_FROM, $ADJ_N, $step_size ],

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

      my $p = practical_sieve(
         $FROM, $ADJ_FROM, $ADJ_N, $seq_n, $step_size, $chunk_id, $cnt_only
      );

      if ($cnt_only) {
         $self->{total} += $p->[0];
      } else {
         $self->{total} += scalar(@$p);
         $self->do("display_primes", join("\n", @$p)."\n", $chunk_id);
      }
   }
);

###############################################################################
 # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * #
###############################################################################

if ($FROM != $N) {
   my $start = time();

   $mce->run;

   print  STDERR "\n## There are $total prime numbers between $FROM and $N.\n";
   printf STDERR "## Compute time: %0.03f secs\n\n", time() - $start;
}
else {
   my $is_composite = 0;

   $is_composite = 1 if ($N >  2 && $N %  2 == 0);
   $is_composite = 1 if ($N >  3 && $N %  3 == 0);
   $is_composite = 1 if ($N >  5 && $N %  5 == 0);
   $is_composite = 1 if ($N >  7 && $N %  7 == 0);
   $is_composite = 1 if ($N > 11 && $N % 11 == 0);
   $is_composite = 1 if ($N > 13 && $N % 13 == 0);

   $mce->run if ($is_composite == 0);

   if ($total > 0) {
      print "$N is a prime number\n";
   } else {
      print "$N is NOT a prime number\n";
   }
}

## Exit with a status of 0 if prime number(s) were found, otherwise 2

exit ( ($total > 0) ? 0 : 2 );

