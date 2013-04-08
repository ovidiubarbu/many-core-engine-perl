#!/usr/bin/env perl
###############################################################################
 # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * #
###############################################################################

## Prime generating script utilizing MCE for parallel processing.
##
## This script requires very little memory no matter how big N is. This is
## made possible by polling primes from an imaginary list, described below.
## However, listing primes between 2 and 25 billion and directed to a file
## requires 11 gigabytes on disk.
##
## This implementation utilizes Inline C code for the algorithm.
##
## Usage:
##   perl primes3_c.pl <N> [ <run_mode> ] [ <max_workers> ]
##
##   perl primes3_c.pl 10000 1 8   ## Count primes only, 8 workers
##   perl primes3_c.pl 10000 2 8   ## Display primes and count, 8 workers
##   perl primes3_c.pl 10000 3 8   ## Find the sum of all the primes, 8 workers
##
##   perl primes3_c.pl check 23
##   perl primes3_c.pl between 900 950 [ <run_mode> ] [ <max_workers> ]
##
##   Exits with a status of 0 if a prime number was found, otherwise 2.

use strict;
use warnings;

use Cwd qw(abs_path);
use lib abs_path . "/../../lib";

use Time::HiRes qw(time);
use MCE;

## Parse command-line arguments

my ($FROM, $FROM_ADJ, $N, $N_ADJ, $max_workers, $run_mode);
my $check_flag = 0;

if (@ARGV && ($ARGV[0] eq '-check' || $ARGV[0] eq 'check')) {
   shift;  $check_flag = 1;

   $N    = @ARGV ? shift : 2;                    ## Default 2
   $FROM = $N;
}
elsif (@ARGV && ($ARGV[0] eq '-between' || $ARGV[0] eq 'between')) {
   shift;

   $FROM = @ARGV ? shift : 2;                    ## Default 2
   $N    = @ARGV ? shift : $FROM + 1000;         ## Default $FROM + 1000

   die "FROM: $FROM must be a number greater than 1.\n"
      if ($FROM !~ /^\d+$/ || $FROM < 2);

   die "FROM: 9223372036854775807 is the maximum allowed.\n"
      if ($FROM > 9223372036854775807);
}
else {
   $FROM = 2;
   $N    = @ARGV ? shift : 1000;                 ## Default 1000
}

$run_mode    = @ARGV ? shift : 1;                ## Default 1
$max_workers = @ARGV ? shift : 8;                ## Default 8

## Inline C (64-bit) is failing when declaring (unsigned long long) for type
## declaration. The maximum allowed is (signed long long) for now.

die "N: $N must be a number equal_to or greater than $FROM.\n"
   if ($N !~ /^\d+$/ || $N < $FROM);

die "N: 9223372036854775807 is the maximum allowed.\n"
   if ($N > 9223372036854775807);

die "run_mode: $run_mode must be either 1 = count, 2 = list, 3 = sum.\n"
   if ($run_mode !~ /^[123]$/);

die "max_workers: $max_workers must be a number greater than 0.\n"
   if ($max_workers !~ /^\d+$/ || $max_workers < 1);

## Ensure divisible by 3 for the algorithm (starting value is critical)

$FROM_ADJ  = $FROM - 3;
$FROM_ADJ  = $FROM_ADJ - ($FROM_ADJ % 3) if ($FROM_ADJ % 3);
$FROM_ADJ  = 1 if ($FROM_ADJ < 1);

$FROM_ADJ += 1 if ($FROM_ADJ % 2 == 0);

$N_ADJ     = ($N % 2) ? $N + 1 : $N;

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
#include <stdlib.h>

AV * practical_sieve(
      unsigned long FROM, unsigned long FROM_ADJ, unsigned long N_ADJ,
      unsigned long seq_n, unsigned long chunk_id, unsigned int run_mode,
      unsigned long step_size, unsigned long big_step
) {

   AV * ret = newAV();

   unsigned long seq_limit  = seq_n + big_step;
   unsigned long s_offset   = 0;
   unsigned int  break_flag = 0;

   while (seq_n + step_size <= seq_limit) {
      unsigned long to = seq_n + step_size - 1;

      if (to > N_ADJ) {
         break_flag = 1;
         to = N_ADJ;
      }

      unsigned int  k = 1, t = 2, ij;
      unsigned int  q = sqrt(to) / 3;
      unsigned long n_offset, j_offset, M = to / 3, c = 0, j;
      unsigned int  size, *is_prime, *p, i;

      size     = (to - seq_n) / 3;
      n_offset = (chunk_id - 1) * big_step + s_offset + (FROM_ADJ - 1);
      j_offset = n_offset / 3;

      // Initialize

      is_prime = (unsigned int *) malloc(sizeof(is_prime) * (size + 1));
      *(is_prime) = 0;

      p = is_prime + 1; i = 0;
      while (i++ <= size) *p++ = 1;
      p = is_prime;

      // Clear out values less than FROM

      if (chunk_id == 1) {
         for (unsigned int i = 1; i <= size; i += 2) {
            if (n_offset + (3 * i + 2) >= FROM)
               break;
            *(p + i) = 0;
            if (n_offset + (3 * (i + 1) + 1) >= FROM)
               break;
            *(p + i + 1) = 0;
         }
      }

      // Clear out values greater than N_ADJ

      if (to == N_ADJ) {
         if (n_offset + (3 * (size + 1) + 1) > N_ADJ)
            *(p + size + 1) = 0;
         if (n_offset + (3 *  size + 2) > N_ADJ)
            *(p + size) = 0;
      }

      // Process chunk

      for (unsigned int i = 1; i <= q; i++) {
         k  = 3 - k;  c = 4 * k * i + c;  j = c;
         ij = 2 * i * (3 - k) + 1;  t = 4 * k + t;

         // Skip numbers before current chunk

         if (j < j_offset) {
            j += (j_offset - j) / t * t + ij;
            ij = t - ij;
            if (j < j_offset) {
               j += ij;  ij = t - ij;
            }
         }

         // Clear out composites

         while (j <= M) {
            unsigned int index = (unsigned int) j - j_offset;
            *(p + index) = 0;
            j += ij;  ij = t - ij;
         }
      }

      // Count primes, sum primes, otherwise send list of primes for this chunk

      if (run_mode == 1) {
         unsigned long found = 0;

         if (2 >= seq_n && 2 <= to && 2 >= FROM)
            found++;
         if (3 >= seq_n && 3 <= to && 3 >= FROM)
            found++;

         p = is_prime + 1; i = 0;

         while (i++ <= size) {
            if (*p++)
               found++;
         }

         av_push(ret, newSVuv(found));
      }
      else if (run_mode == 3) {
         unsigned long sum = 0;

         if (2 >= seq_n && 2 <= to && 2 >= FROM)
            sum += 2;
         if (3 >= seq_n && 3 <= to && 3 >= FROM)
            sum += 3;

         for (unsigned int i = 1; i <= size; i += 2) {
            if (*(p + i))
               sum += n_offset + (3 * i + 2);
            if (*(p + i + 1))
               sum += n_offset + (3 * (i + 1) + 1);
         }

         av_push(ret, newSVuv(sum));
      }
      else {

         // Think of an imaginary list containing sequence of numbers. The
         // n_offset value is used to determine the starting offset position.
         //
         // Avoid all composites that have 2 or 3 as one of their prime factors
         // (where i is odd).
         //
         // { 0, 5, 7, 11, 13, ... 3i + 2, 3(i + 1) + 1, ..., N }
         //   0, 1, 2,  3,  4, ... list indices (0 is not used)

         if (2 >= seq_n && 2 <= to && 2 >= FROM)
            av_push(ret, newSVuv(2));
         if (3 >= seq_n && 3 <= to && 3 >= FROM)
            av_push(ret, newSVuv(3));

         for (unsigned int i = 1; i <= size; i += 2) {
            if (*(p + i))
               av_push(ret, newSVuv(n_offset + (3 * i + 2)));
            if (*(p + i + 1))
               av_push(ret, newSVuv(n_offset + (3 * (i + 1) + 1)));
         }
      }

      free (is_prime);
      is_prime = NULL;

      if (break_flag)
         break;

      seq_n    += step_size;
      s_offset += step_size;
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

   my $number = $_[0];

   $total += $number;

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

## Step size must be divisible by 3. Do not increase beyond the maximum below.

my $step_size = 27 * 15000;

$step_size += $step_size if ($N >= 1_000_000_000_000);           ## step   2x
$step_size += $step_size if ($N >= 10_000_000_000_000);          ## step   4x
$step_size += $step_size if ($N >= 100_000_000_000_000);         ## step   8x
$step_size += $step_size if ($N >= 1_000_000_000_000_000);       ## step  16x
$step_size += $step_size if ($N >= 10_000_000_000_000_000);      ## step  32x
$step_size += $step_size if ($N >= 100_000_000_000_000_000);     ## step  64x
$step_size += $step_size if ($N >= 1_000_000_000_000_000_000);   ## step 128x

my $big_step = $step_size * int(
   ($N_ADJ - $FROM_ADJ) / $step_size / $max_workers / 128 + 1
);

## MCE follows a bank-teller queuing model when distributing the sequence of
## numbers at big_step size to workers. User_func is called once per each
## step. Both user_begin and user_end are called once per worker.
##
##    <user_begin> <user_func> <user_func> ... <user_func> <user_end>

my $mce = MCE->new(
   max_workers => (($FROM == $N) ? 1 : $max_workers),
   sequence    => [ $FROM_ADJ, $N_ADJ, $big_step ],

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
         $FROM, $FROM_ADJ, $N_ADJ, $seq_n, $chunk_id, $run_mode,
         $step_size, $big_step
      );

      if ($run_mode == 1 || $run_mode == 3) {
         foreach (@$p) {
            $self->{total} += $_;
         }
      }
      else {
         $self->{total} += scalar(@$p);
         $self->do("display_primes", join("\n", @$p)."\n", $chunk_id);
      }
   }
);

###############################################################################
 # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * #
###############################################################################

if ($check_flag == 0) {
   my $start = time();

   $mce->run;

   if ($run_mode == 3) {
      print STDERR
         "\n## The sum of all the primes between $FROM and $N is $total.\n";
   }
   else {
      print STDERR
         "\n## There are $total primes between $FROM and $N.\n";
   }

   printf STDERR
      "## Compute time: %0.03f secs\n\n", time() - $start;
}
else {
   my $is_composite = 0;

   for my $prime ( qw(
      2 3 5 7 11 13 17 19 23 29 31 37 41 43 47 53 59 61 67 71 73 79 83 89 97
   ) ) {
      if ($N > $prime && $N % $prime == 0) {
         $is_composite = 1;
         last;
      }
   }

   $mce->run unless $is_composite;

   if ($total > 0) {
      print "$N is a prime number\n";
   }
   else {
      print "$N is NOT a prime number\n";
   }
}

## Exits with a status of 0 if a prime number was found, otherwise 2

exit ( ($total > 0) ? 0 : 2 );

