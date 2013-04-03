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
##   perl primes1_c.pl <N> [ <max_workers> ] [ <cnt_only> ]
##
##   perl primes1_c.pl 10000 8 0   ## Display prime numbers and total count
##   perl primes1_c.pl 10000 8 1   ## Count prime numbers only
##
##   perl primes1_c.pl check 23
##   perl primes1_c.pl between 900 950 [ <max_workers> ] [ <cnt_only> ]
##
##   Exits with a status of 0 if a prime number was found, otherwise 2.

use strict;
use warnings;

use Cwd qw(abs_path);
use lib abs_path . "/../../lib";

use Time::HiRes qw(time);
use MCE;

## Parse command-line arguments

my ($FROM, $FROM_ADJ, $N, $N_ADJ, $max_workers, $cnt_only);
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

## Ensure (power of 2) for the algorithm (the starting value is critical)

$FROM_ADJ  = $FROM;
$FROM_ADJ -= 1 if ($FROM_ADJ % 2);

$N_ADJ     = ($N % 2) ? $N + 1 : $N;

###############################################################################
 # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * #
###############################################################################

## :: Parallel Sieve of Eratosthenes based on C code from Stephan Brumme
##    http://create.stephan-brumme.com/eratosthenes/
##
## :: Added logic to poll primes from an imaginary list.

use Config;

use Inline C => Config => CCFLAGS => $Config{ccflags} . ' -std=c99';
use Inline C => <<'END_C';

AV * find_primes(

      unsigned long FROM, unsigned long FROM_ADJ, unsigned long N_ADJ,
      unsigned long seq_n, unsigned long step_size, unsigned long chunk_id,
      unsigned int cnt_only
) {

   AV * ret = newAV();

   unsigned long from = seq_n;
   unsigned long to   = from + step_size; if (to > N_ADJ) to = N_ADJ;
   unsigned int  size = (to - from + 1) / 2;

   unsigned int is_prime[size];

   // Initialize

   for (unsigned int i = 0; i < size; i++)
      is_prime[i] = 1;

   // Process chunk

   for (unsigned long i = 3; i * i <= to; i += 2) {

      if (i >=   9 && i %  3 == 0) continue;   // Skip multiples of  3
      if (i >=  25 && i %  5 == 0) continue;   // Skip multiples of  5
      if (i >=  49 && i %  7 == 0) continue;   // Skip multiples of  7
      if (i >= 121 && i % 11 == 0) continue;   // Skip multiples of 11
      if (i >= 169 && i % 13 == 0) continue;   // Skip multiples of 13

      // Skip numbers before current slice

      unsigned long minJ = (from + i - 1) / i * i;
      if (minJ < i * i) minJ = i * i;

      // Start value must be odd

      if ((minJ & 1) == 0) minJ += i;

      // Find all odd non-primes

      for (unsigned long j = minJ; j <= to; j += 2 * i) {
         unsigned int index = (unsigned int) (j - from) / 2;
         is_prime[index] = 0;
      }
   }

   // Count primes only, otherwise send list of primes for this chunk

   if (cnt_only) {
      unsigned long found = 0;

      if (2 >= seq_n && 2 <= to && 2 >= FROM) found++;

      for (unsigned int i = 0; i < size; i++) {
         if (is_prime[i]) found++;
      }

      av_push(ret, newSVuv(found));
   }
   else {

      // Think of an imaginary list containing the number 2 and all odd numbers
      // beginning with 3. The chunk_id value is used to determine the starting
      // offset position. The first chunk_id has a value of 1 in MCE.
      //
      // I imagined 2 for the 1st element as 1 is neither a prime or composite.
      //
      // { 2, 3, 5, 7, 9, 11, 13, 15, 17, 19, 21, ... odd numbers ... }
      //   0, 1, 2, 3, 4,  5,  6,  7,  8,  9, 10, ... list indices ...
      //
      // Although not necessary, one can create this list using PDL in Perl.
      //
      // $imaginary_list = sequence($N/2) * 2 + 1;
      // $imaginary_list(0) .= 2;

      unsigned long offset = (chunk_id - 1) * step_size / 2 + 1;
      offset += (FROM_ADJ / 2) - 1;

      if (2 >= seq_n && 2 <= to && 2 >= FROM) av_push(ret, newSVuv(2));

      // Append prime numbers from the imaginary list

      for (unsigned int i = 0; i < size; i++) {
         if (is_prime[i]) {
            av_push(ret, newSVuv((offset + i) * 2 + 1));
         }
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

## Step size must be a power of 2. Do not increase beyond the maximum below.

my $step_size = 128 * 1024;

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
   sequence    => [ $FROM_ADJ, $N_ADJ, $step_size ],

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

      my $p = find_primes(
         $FROM, $FROM_ADJ, $N_ADJ, $seq_n, $step_size, $chunk_id, $cnt_only
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

if ($check_flag == 0) {
   my $start = time();

   $mce->run;

   print  STDERR "\n## There are $total prime numbers between $FROM and $N.\n";
   printf STDERR "## Compute time: %0.03f secs\n\n", time() - $start;
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

   $mce->run if ($is_composite == 0);

   if ($total > 0) {
      print "$N is a prime number\n";
   } else {
      print "$N is NOT a prime number\n";
   }
}

## Exits with a status of 0 if a prime number was found, otherwise 2

exit ( ($total > 0) ? 0 : 2 );

