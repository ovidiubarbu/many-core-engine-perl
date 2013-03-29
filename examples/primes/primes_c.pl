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
## This implementation utilizes Inline C code for the algorithm.
##
## Usage:
##   perl primes_c.pl <N> <max_workers> <cnt_only>
##
##   perl primes_c.pl 10000 8 0   ## Display prime numbers and total count
##   perl primes_c.pl 10000 8 1   ## Count prime numbers only
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

if ($N !~ /^\d+$/ || $N < 2) {
   die "error: $N must be an integer greater than 1.\n";
}
if ($max_workers !~ /^\d+$/ || $max_workers < 1) {
   die "error: $max_workers must be an integer greater than 0.\n";
}

###############################################################################
 # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * #
###############################################################################

use Config;

use Inline C => Config => CCFLAGS => $Config{ccflags} . ' -std=c99';
use Inline C => <<'END_C';

//
// Parallel Sieve of Eratosthenes based on C code from Stephan Brumme at
// http://create.stephan-brumme.com/eratosthenes/.
//
// Added logic to extract primes from an imaginary list.
//

AV * find_primes(
      unsigned long N, unsigned long seq_n, unsigned int step_size,
      unsigned int chunk_id, unsigned int cnt_only
) {

   AV * ret = newAV();

   unsigned long from = seq_n;
   unsigned long to   = from + step_size; if (to > N) to = N;
   unsigned int  size = (to - from + 1) / 2;

   unsigned int is_prime[size];

   // Initialize
   for (unsigned int i = 0; i < size; i++)
      is_prime[i] = 1;

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

   // Count primes only, otherwise send back a list of primes for this chunk
   if (cnt_only) {
      unsigned int found = 0; if (from <= 2) found++;

      for (unsigned int i = 0; i < size; i++) {
         if (is_prime[i]) found++;
      }

      av_push(ret, newSVuv(found));
   }
   else {
      //
      // Think of an imaginary list containing the number 2 and all odd numbers
      // beginning with 3. The chunk_id value is used to determine the starting
      // offset position. The first chunk_id has a value of 1 in MCE.
      //
      // I imagined 2 for the 1st element as 1 is neither a prime or composite.
      //
      // ( 2, 3, 5, 7, 9, 11, 13, 15, 17, 19, 21, ... odd numbers ... )
      //   0, 1, 2, 3, 4,  5,  6,  7,  8,  9, 10, ... list indices ...
      //
      // Although not necessary, one can create this list using PDL in Perl.
      //
      // $imaginary_list = sequence($N/2) * 2 + 1;
      // $imaginary_list(0) .= 2;
      //

      unsigned long offset = (chunk_id - 1) * step_size / 2 + 1;

      if (from <= 2) av_push(ret, newSVuv(2));

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

my $step_size = 128 * 1024;
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
   sequence    => [2, $N, $step_size],

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

      my $p = find_primes($N, $seq_n, $step_size, $chunk_id, $cnt_only);

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

