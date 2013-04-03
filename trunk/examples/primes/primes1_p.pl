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
## This implementation utilizes 100% Perl code for the algorithm.
##
## Usage:
##   perl primes1_p.pl <N> [ <max_workers> ] [ <cnt_only> ]
##
##   perl primes1_p.pl 10000 8 0   ## Display prime numbers and total count
##   perl primes1_p.pl 10000 8 1   ## Count prime numbers only
##
##   perl primes1_p.pl check 23
##   perl primes1_p.pl between 900 950 [ <max_workers> ] [ <cnt_only> ]
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

if (@ARGV && ($ARGV[0] eq '-check' || $ARGV[0] eq 'check')) {
   shift;
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

sub find_primes {

   my (
      $FROM, $FROM_ADJ, $N_ADJ, $seq_n, $step_size, $chunk_id, $cnt_only
   ) = @_;

   my @ret;

   my $from = $seq_n;
   my $to   = $from + $step_size; $to = $N_ADJ if ($to > $N_ADJ);
   my $size = int(($to - $from + 1) / 2);

   my (@is_prime, $minJ, $index, $i, $j);

   ## Initialize

   $is_prime[$_] = 1 for (0 .. $size - 1);

   ## Process chunk

   for ($i = 3; $i * $i <= $to; $i += 2) {

      next if ($i >=   9 && $i %  3 == 0);   ## Skip multiples of  3
      next if ($i >=  25 && $i %  5 == 0);   ## Skip multiples of  5
      next if ($i >=  49 && $i %  7 == 0);   ## Skip multiples of  7
      next if ($i >= 121 && $i % 11 == 0);   ## Skip multiples of 11
      next if ($i >= 169 && $i % 13 == 0);   ## Skip multiples of 13

      ## Skip numbers before current slice

      $minJ = int(($from + $i - 1) / $i) * $i;
      $minJ = $i * $i if ($minJ < $i * $i);

      ## Start value must be odd

      $minJ += $i if (($minJ & 1) == 0);

      ## Find all odd non-primes

      for ($j = $minJ; $j <= $to; $j += 2 * $i) {
         $index = int(($j - $from) / 2);
         $is_prime[$index] = 0;
      }
   }

   ## Count primes only, otherwise send list of primes for this chunk

   if ($cnt_only) {
      my $found = 0;

      $found++ if (2 >= $seq_n && 2 <= $to && 2 >= $FROM);

      foreach (@is_prime) {
         $found++ if ($_);
      }

      push @ret, $found;
   }
   else {

      ## Think of an imaginary list containing the number 2 and all odd numbers
      ## beginning with 3. The chunk_id value is used to determine the starting
      ## offset position. The first chunk_id has a value of 1 in MCE.
      ##
      ## I imagined 2 for the 1st element as 1 is neither a prime or composite.
      ##
      ## { 2, 3, 5, 7, 9, 11, 13, 15, 17, 19, 21, ... odd numbers ... }
      ##   0, 1, 2, 3, 4,  5,  6,  7,  8,  9, 10, ... list indices ...
      ##
      ## Although not necessary, one can create this list using PDL in Perl.
      ##
      ## $imaginary_list = sequence($N/2) * 2 + 1;
      ## $imaginary_list(0) .= 2;

      my $offset = int(($chunk_id - 1) * $step_size / 2) + 1;
      $offset += int($FROM_ADJ / 2) - 1;

      push @ret, 2 if (2 >= $seq_n && 2 <= $to && 2 >= $FROM);

      ## Append prime numbers from the imaginary list

      for ($i = 0; $i < $size; $i++) {
         push @ret, ($offset + $i) * 2 + 1 if ($is_prime[$i]);
      }
   }

   return \@ret;
}

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

my $step_size = 128 * 1024;

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

## Exits with a status of 0 if a prime number was found, otherwise 2

exit ( ($total > 0) ? 0 : 2 );

