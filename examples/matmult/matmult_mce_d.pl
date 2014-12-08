#!/usr/bin/env perl

##
## Usage:
##    perl matmult_mce_d.pl 1024 [ n_workers ]     ## Default matrix size 512
##                                                 ## Default n_workers 8
##

use strict;
use warnings;

use Cwd 'abs_path';  ## Remove taintedness from path
use lib ($_) = (abs_path().'/../../lib') =~ /(.*)/;

my $prog_name = $0; $prog_name =~ s{^.*[\\/]}{}g;

use Time::HiRes qw(time);

use PDL;
use PDL::IO::FastRaw;
use PDL::IO::Storable;                   ## Required for passing PDL data

use MCE::Signal qw($tmp_dir -use_dev_shm);
use MCE;

my $pdl_version = sprintf("%20s", $PDL::VERSION); $pdl_version =~ s/_.*$//;
my $chk_version = sprintf("%20s", '2.4.10');

if ($^O eq 'MSWin32' && $pdl_version lt $chk_version) {
   print "This script requires PDL 2.4.10 or later for PDL::IO::FastRaw\n";
   print "to work under the Windows environment.\n";
   exit 1;
}

###############################################################################
 # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * #
###############################################################################

my $tam = @ARGV ? shift : 512;
my $n_workers = @ARGV ? shift : 8;

if ($tam !~ /^\d+$/ || $tam < 2) {
   die "error: $tam must be an integer greater than 1.\n";
}

my $cols = $tam;
my $rows = $tam;

my $step_size = ($tam > 2048) ? 24 : ($tam > 1024) ? 16 : 8;

my $mce = configure_and_spawn_mce($n_workers);

my $a = sequence $cols,$rows;
my $b = sequence $rows,$cols;
my $c = zeroes   $rows,$rows;

writefraw($b, "$tmp_dir/b");

my $start = time;

$mce->run(0, {
   sequence  => [ 0, $rows - 1, $step_size ],
   user_args => { b => "$tmp_dir/b" }
} );

my $end = time;

$mce->shutdown();

## Print results -- use same pairs to match David Mertens' output.
printf "\n## $prog_name $tam: compute time: %0.03f secs\n\n", $end - $start;

for my $pair ([0, 0], [324, 5], [42, 172], [$rows-1, $rows-1]) {
   my ($col, $row) = @$pair; $col %= $rows; $row %= $rows;
   printf "## (%d, %d): %s\n", $col, $row, $c->at($col, $row);
}

print "\n";

###############################################################################
 # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * #
###############################################################################

sub get_rows_a {

   my ($start) = @_;
   my $stop  = $start + $step_size - 1;

   $stop = $rows - 1 if ($stop >= $rows);

   return $a->slice(":,$start:$stop");
}

sub insert_rows {

   my ($seq_n, $result_chunk) = @_;

   ins(inplace($c), $result_chunk, 0, $seq_n);

   return;
}

sub configure_and_spawn_mce {

   my $n_workers = shift || 8;

   return MCE->new(

      max_workers => $n_workers,
      job_delay   => ($tam > 2048) ? 0.031 : undef,

      user_begin  => sub {
         my ($self) = @_;
         $self->{matrix_b} = mapfraw($self->{user_args}->{b});
      },

      user_func   => sub {
         my ($self, $seq_n, $chunk_id) = @_;

         my $a_chunk = $self->do('get_rows_a', $seq_n);
         my $result_chunk = $a_chunk x $self->{matrix_b};

         $self->do('insert_rows', $seq_n, $result_chunk);

         return;
      }

   )->spawn;
}

