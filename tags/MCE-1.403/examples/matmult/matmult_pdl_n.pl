#!/usr/bin/env perl

##
## Usage:
##    perl matmult_pdl_n.pl 1024         ## Default size 512
##

use strict;
use warnings;

use Cwd qw(abs_path);
use lib abs_path . "/../../lib";

my $prog_name = $0; $prog_name =~ s{^.*[\\/]}{}g;

use Time::HiRes qw(time);

use PDL;
use PDL::IO::Storable;                   ## Required for PDL + MCE combo
use PDL::IO::FastRaw;                    ## Required for MMAP IO

use MCE::Signal qw($tmp_dir -use_dev_shm);
use MCE;

my $pdl_version = sprintf("%20s", $PDL::VERSION); $pdl_version =~ s/_.*$//;
my $chk_version = sprintf("%20s", '2.4.11');

if ($^O eq 'MSWin32' && $pdl_version lt $chk_version) {
   print "This script requires PDL 2.4.11 or later for PDL::IO::FastRaw\n";
   print "to work using MMAP IO under the Windows environment.\n";
   exit 1;
}

###############################################################################
 # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * #
###############################################################################

my $tam = shift;
   $tam = 512 unless (defined $tam);

unless ($tam > 1) {
   print STDERR "Error: $tam must be an integer greater than 1. Exiting.\n";
   exit 1;
}

my $step_size   = 32;
my $max_workers =  8;

my $mce = configure_and_spawn_mce($max_workers);

my $cols = $tam;
my $rows = $tam;

my $a = sequence $cols,$rows;
my $b = sequence $rows,$cols;
my $c = zeroes   $rows,$rows;

writefraw($b, "$tmp_dir/cache.b");

my $start = time();

$mce->run(0, {
   sequence  => { begin => 0, end => $rows - 1, step => $step_size },
   user_args => { cols => $cols, rows => $rows, path_b => "$tmp_dir/cache.b" }
} );

my $end = time();

$mce->shutdown();

printf STDERR "\n## $prog_name $tam: compute time: %0.03f secs\n\n",
   $end - $start;

my $dim_1 = $tam - 1;

print "## (0,0) ", $c->at(0,0), "  ($dim_1,$dim_1) ", $c->at($dim_1,$dim_1);
print "\n\n";

###############################################################################
 # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * #
###############################################################################

sub get_rows_a {

   my $start = $_[0];
   my $stop  = $start + $step_size - 1;

   $stop = $rows - 1 if ($stop >= $rows);

   return $a->slice(":,$start:$stop");
}

sub insert_rows {

   ins(inplace($c), $_[1], 0, $_[0]);

   return;
}

sub configure_and_spawn_mce {

   my $max_workers = shift || 8;

   return MCE->new(

      max_workers => $max_workers,
      job_delay   => ($tam > 2048) ? 0.031 : undef,

      user_begin  => sub {
         my ($self) = @_;

         $self->{matrix_b} = mapfraw(
            $self->{user_args}->{path_b}, { ReadOnly => 1 }
         );
      },

      user_func   => sub {
         my ($self, $seq_n, $chunk_id) = @_;

         my $a_i = $self->do('get_rows_a', $seq_n);
         my $result_i = $a_i x $self->{matrix_b};

         $self->do('insert_rows', $seq_n, $result_i);

         return;
      }

   )->spawn;
}

