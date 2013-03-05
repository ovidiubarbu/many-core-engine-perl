#!/usr/bin/env perl

##
## Usage:
##    perl matmult_pdl_q.pl 1024         ## Default size 512
##

use strict;
use warnings;

use Cwd qw(abs_path);
use lib abs_path . "/../../lib";

my $prog_name = $0; $prog_name =~ s{^.*[\\/]}{}g;

use Time::HiRes qw(time);

use PDL;
use PDL::Parallel::threads qw(retrieve_pdls);

use PDL::IO::Storable;                   ## Required for passing PDL data

use MCE::Signal qw($tmp_dir -use_dev_shm);
use MCE;

###############################################################################
 # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * #
###############################################################################

my $tam = shift;
   $tam = 512 unless (defined $tam);

unless ($tam > 1) {
   print STDERR "Error: $tam must be an integer greater than 1. Exiting.\n";
   exit 1;
}

my $cols = $tam;
my $rows = $tam;

my $step_size   = 16;
my $max_workers =  8;

my $mce = configure_and_spawn_mce($max_workers);

my $a = sequence $cols,$rows;
my $b = sequence $rows,$cols;
my $c = zeroes   $rows,$rows;

$a->share_as('left_input');
$b->share_as('right_input');
$c->share_as('output');

my $start = time();
$mce->process([ 0 .. $rows - 1 ], { chunk_size => $step_size });
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

sub configure_and_spawn_mce {

   my $max_workers = shift || 8;

   return MCE->new(

      max_workers => $max_workers,
    # job_delay   => ($tam > 2048) ? 0.031 : undef,

      user_begin  => sub {
         my ($self) = @_;

         ( $self->{l}, $self->{r}, $self->{o} ) = retrieve_pdls(
            'left_input', 'right_input', 'output'
         );
      },

      user_func   => sub {
         my ($self, $chunk_ref, $chunk_id) = @_;
         my $seq_n = $chunk_ref->[0];

         my $l = $self->{l};
         my $r = $self->{r};
         my $o = $self->{o};

         my $start = $seq_n;
         my $stop  = $start + $step_size - 1;

         $stop = $rows - 1 if ($stop >= $rows);

         use PDL::NiceSlice;
         $o(:,$start:$stop) .= $l(:,$start:$stop) x $r;
         no PDL::NiceSlice;

         return;
      }

   )->spawn;
}

