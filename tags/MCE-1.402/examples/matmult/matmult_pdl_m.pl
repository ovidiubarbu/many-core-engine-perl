#!/usr/bin/env perl

##
## Usage:
##    perl matmult_pdl_m.pl 1024  ## Default size is 512:  $c = $a x $b
##

use strict;
use warnings;

use FindBin;
use lib "$FindBin::Bin/../../lib";

my $prog_name = $0; $prog_name =~ s{^.*[\\/]}{}g;

use Storable qw(freeze thaw);
use Time::HiRes qw(time);

use PDL;
use PDL::IO::Storable;                   ## Required for PDL + MCE combo

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

my $max_workers = 8;
my $step_size   = 10;

my $mce = configure_and_spawn_mce($max_workers);

my $cols = $tam;
my $rows = $tam;

my $a = sequence $cols,$rows;
my $b = sequence $rows,$cols;
my $c = zeroes   $rows,$rows;

open my $fh, '>', "$tmp_dir/cache.b";

for my $j (0 .. $cols - 1) {
   my $row_serialized = freeze $b->slice(":,($j)");
   print $fh length($row_serialized), "\n", $row_serialized;
}

close $fh;

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

sub get_row_a {

   my $start = $_[0];
   my $stop  = $start + $step_size - 1;

   $stop = $rows - 1 if ($stop >= $rows);

   return $a->slice(":,$start:$stop");
}

sub insert_row {

   ins(inplace($c), $_[1], 0, $_[0]);

   return;
}

sub configure_and_spawn_mce {

   my $max_workers = shift || 8;

   return MCE->new(

      max_workers => $max_workers,
      job_delay   => ($tam > 2048) ? 0.043 : undef,

      user_begin  => sub {
         my ($self) = @_;
         my $buffer;

         my $cols = $self->{user_args}->{cols};
         my $rows = $self->{user_args}->{rows};
         my $b    = zeros $rows,$cols;

         open my $fh, '<', $self->{user_args}->{path_b};
         use PDL::NiceSlice;

         for my $j (0 .. $self->{user_args}->{cols} - 1) {
            read $fh, $buffer, <$fh>;
            $b(:,$j) .= thaw $buffer;
         }

         no PDL::NiceSlice;
         close $fh;

         $self->{matrix_b} = $b;
      },

      user_func   => sub {
         my ($self, $i, $chunk_id) = @_;

         my $a_i = $self->do('get_row_a', $i);
         my $result_i = $a_i x $self->{matrix_b};

         $self->do('insert_row', $i, $result_i);

         return;
      }

   )->spawn;
}

