#!/usr/bin/env perl

##
## Usage:
##    perl matmult_perl_m.pl 1024        ## Default size 512
##

use strict;
use warnings;

use Cwd qw(abs_path);
use lib abs_path . "/../../lib";

my $prog_name = $0; $prog_name =~ s{^.*[\\/]}{}g;

use Storable qw(freeze thaw);
use Time::HiRes qw(time);

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

my $mce  = configure_and_spawn_mce(8);
my $cols = $tam; my $rows = $tam;

my $a = [ ]; my $b = [ ]; my $c = [ ];
my $cnt;

$cnt = 0; for (0 .. $rows - 1) {
   $a->[$_] = freeze([ $cnt .. $cnt + $cols - 1 ]);
   $cnt += $cols;
}

## Transpose $b to a cache file
open my $fh, '>', "$tmp_dir/cache.b";

for my $col (0 .. $rows - 1) {
   my $d = [ ]; $cnt = $col;
   for my $row (0 .. $cols - 1) {
      $d->[$row] = $cnt; $cnt += $rows;
   }
   my $d_serialized = freeze($d);
   print $fh length($d_serialized), "\n", $d_serialized;
}

close $fh;

my $start = time();

$mce->run(0, {
   sequence  => { begin => 0, end => $rows - 1, step => 1 },
   user_args => { cols => $cols, rows => $rows, path_b => "$tmp_dir/cache.b" }
} );

my $end = time();

$mce->shutdown();

printf STDERR "\n## $prog_name $tam: compute time: %0.03f secs\n\n",
   $end - $start;

my $dim_1 = $tam - 1;

print "## (0,0) ", $c->[0][0], "  ($dim_1,$dim_1) ", $c->[$dim_1][$dim_1];
print "\n\n";

###############################################################################
 # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * #
###############################################################################

sub get_row_a {
   return $a->[ $_[0] ];
}

sub insert_row {
   $c->[ $_[0] ] = $_[1];
   return;
}

sub configure_and_spawn_mce {

   my $max_workers = shift || 8;

   return MCE->new(

      max_workers => $max_workers,

      user_begin  => sub {
         my ($self) = @_;
         my $buffer;

         open my $fh, '<', $self->{user_args}->{path_b};
         $self->{cache_b} = [ ];

         for my $j (0 .. $self->{user_args}->{rows} - 1) {
            read $fh, $buffer, <$fh>;
            $self->{cache_b}->[$j] = thaw $buffer;
         }

         close $fh;
      },

      user_func   => sub {
         my ($self, $i, $chunk_id) = @_;

         my $a_i = thaw(scalar $self->do('get_row_a', $i));
         my $cache_b = $self->{cache_b};
         my $result_i = [ ];

         my $rows = $self->{user_args}->{rows};
         my $cols = $self->{user_args}->{cols};

         for my $j (0 .. $rows - 1) {
            my $c_j = $cache_b->[$j];
            $result_i->[$j] = 0;
            for my $k (0 .. $cols - 1) {
               $result_i->[$j] += $a_i->[$k] * $c_j->[$k];
            }
         }

         $self->do('insert_row', $i, $result_i);

         return;
      }

   )->spawn;
}

