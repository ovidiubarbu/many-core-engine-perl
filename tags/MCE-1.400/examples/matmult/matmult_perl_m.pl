#!/usr/bin/env perl

##
## Usage:
##    perl matmult_perl_m.pl  1024  ## Default size is 512:  $c = $a * $b
##

use strict;
use warnings;

use FindBin;
use lib "$FindBin::Bin/../../lib";

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
   print STDERR "Error: $tam is not 2 or greater. Exiting.\n";
   exit 1;
}

my $cols = $tam;
my $rows = $tam;

my $mce = MCE->new(

   max_workers => 8,
   sequence    => { begin => 0, end => $rows - 1, step => 1 },

   user_begin  => sub {
      my ($self) = @_;
      open $self->{cache_b}, '<', "$tmp_dir/cache.b";
   },

   user_end    => sub {
      my ($self) = @_;
      close $self->{cache_b};
   },

   user_func   => sub {
      my ($self, $i, $chunk_id) = @_;

      my $a_i = thaw(scalar $self->do('get_row_a', $i));
      my $result_i = [ ];
      my $buffer;

      my $fh = $self->{cache_b}; seek $fh, 0, 0;

      for my $j (0 .. $rows - 1) {
         read $fh, $buffer, <$fh>;
         my $c_j = thaw($buffer);
         $result_i->[$j] = 0;
         for my $k (0 .. $cols - 1) {
            $result_i->[$j] += $a_i->[$k] * $c_j->[$k];
         }
      }

      $self->do('insert_row', $i, $result_i);

      return;
   }
);

$mce->spawn();

my $a = [ ];
my $b = [ ];
my $c = [ ];
my $cnt;

$cnt = 0; for (0 .. $rows - 1) {
   $a->[$_] = freeze([ $cnt .. $cnt + $cols - 1 ]);
   $cnt += $cols;
}

## Transpose $b into a cache file

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

$mce->run(0);

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

