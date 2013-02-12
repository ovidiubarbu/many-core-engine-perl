#!/usr/bin/env perl

##
## Usage:
##    perl matmult_pdl_m.pl   1024  ## Default size is 512:  $c = $a * $b
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
      my $buffer;

      open my $fh, '<', "$tmp_dir/cache.b";
      $self->{cache_b} = [ ];

      for my $j (0 .. $rows - 1) {
         read $fh, $buffer, <$fh>;
         $self->{cache_b}->[$j] = thaw $buffer;
      }

      close $fh;
   },

   user_func   => sub {
      my ($self, $i, $chunk_id) = @_;

      my $a_i = $self->do('get_row_a', $i);
      my $cache_b = $self->{cache_b};
      my $result_i = [ ];

      for my $j (0 .. $rows - 1) {
         my $c_j = $cache_b->[$j];
         $result_i->[$j] = ( $a_i * $c_j )->sum();
      }

      $self->do('insert_row', $i, pdl($result_i));

      return;
   }
);

$mce->spawn();

my $a = sequence($cols,$rows);
my $b = sequence($rows,$cols)->transpose;
my $c = zeroes   $rows,$rows;

open my $fh, '>', "$tmp_dir/cache.b";

for my $j (0 .. $rows - 1) {
   my $row_serialized = freeze $b->slice(":,($j)");
   print $fh length($row_serialized), "\n", $row_serialized;
}

close $fh;

my $start = time();

$mce->run(0);

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
   return $a->slice(":,($_[0])");
}

sub insert_row {
   ins(inplace($c), $_[1], 0, $_[0]);
   return;
}

