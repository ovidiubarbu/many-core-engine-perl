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

use Time::HiRes qw(time);

use PDL;
use PDL::IO::FastRaw;                    ## Required for MMAP IO
use PDL::IO::Storable;                   ## Required for PDL + MCE combo

use MCE::Signal qw($tmp_dir -use_dev_shm);
use MCE;

if ($^O eq 'MSWin32') {
   print "PDL + MCE does not run reliably under Windows. Exiting.\n";
   print "Not sure what the problem is at the moment.\n";
   exit;
}  

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
      $self->{matrix_b} = mapfraw("$tmp_dir/matrix_b.raw", { ReadOnly => 1 });
   },

   user_func   => sub {
      my ($self, $i, $chunk_id) = @_;

      my $a_i = $self->do('get_row_a', $i);
      my $matrix_b = $self->{matrix_b};
      my $result_i = [ ];

      for my $j (0 .. $rows - 1) {
         my $c_j = $matrix_b->slice(":,($j)");
         $result_i->[$j] = ( $a_i * $c_j )->sum();
      }

      $self->do('insert_row', $i, pdl($result_i));

      return;
   }
);

$mce->spawn();

my $a = sequence $cols,$rows;
my $b = sequence $rows,$cols;
my $c = zeroes   $rows,$rows;

writefraw($b->transpose, "$tmp_dir/matrix_b.raw");

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

