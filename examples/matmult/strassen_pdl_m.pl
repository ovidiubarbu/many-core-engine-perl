#!/usr/bin/env perl

##
## Usage:
##    perl strassen_pdl_m.pl 1024  ## Default size is 512:  divide-and-conquer
##

use strict;
use warnings;

use FindBin;
use lib "$FindBin::Bin/../../lib";

my $prog_name = $0; $prog_name =~ s{^.*[\\/]}{}g;

use Time::HiRes qw(time);

use PDL;
use PDL::IO::Storable;                   ## Required for PDL + MCE combo

use MCE::Signal qw(-use_dev_shm);
use MCE;

###############################################################################
 # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * #
###############################################################################

my $tam = shift;                         ## Wants power of 2 only
   $tam = 512 unless (defined $tam);

unless (is_power_of_two($tam)) {
   print STDERR "Error: $tam must be a power of 2 integer. Exiting.\n";
   exit 1;
}

my $a = sequence $tam,$tam;
my $b = sequence $tam,$tam;
my $c = zeroes   $tam,$tam;

my $max_parallel_level = 1;              ## Levels deep to parallelize
my @p = ( );                             ## For MCE results - must be global

my $start = time();

strassen($a, $b, $c, $tam);              ## Start matrix multiplication

my $end = time();

printf STDERR "\n## $prog_name $tam: compute time: %0.03f secs\n\n",
   $end - $start;

my $dim_1 = $tam - 1;

print "## (0,0) ", $c->at(0,0), "  ($dim_1,$dim_1) ", $c->at($dim_1,$dim_1);
print "\n\n";

###############################################################################
 # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * #
###############################################################################

sub is_power_of_two {

   my $n = $_[0];

   return ($n != 0 && (($n & $n - 1) == 0));
}

sub strassen {

   my $a = $_[0]; my $b = $_[1]; my $c = $_[2]; my $tam = $_[3];
   my $level = $_[4] || 0;

   ## Perform the classic multiplication when matrix is <= 128 X 128

   if ($tam <= 128) {

      ins(inplace($c), $a x $b);

      return;
   }

   ## Otherwise, perform multiplication using Strassen's algorithm

   my ($mce, $p1, $p2, $p3, $p4, $p5, $p6, $p7);

   my $nTam = $tam / 2;

   if (++$level <= $max_parallel_level) {

      ## Configure and spawn MCE workers early

      sub store_result {
         my ($n, $result) = @_;
         $p[$n] = $result;
      }

      $mce = MCE->new(
         max_workers => 7,
         user_tasks => [{
            user_func => sub {
               my $self = $_[0];
               my $data = $self->{user_data};
               my $result = zeroes $nTam,$nTam;
               strassen($data->[0], $data->[1], $result, $data->[3], $level);
               $self->do('store_result', $data->[2], $result);
            },
            task_end => sub {
               $p1 = $p[1]; $p2 = $p[2]; $p3 = $p[3]; $p4 = $p[4];
               $p5 = $p[5]; $p6 = $p[6]; $p7 = $p[7];
               @p  = ( );
            }
         }]
      );

      $mce->spawn();
   }

   ## Allocate memory after spawning MCE workers

   my $a11 = zeroes $nTam,$nTam;  my $a12 = zeroes $nTam,$nTam;
   my $a21 = zeroes $nTam,$nTam;  my $a22 = zeroes $nTam,$nTam;
   my $b11 = zeroes $nTam,$nTam;  my $b12 = zeroes $nTam,$nTam;
   my $b21 = zeroes $nTam,$nTam;  my $b22 = zeroes $nTam,$nTam;

   my $t1  = zeroes $nTam,$nTam;  my $t2  = zeroes $nTam,$nTam;

      $p1  = zeroes $nTam,$nTam;     $p2  = zeroes $nTam,$nTam;
      $p3  = zeroes $nTam,$nTam;     $p4  = zeroes $nTam,$nTam;
      $p5  = zeroes $nTam,$nTam;     $p6  = zeroes $nTam,$nTam;
      $p7  = zeroes $nTam,$nTam;

   ## Divide the matrices into 4 sub-matrices

   divide_m($a11, $a12, $a21, $a22, $a, $nTam);
   divide_m($b11, $b12, $b21, $b22, $b, $nTam);

   ## Calculate p1 to p7

   if ($level <= $max_parallel_level) {
      sum_m($a11, $a22, $t1, $nTam);
      sum_m($b11, $b22, $t2, $nTam);
      $mce->send([ $t1, $t2, 1, $nTam ]);

      sum_m($a21, $a22, $t1, $nTam);
      $mce->send([ $t1, $b11, 2, $nTam ]);

      subtract_m($b12, $b22, $t2, $nTam);
      $mce->send([ $a11, $t2, 3, $nTam ]);

      subtract_m($b21, $b11, $t2, $nTam);
      $mce->send([ $a22, $t2, 4, $nTam ]);

      sum_m($a11, $a12, $t1, $nTam);
      $mce->send([ $t1, $b22, 5, $nTam ]);

      subtract_m($a21, $a11, $t1, $nTam);
      sum_m($b11, $b12, $t2, $nTam);
      $mce->send([ $t1, $t2, 6, $nTam ]);

      subtract_m($a12, $a22, $t1, $nTam);
      sum_m($b21, $b22, $t2, $nTam);
      $mce->send([ $t1, $t2, 7, $nTam ]);

      $mce->run();

   } else {
      sum_m($a11, $a22, $t1, $nTam);
      sum_m($b11, $b22, $t2, $nTam);
      strassen($t1, $t2, $p1, $nTam, $level);

      sum_m($a21, $a22, $t1, $nTam);
      strassen($t1, $b11, $p2, $nTam, $level);

      subtract_m($b12, $b22, $t2, $nTam);
      strassen($a11, $t2, $p3, $nTam, $level);

      subtract_m($b21, $b11, $t2, $nTam);
      strassen($a22, $t2, $p4, $nTam, $level);

      sum_m($a11, $a12, $t1, $nTam);
      strassen($t1, $b22, $p5, $nTam, $level);

      subtract_m($a21, $a11, $t1, $nTam);
      sum_m($b11, $b12, $t2, $nTam);
      strassen($t1, $t2, $p6, $nTam, $level);

      subtract_m($a12, $a22, $t1, $nTam);
      sum_m($b21, $b22, $t2, $nTam);
      strassen($t1, $t2, $p7, $nTam, $level);
   }

   ## Calculate and group into a single matrix $c

   calc_m($p1, $p2, $p3, $p4, $p5, $p6, $p7, $c, $nTam);

   return;
}

###############################################################################

sub divide_m {

   my $m11 = $_[0]; my $m12 = $_[1]; my $m21 = $_[2]; my $m22 = $_[3];
   my $m   = $_[4]; my $tam = $_[5];

   my $n1 = $tam - 1;
   my $n2 = $tam + $n1;

   ins(inplace($m11), $m->slice("0:$n1,0:$n1"));
   ins(inplace($m12), $m->slice("$tam:$n2,0:$n1"));
   ins(inplace($m21), $m->slice("0:$n1,$tam:$n2"));
   ins(inplace($m22), $m->slice("$tam:$n2,$tam:$n2"));

   return;
}

sub calc_m {

   my $p1  = $_[0]; my $p2  = $_[1]; my $p3  = $_[2]; my $p4  = $_[3];
   my $p5  = $_[4]; my $p6  = $_[5]; my $p7  = $_[6]; my $c   = $_[7];
   my $tam = $_[8];

   my $c11 = zeroes $tam,$tam;  my $c12 = zeroes $tam,$tam;
   my $c21 = zeroes $tam,$tam;  my $c22 = zeroes $tam,$tam;
   my $t1  = zeroes $tam,$tam;  my $t2  = zeroes $tam,$tam;

   sum_m($p1, $p4, $t1, $tam);
   sum_m($t1, $p7, $t2, $tam);
   subtract_m($t2, $p5, $c11, $tam);

   sum_m($p3, $p5, $c12, $tam);
   sum_m($p2, $p4, $c21, $tam);

   sum_m($p1, $p3, $t1, $tam);
   sum_m($t1, $p6, $t2, $tam);
   subtract_m($t2, $p2, $c22, $tam);

   ins(inplace($c), $c11, 0, 0);
   ins(inplace($c), $c12, $tam, 0);
   ins(inplace($c), $c21, 0, $tam);
   ins(inplace($c), $c22, $tam, $tam);

   return;
}

sub sum_m {

   my $a = $_[0]; my $b = $_[1]; my $r = $_[2]; my $tam = $_[3];

   ins(inplace($r), $a + $b);

   return;
}

sub subtract_m {

   my $a = $_[0]; my $b = $_[1]; my $r = $_[2]; my $tam = $_[3];

   ins(inplace($r), $a - $b);

   return;
}

