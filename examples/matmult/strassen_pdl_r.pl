#!/usr/bin/env perl

##
## Usage:
##    perl strassen_pdl_r.pl 1024        ## Default size 512
##

use strict;
use warnings;

use Cwd qw(abs_path);
use lib abs_path . "/../../lib";

my $prog_name = $0; $prog_name =~ s{^.*[\\/]}{}g;

use Time::HiRes qw(time);

use PDL;
use PDL::Parallel::threads qw(retrieve_pdls free_pdls);

use PDL::IO::Storable;                   ## Required for PDL + MCE combo

use MCE::Signal qw($tmp_dir -use_dev_shm);
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

my $mce = configure_and_spawn_mce() if ($tam > 128);

my $a = sequence $tam,$tam;
my $b = sequence $tam,$tam;
my $c = zeroes   $tam,$tam;

my $start = time();
strassen($a, $b, $c, $tam, $mce);
my $end = time();

printf STDERR "\n## $prog_name $tam: compute time: %0.03f secs\n\n",
   $end - $start;

my $dim_1 = $tam - 1;

print "## (0,0) ", $c->at(0,0), "  ($dim_1,$dim_1) ", $c->at($dim_1,$dim_1);
print "\n\n";

###############################################################################
 # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * #
###############################################################################

my @p;

sub store_result {

   my ($n, $result) = @_;

   $p[$n] = $result;

   return;
}

sub configure_and_spawn_mce {

   return MCE->new(

      max_workers => 7,

      user_func   => sub {
         my $self = $_[0];
         my $data = $self->{user_data};
         return unless (defined $data);

         my $tam = $data->[3];
         my $result = zeroes $tam,$tam;
         my ($a, $b) = retrieve_pdls($data->[0], $data->[1]);
         strassen_r($a, $b, $result, $tam);

         $self->do('store_result', $data->[2], $result);
      }

   )->spawn;
}

sub is_power_of_two {

   my $n = $_[0];

   return ($n != 0 && (($n & $n - 1) == 0));
}

###############################################################################
 # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * #
###############################################################################

sub strassen {

   my $a   = $_[0]; my $b = $_[1]; my $c = $_[2]; my $tam = $_[3];
   my $mce = $_[4];

   if ($tam <= 128) {
      ins(inplace($c), $a x $b);
      return;
   }

   my ($p1, $p2, $p3, $p4, $p5, $p6, $p7);
   my $nTam = $tam / 2;

   my ($a11, $a12, $a21, $a22) = divide_m($a, $nTam);
   my ($b11, $b12, $b21, $b22) = divide_m($b, $nTam);

   my $t1 = zeroes $nTam,$nTam;

   sum_m($a21, $a22, $t1, $nTam);
   my $w_2a = $t1->copy->share_as("w_2a");
   my $w_2b = $b11->copy->share_as("w_2b");
   $mce->send([ "w_2a", "w_2b", 2, $nTam ]);

   subtract_m($b12, $b22, $t1, $nTam);
   my $w_3a = $a11->copy->share_as("w_3a");
   my $w_3b = $t1->copy->share_as("w_3b");
   $mce->send([ "w_3a", "w_3b", 3, $nTam ]);

   subtract_m($b21, $b11, $t1, $nTam);
   my $w_4a = $a22->copy->share_as("w_4a");
   my $w_4b = $t1->copy->share_as("w_4b");
   $mce->send([ "w_4a", "w_4b", 4, $nTam ]);

   sum_m($a11, $a12, $t1, $nTam);
   my $w_5a = $t1->copy->share_as("w_5a");
   my $w_5b = $b22->copy->share_as("w_5b");
   $mce->send([ "w_5a", "w_5b", 5, $nTam ]);

   subtract_m($a12, $a22, $t1, $nTam);
   sum_m($b21, $b22, $a12, $nTam);               ## Reuse $a12 as $t2
   my $w_7a = $t1->copy->share_as("w_7a");
   my $w_7b = $a12->copy->share_as("w_7b");
   $mce->send([ "w_7a", "w_7b", 7, $nTam ]);     ## Reuse $a12 as $t2

   subtract_m($a21, $a11, $t1, $nTam);
   sum_m($b11, $b12, $a12, $nTam);               ## Reuse $a12 as $t2
   my $w_6a = $t1->copy->share_as("w_6a");
   my $w_6b = $a12->copy->share_as("w_6b");
   $mce->send([ "w_6a", "w_6b", 6, $nTam ]);     ## Reuse $a12 as $t2

   sum_m($a11, $a22, $t1, $nTam);
   sum_m($b11, $b22, $a12, $nTam);               ## Reuse $a12 as $t2
   my $w_1a = $t1->copy->share_as("w_1a");
   my $w_1b = $a12->copy->share_as("w_1b");
   $mce->send([ "w_1a", "w_1b", 1, $nTam ]);     ## Reuse $a12 as $t2

   undef $a11;             undef $a21; undef $a22;
   undef $b11; undef $b12; undef $b21; undef $b22;

   $mce->run();

   free_pdls(
      "w_1a", "w_1b", "w_2a", "w_2b", "w_3a", "w_3b", "w_4a", "w_4b",
      "w_5a", "w_5b", "w_6a", "w_6b", "w_7a", "w_7b"
   );

   $p2 = $p[2]; $p3 = $p[3]; $p4 = $p[4]; $p5 = $p[5];
   $p1 = $p[1]; $p6 = $p[6]; $p7 = $p[7];

   calc_m($p1, $p2, $p3, $p4, $p5, $p6, $p7, $c, $nTam, $t1, $a12);

   @p = ();

   return;
}

###############################################################################
 # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * #
###############################################################################

sub strassen_r {

   my $a = $_[0]; my $b = $_[1]; my $c = $_[2]; my $tam = $_[3];

   ## Perform the classic multiplication when matrix is <= 128 X 128

   if ($tam <= 128) {
      ins(inplace($c), $a x $b);
      return;
   }

   ## Otherwise, perform multiplication using Strassen's algorithm

   my $nTam = $tam / 2;

   my $p2 = zeroes $nTam,$nTam; my $p3 = zeroes $nTam,$nTam;
   my $p4 = zeroes $nTam,$nTam; my $p5 = zeroes $nTam,$nTam;

   ## Divide the matrices into 4 sub-matrices

   my ($a11, $a12, $a21, $a22) = divide_m($a, $nTam);
   my ($b11, $b12, $b21, $b22) = divide_m($b, $nTam);

   ## Calculate p1 to p7

   my $t1 = zeroes $nTam,$nTam;

   sum_m($a21, $a22, $t1, $nTam);
   strassen_r($t1, $b11, $p2, $nTam);

   subtract_m($b12, $b22, $t1, $nTam);
   strassen_r($a11, $t1, $p3, $nTam);

   subtract_m($b21, $b11, $t1, $nTam);
   strassen_r($a22, $t1, $p4, $nTam);

   sum_m($a11, $a12, $t1, $nTam);
   strassen_r($t1, $b22, $p5, $nTam);

   subtract_m($p4, $p5, $t1, $nTam);             ## c11
   ins(inplace($c), $t1, 0, 0);

   sum_m($p3, $p5, $t1, $nTam);                  ## c12
   ins(inplace($c), $t1, $nTam, 0);

   sum_m($p2, $p4, $t1, $nTam);                  ## c21
   ins(inplace($c), $t1, 0, $nTam);

   subtract_m($p3, $p2, $t1, $nTam);             ## c22
   ins(inplace($c), $t1, $nTam, $nTam);

   my $t2 = zeroes $nTam,$nTam;

   sum_m($a11, $a22, $t1, $nTam);
   sum_m($b11, $b22, $t2, $nTam);
   strassen_r($t1, $t2, $p2, $nTam);             ## Reuse $p2 to store p1

   subtract_m($a21, $a11, $t1, $nTam);
   sum_m($b11, $b12, $t2, $nTam);
   strassen_r($t1, $t2, $p3, $nTam);             ## Reuse $p3 to store p6

   subtract_m($a12, $a22, $t1, $nTam);
   sum_m($b21, $b22, $t2, $nTam);
   strassen_r($t1, $t2, $p4, $nTam);             ## Reuse $p4 to store p7

   my $n1 = $nTam - 1;
   my $n2 = $nTam + $n1;

   sum_m($p2, $p4, $t1, $nTam);                  ## c11
   use PDL::NiceSlice;
   $c(0:$n1,0:$n1) += $t1;
   no PDL::NiceSlice;

   sum_m($p2, $p3, $t1, $nTam);                  ## c22
   use PDL::NiceSlice;
   $c($nTam:$n2,$nTam:$n2) += $t1;
   no PDL::NiceSlice;

   return;
}

###############################################################################
 # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * #
###############################################################################

sub divide_m {

   my $m = $_[0]; my $tam = $_[1];

   my $n1 = $tam - 1;
   my $n2 = $tam + $n1;

   return (
      $m->slice("0:$n1,0:$n1"),             ## m11
      $m->slice("$tam:$n2,0:$n1"),          ## m12
      $m->slice("0:$n1,$tam:$n2"),          ## m21
      $m->slice("$tam:$n2,$tam:$n2")        ## m22
   );
}

sub calc_m {

   my $p1  = $_[0]; my $p2 = $_[1]; my $p3 = $_[2]; my $p4 = $_[3];
   my $p5  = $_[4]; my $p6 = $_[5]; my $p7 = $_[6]; my $c  = $_[7];
   my $tam = $_[8];

   my $t1  = $_[9]; my $t2 = $_[10];

   sum_m($p1, $p4, $t1, $tam);
   sum_m($t1, $p7, $t2, $tam);
   subtract_m($t2, $p5, $p7, $tam);         ## reuse $p7 to store c11

   sum_m($p1, $p3, $t1, $tam);
   sum_m($t1, $p6, $t2, $tam);
   subtract_m($t2, $p2, $p6, $tam);         ## reuse $p6 to store c22

   sum_m($p3, $p5, $p1, $tam);              ## reuse $p1 to store c12
   sum_m($p2, $p4, $p3, $tam);              ## reuse $p3 to store c21

   ins(inplace($c), $p7, 0, 0);             ## c11 = $p7
   ins(inplace($c), $p1, $tam, 0);          ## c12 = $p1
   ins(inplace($c), $p3, 0, $tam);          ## c21 = $p3
   ins(inplace($c), $p6, $tam, $tam);       ## c22 = $p6

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

