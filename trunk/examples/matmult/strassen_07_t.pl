#!/usr/bin/env perl

##
## Usage:
##    perl strassen_07_t.pl 1024                   ## Default matrix size 512
##

use strict;
use warnings;

use Cwd 'abs_path';  ## Remove taintedness from path
use lib ($_) = (abs_path().'/../../lib') =~ /(.*)/;

my $prog_name = $0; $prog_name =~ s{^.*[\\/]}{}g;

use Time::HiRes qw(time);

use PDL;
use PDL::Parallel::threads qw(retrieve_pdls free_pdls);

use MCE::Signal qw($tmp_dir -use_dev_shm);
use MCE;

###############################################################################
 # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * #
###############################################################################

my $tam = @ARGV ? shift : 512;           ## Wants power of 2 only

unless (is_power_of_two($tam)) {
   die "error: $tam must be a power of 2 integer.\n";
}

my $mce = configure_and_spawn_mce() if ($tam > 128);

my $a = sequence $tam,$tam;
my $b = sequence $tam,$tam;
my $c = zeroes   $tam,$tam;

my $start = time();
strassen($a, $b, $c, $tam, $mce);
my $end = time();

$mce->shutdown if (defined $mce);

## Print results -- use same pairs to match David Mertens' output.
printf "\n## $prog_name $tam: compute time: %0.03f secs\n\n", $end - $start;

for my $pair ([0, 0], [324, 5], [42, 172], [$tam-1, $tam-1]) {
   my ($col, $row) = @$pair; $col %= $tam; $row %= $tam;
   printf "## (%d, %d): %s\n", $col, $row, $c->at($col, $row);
}

print "\n";

###############################################################################
 # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * # * #
###############################################################################

sub configure_and_spawn_mce {

   return MCE->new(

      max_workers => 7,

      user_func   => sub {
         my $self = $_[0];
         my $data = $self->{user_data};

         my $tam = $data->[3];
         my $result = zeroes $tam,$tam;

         my ($a, $b) = retrieve_pdls($data->[0], $data->[1]);

         strassen_r($a, $b, $result, $tam);

         free_pdls($a, $b);

         $result->share_as($self->sess_dir . "/p" . $data->[2]);
      }

   )->spawn;
}

sub is_power_of_two {

   my $n = $_[0];

   return 0 if ($n !~ /^\d+$/); 
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

   my $sess_dir = $mce->sess_dir;

   my ($p1, $p2, $p3, $p4, $p5, $p6, $p7);
   my $nTam = $tam / 2;

   my ($a11, $a12, $a21, $a22) = divide_m($a, $nTam);
   my ($b11, $b12, $b21, $b22) = divide_m($b, $nTam);

   my $t1 = zeroes $nTam,$nTam;

   sum_m($a21, $a22, $t1, $nTam);
   $t1->copy->share_as("$sess_dir/2a");
   $b11->copy->share_as("$sess_dir/2b");
   $mce->send([ "$sess_dir/2a", "$sess_dir/2b", 2, $nTam ]);

   subtract_m($b12, $b22, $t1, $nTam);
   $a11->copy->share_as("$sess_dir/3a");
   $t1->copy->share_as("$sess_dir/3b");
   $mce->send([ "$sess_dir/3a", "$sess_dir/3b", 3, $nTam ]);

   subtract_m($b21, $b11, $t1, $nTam);
   $a22->copy->share_as("$sess_dir/4a");
   $t1->copy->share_as("$sess_dir/4b");
   $mce->send([ "$sess_dir/4a", "$sess_dir/4b", 4, $nTam ]);

   sum_m($a11, $a12, $t1, $nTam);
   $t1->copy->share_as("$sess_dir/5a");
   $b22->copy->share_as("$sess_dir/5b");
   $mce->send([ "$sess_dir/5a", "$sess_dir/5b", 5, $nTam ]);

   subtract_m($a12, $a22, $t1, $nTam);
   sum_m($b21, $b22, $a12, $nTam);               ## Reuse $a12
   $t1->copy->share_as("$sess_dir/7a");
   $a12->copy->share_as("$sess_dir/7b");
   $mce->send([ "$sess_dir/7a", "$sess_dir/7b", 7, $nTam ]);

   subtract_m($a21, $a11, $t1, $nTam);
   sum_m($b11, $b12, $a12, $nTam);               ## Reuse $a12
   $t1->copy->share_as("$sess_dir/6a");
   $a12->copy->share_as("$sess_dir/6b");
   $mce->send([ "$sess_dir/6a", "$sess_dir/6b", 6, $nTam ]);

   sum_m($a11, $a22, $t1, $nTam);
   sum_m($b11, $b22, $a12, $nTam);               ## Reuse $a12
   $t1->share_as("$sess_dir/1a");                ## Share $t1 (no need to copy)
   $a12->copy->share_as("$sess_dir/1b");
   $mce->send([ "$sess_dir/1a", "$sess_dir/1b", 1, $nTam ]);

   $mce->run(0);

   ($p1, $p2, $p3, $p4, $p5, $p6, $p7) = retrieve_pdls(
      "$sess_dir/p1", "$sess_dir/p2", "$sess_dir/p3", "$sess_dir/p4",
      "$sess_dir/p5", "$sess_dir/p6", "$sess_dir/p7"
   );

   calc_m($p1, $p2, $p3, $p4, $p5, $p6, $p7, $c, $nTam, $t1, $a12);

   free_pdls($p1, $p2, $p3, $p4, $p5, $p6, $p7);

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

