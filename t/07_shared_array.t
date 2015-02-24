#!/usr/bin/env perl

use strict;
use warnings;

use Test::More tests => 14;
use MCE::Flow max_workers => 1;
use MCE::Shared;

my @a1 = (10, '', '');
my ($e1, $e2, $d1, $s1, $s2, $s3);

mce_share \@a1, \$e1, \$e2, \$d1, \$s1, \$s2, \$s3;

###############################################################################

MCE::Flow::run( sub {
   $a1[0] +=  1; tied(@a1)->add(0, 4);
   $a1[1] .= ''; tied(@a1)->concat(1, '');
   $a1[2] .= 10; tied(@a1)->concat(2, 'string');
   $s1     = scalar @a1;
   $#a1    = 20;
   $s2     = scalar @a1;
});

MCE::Flow::finish;

is( $a1[0], 15, 'shared array, check fetch, store' );
is( $a1[1], '', 'shared array, check blank value' );
is( $a1[2], '10string', 'shared array, check concatenation' );
is( $s1, 3, 'shared array, check fetchsize' );
is( $s2, 21, 'shared array, check storesize' );

###############################################################################

MCE::Flow::run( sub {
   $e1 = exists $a1[1] ? 1 : 0;
   $d1 = delete $a1[1];
   $e2 = exists $a1[1] ? 1 : 0;
   @a1 = (); $s1 = scalar @a1;
   $a1[2] = [ 'wind', 'air' ];
});

MCE::Flow::finish;

is( $e1,  1, 'shared array, check exists before delete' );
is( $d1, '', 'shared array, check delete' );
is( $e2,  0, 'shared array, check exists after delete' );
is( $s1,  0, 'shared array, check clear' );
is( $a1[2]->[1], 'air', 'shared array, check auto freeze/thaw' );

###############################################################################

@a1 = qw( One for all... All for one... );

MCE::Flow::run( sub {
   push(@a1, 'sun', 'moon'); unshift(@a1, 'wind', 'air');
   my @tmp = splice(@a1, 2, 6); $s3 = length(join('', @tmp));
   $s1 = shift(@a1); $s2 = pop(@a1);
});

MCE::Flow::finish;

is( $s3, 24, 'shared array, check splice' );
is( join(' ', @a1), 'air sun', 'shared array, check push, unshift' );
is( $s1, 'wind', 'shared array, check shift' );
is( $s2, 'moon', 'shared array, check pop' );

