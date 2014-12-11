#!/usr/bin/env perl

use strict;
use warnings;

use Test::More tests => 32;

use MCE;
use MCE::Queue;

###############################################################################

##  Test MCE::Queue 'priority' queuing by the MCE Manager

my ($q, @r);

###############################################################################

##  FIFO tests

$q = MCE::Queue->new( type => $MCE::Queue::FIFO );

$q->enqueuep(5, '1', '2');
$q->enqueuep(5, '3');
$q->enqueuep(5, '4');

is( join('', @{ $q->_get_aref(5) }), '1234', 'fifo, check enqueuep' );

@r = $q->dequeue(2);
push @r, $q->dequeue;

is( join('', @r), '123', 'fifo, check dequeue' );
is( join('', @{ $q->_get_aref(5) }), '4', 'fifo, check array' );

$q->clear;

is( $q->_get_aref(5), undef, 'fifo, check clear' );

$q->enqueuep(5, 'a', 'b', 'c', 'd');

$q->insertp(5,   1, 'e', 'f');
$q->insertp(5,   3, 'g');
$q->insertp(5,  -2, 'h');
$q->insertp(5,   7, 'i');
$q->insertp(5,   9, 'j');
$q->insertp(5,  20, 'k');
$q->insertp(5, -10, 'l');
$q->insertp(5, -12, 'm');
$q->insertp(5, -20, 'n');

is( join('', @{ $q->_get_aref(5) }), 'nmalefgbhcidjk', 'fifo, check insertp' );
is( $q->pending(), 14, 'fifo, check pending' );

is( $q->peekp(5     ),   'n', 'fifo, check peekp at head'      );
is( $q->peekp(5,   0),   'n', 'fifo, check peekp at index   0' );
is( $q->peekp(5,   2),   'a', 'fifo, check peekp at index   2' );
is( $q->peekp(5,  13),   'k', 'fifo, check peekp at index  13' );
is( $q->peekp(5,  20), undef, 'fifo, check peekp at index  20' );
is( $q->peekp(5,  -2),   'j', 'fifo, check peekp at index  -2' );
is( $q->peekp(5, -13),   'm', 'fifo, check peekp at index -13' );
is( $q->peekp(5, -14),   'n', 'fifo, check peekp at index -14' );
is( $q->peekp(5, -15), undef, 'fifo, check peekp at index -15' );
is( $q->peekp(5, -20), undef, 'fifo, check peekp at index -20' );

###############################################################################

##  LIFO tests

$q = MCE::Queue->new( type => $MCE::Queue::LIFO );

$q->enqueuep(5, '1', '2');
$q->enqueuep(5, '3');
$q->enqueuep(5, '4');

##  Note (lifo)
##
##  Enqueue appends to an array similarly to fifo
##  Thus, the enqueuep check is identical to fifo

is( join('', @{ $q->_get_aref(5) }), '1234', 'lifo, check enqueuep' );

@r = $q->dequeue(2);
push @r, $q->dequeue;

is( join('', @r), '432', 'lifo, check dequeue' );
is( join('', @{ $q->_get_aref(5) }), '1', 'lifo, check array' );

$q->clear;

is( $q->_get_aref(5), undef, 'lifo, check clear' );

$q->enqueuep(5, 'a', 'b', 'c', 'd');

$q->insertp(5,   1, 'e', 'f');
$q->insertp(5,   3, 'g');
$q->insertp(5,  -2, 'h');
$q->insertp(5,   7, 'i');
$q->insertp(5,   9, 'j');
$q->insertp(5,  20, 'k');
$q->insertp(5, -10, 'l');
$q->insertp(5, -12, 'm');
$q->insertp(5, -20, 'n');

is( join('', @{ $q->_get_aref(5) }), 'kjaibhcgefldmn', 'lifo, check insertp' );
is( $q->pending(), 14, 'lifo, check pending' );

is( $q->peekp(5     ),   'n', 'lifo, check peekp at head'      );
is( $q->peekp(5,   0),   'n', 'lifo, check peekp at index   0' );
is( $q->peekp(5,   2),   'd', 'lifo, check peekp at index   2' );
is( $q->peekp(5,  13),   'k', 'lifo, check peekp at index  13' );
is( $q->peekp(5,  20), undef, 'lifo, check peekp at index  20' );
is( $q->peekp(5,  -2),   'j', 'lifo, check peekp at index  -2' );
is( $q->peekp(5, -13),   'm', 'lifo, check peekp at index -13' );
is( $q->peekp(5, -14),   'n', 'lifo, check peekp at index -14' );
is( $q->peekp(5, -15), undef, 'lifo, check peekp at index -15' );
is( $q->peekp(5, -20), undef, 'lifo, check peekp at index -20' );

