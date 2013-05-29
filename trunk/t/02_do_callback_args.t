#!/usr/bin/env perl

use strict;
use warnings;

BEGIN {
   eval 'use threads; use threads::shared;' if $^O eq 'MSWin32';
}

use Test::More tests => 4;

use MCE;

sub callback1 {
   my ($a_ref, $h_ref, $s_ref) = @_;

   is($a_ref->[1], 'two', 'check array reference');
   is($h_ref->{'two'}, 'TWO', 'check hash reference');
   is($$s_ref, 'fall colors', 'check scalar reference'); 

   return;
}

sub callback2 {
   my $s = $_[0];
   is($s, 1, 'check scalar value');
}

my $mce = MCE->new(
   use_threads => ($^O eq 'MSWin32') ? 1 : 0,
   spawn_delay => 0.2,
   max_workers => 1,

   user_func => sub {
      my ($self) = @_;
      my @a = ('one', 'two');
      my %h = ('one' => 'ONE', 'two' => 'TWO');
      my $s = 'fall colors';
      $self->do('callback1', \@a, \%h, \$s);
      $self->do('callback2', $self->wid());
   }
);

$mce->run;

