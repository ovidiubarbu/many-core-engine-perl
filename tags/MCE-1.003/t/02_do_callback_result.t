#!/usr/bin/env perl

use Test::More tests => 5;

use MCE;

my (@ans, @rpl, %hsh, $mce);

###############################################################################

sub callback {
   push @ans, $_[0];
   return;
}

$mce = MCE->new(
   use_threads => 0,
   max_workers => 4,
   user_func   => sub {
      my ($self) = @_;
      $self->do('callback', $self->wid());
   }
);

@ans = ();
$mce->run;

is(join('', sort @ans), '1234', 'check that wid is correct for children');

###############################################################################

sub callback2 {
   push @ans, $_[0];
   return $_[0] * 2;
}

sub callback3 {
   push @rpl, $_[0];
   return;
}

$mce = MCE->new(
   use_threads => 0,
   max_workers => 4,
   user_func   => sub {
      my ($self) = @_;
      my $reply  = $self->do('callback2', $self->wid());
      $self->do('callback3', $reply);
   }
);

@ans = (); @rpl = ();
$mce->run;

is(join('', sort @ans), '1234', 'check that wid is correct for threads');
is(join('', sort @rpl), '2468', 'check that scalar is correct');

###############################################################################

sub callback4 {
   return @rpl;
}

sub callback5 {
   my $a_ref = $_[0];
   my $h = ();

   @ans = ();

   foreach (@{ $a_ref }) {
      push @ans, $_ / 2;
      $h{$_ / 2} = $_;
   }

   return %h;
}

sub callback6 {
   my $h_ref = $_[0];

   @rpl = (); 

   foreach (sort keys %{ $h_ref }) {
      $rpl[$_ - 1] = $h_ref->{$_};
   }

   return;
}

$mce = MCE->new(
   use_threads => 0,
   max_workers => 1,
   user_func   => sub {
      my ($self) = @_;
      my @reply  = $self->do('callback4');
      my %reply  = $self->do('callback5', \@reply);
      $self->do('callback6', \%reply);
   }
);

$mce->run;

is(join('', sort @ans), '1234', 'check that list is correct');
is(join('', sort @rpl), '2468', 'check that hash is correct');

