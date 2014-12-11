#!/usr/bin/env perl

use strict; use warnings;

use Cwd 'abs_path';  ## Remove taintedness from path
use lib ($_) = (abs_path().'/../lib') =~ /(.*)/;

# flow_demo.pl
# https://gist.github.com/marioroy/37817977b4e101f3e880

use MCE::Flow Sereal => 1;        # Use Sereal for serialization if available
use MCE::Queue  fast => 1;        # MCE 1.520 (compare with => 0, also Sereal)

# Results from CentOS 7 VM (4 cores): time flow_demo.pl | wc -l
#
# Sereal 0, fast 0:   4.703s      # Serialization via Storable
# Sereal 1, fast 0:   3.926s      # Serialization via Sereal
# Sereal 1, fast 1:   2.092s      # Enable fast optimization; crazy :)

my $setter_q = MCE::Queue->new;
my $pinger_q = MCE::Queue->new;
my $writer_q = MCE::Queue->new;

# See https://metacpan.org/pod/MCE::Core#SYNTAX-for-INPUT_DATA for a DBI
# input iterator (db_iter). Set chunk_size accordingly. Do not go above
# 300 for chunk_size if running a SNMP crawling (imho).

sub make_number_iter {
   my ($first, $last) = @_;
   my $done; my $n = $first;

   return sub {
      my $chunk_size = $_[0]; return if $done;

      my $min = ($n + $chunk_size - 1 > $last) ? $last : $n + $chunk_size - 1;
      my @numbers = ($n .. $min);

      $n = $min + 1; $done = 1 if $min == $last;

      return @numbers;
   }
}

# Begin and End functions.

sub _begin {
   my ($mce, $task_id, $task_name) = @_;

   if ($task_name eq 'writer') {
    # $mce->{dbh} = DBI->connect(...);
      $mce->{dbh} = 'each (writer) obtains db handle once';
   }

   return;
}

sub _end {
   my ($mce, $task_id, $task_name) = @_;

   if ($task_name eq 'writer') {
    # $mce->{dbh}->disconnect;
      delete $mce->{dbh};
   }

   return;
}

# Actual roles. Uncomment MCE->yield below if processing thousands or more.

sub poller {
   my ($mce, $chunk_ref, $chunk_id) = @_;
   my (@pinger_w, @setter_w, @writer_w);
 # MCE->yield;                    # run gracefully, see examples/interval.pl

   foreach (@$chunk_ref) {
      if ($_ % 100 == 0) {
         push @pinger_w, $_;      # poller cannot connect, check ping status
      }
      else {
         if ($_ % 33 == 0) {
            push @setter_w, $_;   # device needs settings
         }
         else {
            push @writer_w, $_;   # all is well
         }
      }
   }

   $pinger_q->enqueue( [ \@pinger_w, $chunk_id, 'ping' ] ) if @pinger_w;
   $setter_q->enqueue( [ \@setter_w, $chunk_id, 'set ' ] ) if @setter_w;
   $setter_q->enqueue( [ \@writer_w, $chunk_id, 'ok  ' ] ) if @writer_w;

   return;
}

sub setter {
   my ($mce) = @_;
 # MCE->yield;                    # adjust interval option below; 0.007

   while (defined (my $next_ref = $setter_q->dequeue)) {
      my ($chunk_ref, $chunk_id, $status) = @{ $next_ref };
      $writer_q->enqueue( [ $chunk_ref, $chunk_id, $status ] );
   }

   return;
}

sub pinger {
   my ($mce) = @_;
 # MCE->yield;                    # all workers are assigned interval slot

   while (defined (my $next_ref = $pinger_q->dequeue)) {
      my ($chunk_ref, $chunk_id, $status) = @{ $next_ref };
      $writer_q->enqueue( [ $chunk_ref, $chunk_id, $status ] );
   }

   return;
}

sub writer {
   my ($mce) = @_;
   my $dbh = $mce->{dbh};

   while (defined (my $next_ref = $writer_q->dequeue)) {
      my ($chunk_ref, $chunk_id, $status) = @{ $next_ref };
      MCE->say("$chunk_id $status " . scalar @$chunk_ref);
   }

   return;
}

# Configure MCE options; task_name, max_workers can take and anonymous array.
#
# Change max_workers to [ 160, 100, 80, 4 ] if processing millions of rows.
# Also tune netfilter on Linux: /etc/sysctl.conf
# net.netfilter.nf_conntrack_udp_timeout = 10
# net.netfilter.nf_conntrack_udp_timeout_stream = 10
# net.nf_conntrack_max = 131072

my $n_pollers = 100;
my $n_setters =  20;
my $n_pingers =  10;
my $n_writers =   4;

MCE::Flow::init {
   chunk_size => 300, input_data => make_number_iter(1, 2_000_000),
   interval => 0.007, user_begin => \&_begin, user_end => \&_end,

   task_name   => [ 'poller',   'setter',   'pinger',   'writer'   ],
   max_workers => [ $n_pollers, $n_setters, $n_pingers, $n_writers ],

   task_end => sub {
      my ($mce, $task_id, $task_name) = @_;

      if ($task_name eq 'poller') {
         $setter_q->enqueue((undef) x $n_setters);
      }
      elsif ($task_name eq 'setter') {
         $pinger_q->enqueue((undef) x $n_pingers);
      }
      elsif ($task_name eq 'pinger') {
         $writer_q->enqueue((undef) x $n_writers);
      }
   }
};

mce_flow \&poller, \&setter, \&pinger, \&writer;

