#!/usr/bin/env perl

## Asynchronous code from the web and modified for Many-Core parallelism.
## https://gist.github.com/jhthorsen/076157063b4bdaa47a3f
##
## Imagine a load-balancer configured to spread the load (round-robin)
## to ports 9701, 9702, 9703, ..., 970N.

use strict; use warnings;

use Cwd 'abs_path'; ## Insert lib-path at the head of @INC.
use lib abs_path($0 =~ m{^(.*)[\\/]} && $1 || abs_path) . '/../../lib';

use MCE::Flow;
use MCE::Shared;

my $ncpu = MCE::Util::get_ncpu;
my $next = 0; mce_share \$next; # shared counter (increments by one)

my $mce_opts = {
   user_begin => sub {
      my ($mce) = @_;
      ## Choose a module of your choice if CPU affinity is desired.
      ## e.g. set_cpu_affinity( $$, ($mce->wid - 1) % $ncpu );
   },
   max_workers => 'auto',
};

###############################################################################

use Mojo::Base -strict;
use Mojo::IOLoop;

my $mce_task = sub {
   my ($mce) = @_; my ($pid, $wid) = ($mce->pid, $mce->wid);

   my $next_obj = tied($next); # using tied object to shared variable

   my $id = Mojo::IOLoop->server({ port => 9700 + $wid }, sub {
      my ($ioloop, $stream) = @_;
      my ($disconnect, $next_id, $loop_id);

      ## The ->add helper method does fetch, store, and fetch in one
      ## trip. Thus, $mutex->synchronize is not necessary.
      $next_id = $next_obj->add(1); # same as $next_id = $next += 1

      $disconnect = sub {
         warn "[$pid:$next_id] client disconnected\n";
         Mojo::IOLoop->remove($loop_id);
      };

      $stream->on(close => $disconnect);
      $stream->on(error => $disconnect);
      $stream->on(read  => sub { $_[0]->write($_[1]); });

      $loop_id = Mojo::IOLoop->recurring(
         5 => sub { $stream->write("[$pid:$next_id] ping!\n"); }
      );
   });

   my $hdl = Mojo::IOLoop->acceptor($id)->handle;

   warn "[$pid:0] listening on ".$hdl->sockhost.":".$hdl->sockport."\n";

   Mojo::IOLoop->start;
};

MCE::Flow->run($mce_opts, $mce_task);

