#!/usr/bin/env perl

## Asynchronous code from the web and modified for Many-Core parallelism.
## https://blog.afoolishmanifesto.com/posts/concurrency-and-async-in-perl/
##
## Imagine a load-balancer configured to spread the load (round-robin)
## to ports 9501, 9502, 9503, ..., 950N.

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

use AnyEvent;
use AnyEvent::Socket;
use AnyEvent::Handle;

use Scalar::Util 'refaddr';

my $mce_task = sub {
   my ($mce) = @_; my ($pid, $wid) = ($mce->pid, $mce->wid);

   my $next_obj = tied($next); # using tied object to shared variable
   my %handles;

   my $server = tcp_server undef, 9500 + $wid, sub {
      my ($fh, $host, $port) = @_;
      my ($disconnect, $next_id, $hdl);

      ## The ->add helper method does fetch, store, and fetch in one
      ## trip. Thus, $mutex->synchronize is not necessary.
      $next_id = $next_obj->add(1); # same as $next_id = $next += 1

      $disconnect = sub {
         my ($hdl) = @_;
         warn "[$pid:$next_id] client disconnected\n";
         delete $handles{ refaddr $hdl };
         $hdl->destroy;
      };

      $hdl = AnyEvent::Handle->new(
         fh       => $fh,
         on_eof   => $disconnect,
         on_error => $disconnect,
         on_read  => sub {
            my ($hdl) = @_;
            $hdl->push_write($hdl->rbuf);
            substr($hdl->{rbuf}, 0) = '';
         },
      );

      $handles{ refaddr $hdl } = $hdl;

      $hdl->{timer} = AnyEvent->timer(
         after    => 5,
         interval => 5,
         cb       => sub {
            $hdl->push_write("[$pid:$next_id] ping!\n")
         },
      );
   }, sub {
      my ($fh, $thishost, $thisport) = @_;
      warn "[$pid:0] listening on $thishost:$thisport\n";
   };

   AnyEvent->condvar->wait();
};

MCE::Flow->run($mce_opts, $mce_task);

