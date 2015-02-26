#!/usr/bin/env perl

## Asynchronous code from the web and modified for Many-Core parallelism.
## https://blog.afoolishmanifesto.com/posts/concurrency-and-async-in-perl/
##
## Imagine a load-balancer configured to spread the load (round-robin)
## to ports 9601, 9602, 9603, ..., 960N.

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

use IO::Async::Loop;
use IO::Async::Timer::Periodic;

my $mce_task = sub {
   my ($mce) = @_; my ($pid, $wid) = ($mce->pid, $mce->wid);

   my $next_obj = tied($next); # using tied object to shared variable
   my $loop = IO::Async::Loop->new;

   my $server = $loop->listen(
      host     => '0.0.0.0',
      socktype => 'stream',
      service  => 9600 + $wid,

      on_stream => sub {
         my ($stream) = @_;

         ## The ->add helper method does fetch, store, and fetch in one
         ## trip. Thus, $mce->synchronize is not necessary.
         my $next_id = $next_obj->add(1); # same as $next_id = $next += 1

         $stream->configure(
            on_read => sub {
               my ($self, $buffref, $eof) = @_;
               $self->write($$buffref); $$buffref = '';
               if ($eof) {
                  warn "[$pid:$next_id] client disconnected\n";
                  $self->close_now;
               }
               return 0;
            },
         );
         $stream->add_child(
            IO::Async::Timer::Periodic->new(
               interval => 5, on_tick => sub {
                  my ($self) = @_;
                  $self->parent->write("[$pid:$next_id] ping!\n")
               },
            )->start
         );

         $loop->add( $stream );
      },

      on_resolve_error => sub { die "Cannot resolve - $_[1]\n"; },
      on_listen_error  => sub { die "Cannot listen - $_[1]\n"; },

      on_listen => sub {
         my ($s) = @_;
         warn "[$pid:0] listening on ".$s->sockhost.":".$s->sockport."\n";
      },

   );

   $loop->run;
};

MCE::Flow->run($mce_opts, $mce_task);

