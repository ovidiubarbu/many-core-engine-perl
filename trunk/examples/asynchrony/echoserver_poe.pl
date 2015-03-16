#!/usr/bin/env perl

## Asynchronous code from the web and modified for Many-Core parallelism.
## https://blog.afoolishmanifesto.com/posts/concurrency-and-async-in-perl/
##
## Imagine a load-balancer configured to spread the load (round-robin)
## to ports 9801, 9802, 9803, ..., 980N.

use warnings; use strict;

use Cwd 'abs_path'; ## Insert lib-path at the head of @INC.
use lib abs_path($0 =~ m{^(.*)[\\/]} && $1 || abs_path) . '/../../lib';

use MCE::Flow;
use MCE::Shared;

my $ncpu = MCE::Util::get_ncpu;
mce_share my $next => 0;        # shared counter (increments by one)

my $mce_opts = {
   user_begin => sub {
      my ($mce) = @_;
      ## Choose a module of your choice if CPU affinity is desired.
      ## e.g. set_cpu_affinity( $$, ($mce->wid - 1) % $ncpu );
   },
   max_workers => 'auto',
};

###############################################################################

use POE qw( Component::Server::TCP );

my $mce_task = sub {
   my ($mce) = @_; my ($pid, $wid) = ($mce->pid, $mce->wid);

   my $next_obj = tied($next); # using tied object to shared variable
   my $port = 9800 + $wid;

   POE::Component::Server::TCP->new(
      Port => $port,
      Started => sub {
         warn "[$pid:0] listening on 0.0.0.0:$port\n";
      },
      ClientConnected => sub {
         ## The ->add helper method does fetch, store, and fetch in one
         ## trip. Thus, $mutex->synchronize is not necessary.
         $_[HEAP]{next_id} = $next_obj->add(1); # same as $next_id = $next += 1
         POE::Kernel->delay( ping => 5 );
      },
      ClientInput => sub {
         my $input = $_[ARG0];
         $_[HEAP]{client}->put( $input );
      },
      ClientDisconnected => sub {
         my $next_id = $_[HEAP]{next_id};
         warn "[$pid:$next_id] client disconnected\n";
         POE::Kernel->delay( ping => undef );
      },

      ## Custom event handlers.
      ## Encapsulated in /(Inline|Object|Package)States/ to avoid
      ## potential conflict with reserved constructor parameters.

      InlineStates => {
         ping => sub {
            my $next_id = $_[HEAP]{next_id};
            $_[HEAP]{client}->put("[$pid:$next_id] ping!");
            POE::Kernel->delay( ping => 5 );
         },
      },
   );

   POE::Kernel->run();
};

MCE::Flow->run($mce_opts, $mce_task);

