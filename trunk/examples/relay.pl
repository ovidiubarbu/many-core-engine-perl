#!/usr/bin/env perl

## Relaying is orderly and driven by chunk_id when processing data, otherwise
## task_wid. Only the first sub-task is allowed to relay information.
##
## See findnull.pl, cat.pl, and biofasta/fasta_aidx.pl for other use cases.

use strict;
use warnings;

use Cwd 'abs_path'; ## Insert lib-path at the head of @INC.
use lib abs_path($0 =~ m{^(.*)[\\/]} && $1 || abs_path) . '/../lib';

use MCE::Flow max_workers => 4;

print "\n";

###############################################################################

mce_flow {
   init_relay => { a => 0, b => 10 },     ## Relaying multiple values (HASH)
},
sub {
   ## do work ...
   my ($a, $b) = (1, 2);

   ## my %prior_val = MCE->relay( sub { $_->{a} += $a; $_->{b} += $b } );
   my %prior_val = MCE::relay { $_->{a} += $a; $_->{b} += $b };

   MCE->print("$prior_val{a} : $prior_val{b}\n");
};

my %final_val = MCE->relay_final;

print "$final_val{a} : $final_val{b} final\n\n";

###############################################################################

mce_flow {
   init_relay => [ 0, 10 ],               ## Relaying multiple values (ARRAY)
},
sub {
   ## do work ...
   my ($a, $b) = (1, 2);

   ## my @prior_val = MCE->relay( sub { $_->[0] += $a; $_->[1] += $b } );
   my @prior_val = MCE::relay { $_->[0] += $a; $_->[1] += $b };

   MCE->print("$prior_val[0] : $prior_val[1]\n");
};

my @final_val = MCE->relay_final;

print "$final_val[0] : $final_val[1] final\n\n";

###############################################################################

mce_flow {
   init_relay => 100,                     ## Relaying a single value
},
sub {
   ## do work ...
   my $a = 3;

   ## my $prior_val = MCE->relay( sub { $_ += $a } );
   my $prior_val = MCE::relay { $_ += $a };

   MCE->print("$prior_val\n");
};

my $final_val = MCE->relay_final;

print "$final_val final\n\n";

