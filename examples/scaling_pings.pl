#!/usr/bin/env perl
###############################################################################
## ----------------------------------------------------------------------------
## Perform ping test and report back failing IPs to standard output.
##
###############################################################################

use strict;
use warnings;

use Cwd qw(abs_path);
use lib abs_path . "/../lib";

my $prog_name = $0; $prog_name =~ s{^.*[\\/]}{}g;

use Time::HiRes qw( time );
use Net::Ping;
use MCE;

###############################################################################
## ----------------------------------------------------------------------------
## Display usage and exit.
##
###############################################################################

sub usage {

   print <<"::_USAGE_BLOCK_END_::";

NAME
   $prog_name -- Perform ping test and report back failing IPs

SYNOPSIS
   $prog_name ip_list_file

DESCRIPTION
   The $prog_name script utilizes the chunking nature of MCE and
   the Net::Ping module to quickly obtain a list of failing IPs.

   Provide a list of IPs for this script to ping by passing the
   location of the list file.

   Any IP(s) failing ping are displayed to standard output.

   The following options are available:

   --max-workers MAX_WORKERS
          Specify number of workers for MCE      Default: 4

   --chunk-size CHUNK_SIZE
          Specify chunk size for MCE             Default: 100

EXAMPLES

   $prog_name --chunk-size 150 --max-workers 6 ip_list_file

::_USAGE_BLOCK_END_::

   exit 1
}

###############################################################################
## ----------------------------------------------------------------------------
## Define defaults and process command-line arguments.
##
###############################################################################

my $flag = sub { 1; };
my $isOk = sub { (@ARGV == 0 or $ARGV[0] =~ /^-/) ? usage() : shift @ARGV; };

my $chunk_size  = 100;
my $max_workers = 4;
my $skip_args   = 0;

my $listfile;

while ( my $arg = shift @ARGV ) {
   unless ($skip_args) {
      $skip_args   = $flag->() and next if ($arg eq '--');
      $max_workers = $isOk->() and next if ($arg =~ /^--max[-_]workers$/);
      $chunk_size  = $isOk->() and next if ($arg =~ /^--chunk[-_]size$/);

      if ($arg =~ /^--max[-_]workers=(.+)/) {
         $max_workers = $1;
         next;
      }
      if ($arg =~ /^--chunk[-_]size=(.+)/) {
         $chunk_size = $1;
         next;
      }

      usage() if ($arg =~ /^-/);
   }

   $listfile = $arg;
}

usage() unless (defined $listfile);

unless (-e $listfile) {
   print "$prog_name: $listfile: No such file or directory\n";
   exit 2;
}
if (-d $listfile) {
   print "$prog_name: $listfile: Is a directory\n";
   exit 1;
}

###############################################################################
## ----------------------------------------------------------------------------
## Parallelize via MCE. Net::Ping requires a file handle per each host
## for its internal loop. Therefore, $chunk_size should be smaller than
## ulimit -n. Otherwise, workers will run out of file handles.
##
## MCE scales this very well. The chunking nature of MCE is beneficial
## in that no matter the size of the list file, workers will not reach
## ulimit -n contention point. Each worker are given $chunk_size IPs
## at a time to process.
##
## Both parallelization and chunking are at play here.
##
###############################################################################

my $exit_status = 0;

sub failed_callback {
   $exit_status = 1;
}

my $mce = MCE->new(

   input_data  => $listfile,
   chunk_size  => $chunk_size,
   max_workers => $max_workers,

   user_begin => sub {
      my $self = shift;
      $self->{wk_pinger} = Net::Ping->new('syn');
      $self->{wk_pinger}->hires();
   },

   user_end => sub {
      my $self = shift;
      $self->{wk_pinger}->close();
   },

   user_func => sub {
      my ($self, $chunk_ref, $chunk_id) = @_;

      my $pinger = $self->{wk_pinger};
      my %pass   = ();
      my @fail   = ();

      ## $chunk_ref points to an array containing $chunk_size items
      ## Since, the list is a file, we need to chomp off the linefeed.

      chomp @{ $chunk_ref };

      ## Feed pinger the next list of $chunk_size hosts/IPs

      for ( @{ $chunk_ref } ) {
         $pinger->ping($_, 3.333);
      }

      ## Let pinger process entire chunk all at once

      while ((my $host, my $rtt, my $ip) = $pinger->ack) {
         $pass{$host} = $pass{$ip} = 1;
      }

      ## Store failed hosts/IPs

      for ( @{ $chunk_ref } ) {
         push @fail, "Failed ping: $_\n" unless exists $pass{$_};
      }

      ## Display failed result to STDOUT

      if (@fail > 0) {
         $self->do('failed_callback');
         $self->sendto('stdout', @fail);
      }
   }
);

$mce->run();

exit $exit_status;

