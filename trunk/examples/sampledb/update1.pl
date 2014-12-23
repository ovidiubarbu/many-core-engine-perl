#!/usr/bin/env perl

## Usage: perl update1.pl

use strict;
use warnings;

use Cwd 'abs_path'; ## Insert lib-path at the head of @INC.
use lib abs_path($0 =~ m{^(.*)[\\/]} && $1 || abs_path) . '/../../lib';

my ($prog_name, $prog_dir, $db_file);

BEGIN {
   $prog_name = $0;             $prog_name =~ s{^.*[\\/]}{}g;
   $prog_dir  = abs_path($0);   $prog_dir  =~ s{[\\/][^\\/]*$}{};
   $db_file   = "$prog_dir/SAMPLE.DB";
}

use MCE::Loop max_workers => 3;

use Time::HiRes 'time';
use DBI;

my $start = time;

###############################################################################

sub db_iter_client_server {
   my ($dsn, $user, $password) = @_;

   my $dbh = DBI->connect($dsn, $user, $password, {
      PrintError => 0, RaiseError => 1, AutoCommit => 1,
      FetchHashKeyName => 'NAME_lc'
   }) or die $DBI::errstr;

   my $sth = $dbh->prepare(
      "SELECT seq_id, value1, value2 FROM seq"
   );
   $sth->execute();

   return sub {
      if (my @row = $sth->fetchrow_array) {
         return @row;
      }
      return;
   }
}

sub db_iter_auto_offset {
   my ($dsn, $user, $password) = @_;

   my $dbh = DBI->connect($dsn, $user, $password, {
      PrintError => 0, RaiseError => 1, AutoCommit => 1,
      FetchHashKeyName => 'NAME_lc'
   }) or die $DBI::errstr;

   my $offset = 0; my $sth = $dbh->prepare(
      "SELECT seq_id, value1, value2 FROM seq LIMIT ? OFFSET ?"
   );

   my $rows_ref; my $prefetch_size = 300; my $i = $prefetch_size;

   return sub {
      if ($i == $prefetch_size) {
         $sth->execute($prefetch_size, $offset);
         $rows_ref = $sth->fetchall_arrayref(undef, $prefetch_size);
         $offset += $prefetch_size;
         $i = 0;
      }
      if (defined $rows_ref && defined $rows_ref->[$i]) {
         return @{ $rows_ref->[$i++] };
      }
      return;
   }
}

###############################################################################

my ($dsn, $user, $password) = ("dbi:SQLite:dbname=$db_file", "", "");

MCE::Loop::init {
   user_begin => sub {
      my ($mce, $task_id, $task_name) = @_;

      ## Store dbh and prepare statements inside the mce hash.

      $mce->{dbh} = DBI->connect($dsn, $user, $password, {
         PrintError => 0, RaiseError => 1, AutoCommit => 1,
         FetchHashKeyName => 'NAME_lc'
      }) or die $DBI::errstr;

      $mce->{upd} = $mce->{dbh}->prepare(
         "UPDATE seq SET value1 = ?, value2 = ? WHERE seq_id = ?"
      );

      return;
   },
   user_end => sub {
      my ($mce, $task_id, $task_name) = @_;

      $mce->{upd}->finish;
      $mce->{dbh}->disconnect;

      return;
   }
};

mce_loop {
   my ($mce, $chunk_ref, $chunk_id) = @_;
   my ($dbh, $upd) = ($mce->{dbh}, $mce->{upd});
   my ($seq_id, $value1, $value2) = @{ $chunk_ref };

   MCE->say("Updating row $seq_id");

   $upd->execute(int($value1 * 1.333), $value2 * 1.333, $seq_id);

} db_iter_auto_offset($dsn, $user, $password);

printf {*STDERR} "\n## Compute time: %0.03f\n\n", time() - $start;

