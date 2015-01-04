#!/usr/bin/env perl

use strict;
use warnings;

## FASTA index (.fai) generation for FASTA files.
##
## The original plan was to run CPAN BioUtil::Seq::FastaReader in parallel.
## I decided to process by records versus lines.
##
## https://gist.github.com/marioroy/85d08fc82845f11d12b5 (for most current)
##
## Synopsis
##   fastaindxer.pl [ /path/to/fastafile.fa ]
##
## Two million clusters extracted from uniref100.fasta.gz (2013_12).
##   gunzip -c uniref100.fasta.gz | head -15687827 > uniref.fasta
##
## Running serially:  15.642s
## Many-Core Engine:   4.886s

use Cwd 'abs_path';
use lib abs_path($0 =~ m{^(.*)[\\/]} && $1 || abs_path) . '/../lib';

my  $prog_dir = abs_path($0 =~ m{^(.*)[\\/]} && $1 || abs_path);
do "$prog_dir/include/fastareader_fai.inc.pl";

###############################################################################

use MCE::Flow chunk_size => '1024k', max_workers => 'auto';
use Time::HiRes qw(time);

my $exit_status = 0;

## Display error message.

sub print_error {
   my ($error_msg) = @_;
   print {*STDERR} $error_msg."\n";
   $exit_status = 1;
}

## Iterator for preserving output order.

sub output_iterator {
   my ($output_fh, $offset) = @_;
   my (%tmp, $size); my $order_id = 1;

   return sub {
      $tmp{ (shift) } = \@_;

      while (1) {
         last unless exists $tmp{$order_id};
         $size = shift @{ $tmp{$order_id} };
         my $buffer = '';

         foreach my $row ( @{ delete $tmp{$order_id++} } ) {
            if ($row->[1] < 0) {
               $buffer .= $row->[0].$row->[1].$row->[2];
            } else {
               $buffer .= $row->[0].($row->[1] + $offset).$row->[2];
            }
         }
         print {$output_fh} $buffer;
         $offset += $size;
      }

      return;
   };
}

## Open handle to index file *.fai.

my $output_fh;
my $file  = shift || \*DATA;
my $start = time;

if (ref $file) {
   $output_fh = \*STDOUT;
}
else {
   die "$file: $!\n" unless -f $file;
   open $output_fh, '>', "$file.fai" or die "$file.fai: open: $!\n";
}

## Process file in parallel.

my $offset_adj = BioUtil::Test::GetFirstOffset($file);

mce_flow_f {
   gather => output_iterator($output_fh, $offset_adj),
   RS => "\n>", use_slurpio => 1,
},
sub {
   my ($mce, $slurp_ref, $chunk_id) = @_;

   ## prepend leading '>' for chunks 2 and higher
   ${ $slurp_ref } = '>' . ${ $slurp_ref } if $chunk_id > 1;

   ## read from scalar reference
   my $next_seq = BioUtil::Test::FastaReader_fai($slurp_ref, 0);
   my ($name, $len, $off, $bases, $bytes, $acc, @output);

   ## loop through sequences in $slurp_ref
   while (my $fa = &$next_seq()) {
      ($name, $len, $off, $bases, $bytes, $acc) = @{ $fa };

      if ($off == -2) {
         my $err = 'SKIPPED: mismatched line lengths within sequence '.$name;
         MCE->do('print_error', $err);
      }
      else {
         ## concatenate left,right sides to save time for the manager process
         push @output, [ "$name\t$len\t", $off, "\t$bases\t$bytes\n" ];
      }
   }

   ## gather output for this chunk
   MCE->gather($chunk_id, $acc, @output);

}, $file;

## Finish.

close $output_fh unless (ref $file);
printf {*STDERR} "\n## Compute time: %0.03f\n\n", time - $start;

exit $exit_status;

###############################################################################

__END__
>seq1 description1
TAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCC
TAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCAACCCTAACCCT
AACCCTAACCCTAACCCTAACCCTAACCCCTAACCCTAACCCTAACCCTAACCCTAACCT
AACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCCTAACCC
TAACCCTAAACCCTAAACCCTAACCCTAACCCTAACCCTAACCCTAACCCCAACCCCAAC
CCCAACCCCAACCCCAACCCCAACCCTAACCCCTAACCCTAACCCTAACCCTACCCTAAC
CCTAACCCTAACCCTAACCCTAACCCTAACCCCTAACCCCTAACCCTAACCCTAACCCTA
ACCCTAACCCTAACCCTAACCCCTAACCCTAACCCTAACCCTAACCCTCGCGGTACCCTC
AGCCGGCCCGCCCGCCCGGGTCTGACCTGAGGAGAACTGTGCTCCGCCTTCAGAGTACCA
CCGAAATCTGTGCAGAGGACAACGCAGCTCCGCCCTCGCGGTGCTCTCCGGGTCTGTGCT
GAGGAGAACGCAACTCCGCCGGCGCAGGCG

>seq2 description2
TAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCC
TAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCAACCCTAACCCT
AACCCTAACCCTAACCCTAACCCTAACCCCTAACCCTAACCCTAACCCTAACCCTAACCT
AACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCCTAACCC
TAACCCTAAACCCTAAACCCTAACCCTAACCCTAACCCTAACCCTAACCCCAACCCCAAC
CCCAACCCCAACCCCAACCCCAACCCTAACCCCTAACCCTAACCCTAACCCTACCCTAAC
CCTAACCCTAACCCTAACCCTAACCCTAACCCCTAACCCCTAACCCTAACCCTAACCCTA
ACCCTAACCCTAACCCTAACCCCTAACCCTAACCCTAACCCTAACCCTCGCGGTACCCTC
AGCCGGCCCGCCCGCCCGGGTCTGACCTGAGGAGAACTGTGCTCCGCCTTCAGAGTACCA
CCGAAATCTGTGCAGAGGACAACGCAGCTCCGCCCTCGCGGTGCTCTCCGGGTCTGTGCT
GAGGAGAACGCAAC

>seq3 description3
TAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCC
TAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCAACCCTAACCCT
AACCCTAACCCTAACCCTAACCCTAACCCCTAACCCTAACCCTAACCCTAACCCTAACCT
AACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCTAACCCCTAACCC
TAACCCTAAACCCTAAACCCTAACCCTAACCCTAACCCTACTACCCTAACCCTAACCCTA
ACCCTAACCCTAACCCTAACCCCTAACCCCTAACCCTAACCCTAACCCTAACCCTAACCC
TAACCCTAACCCCTAACCCTAACCCTAACCCTAACCCTCGCGGTACCCTCAGCCGGCCCG
CCCGCCCGGGTCTGACCTGAGGAGAACTGTGCTCCGCCTTCAGAGTACCACCGAAATCTG
TGCAGAGGACAACGCAGCTCCGCCCTCGCGGTGCTCTCCGGGTCTGTGCTGAGGAGAACG
CAACTCCGCCGGCGCAGGCGACCCTAACCCCAACCCCAACCCCAACCCCAACCCCAACCC
CAACCCTAACCCCTAACCCTAACCCT
