#!/usr/bin/env perl

use strict;
use warnings;

## FASTA reader for FASTA files.
##
## The original plan was to run CPAN BioUtil::Seq::FastaReader in parallel.
## I decided to process by records versus lines.
##
## https://gist.github.com/marioroy/1294672e8e3ba42cb684 (for most current)
##
## Synopsis
##   fastareader.pl [ /path/to/fastafile.fa ]
##
## Two million clusters extracted from uniref100.fasta.gz (2013_12).
##   gunzip -c uniref100.fasta.gz | head -15687827 > uniref.fasta
##
## Running serially:   7.877s
## Many-Core Engine:   2.467s

use Cwd 'abs_path';
use lib abs_path($0 =~ m{^(.*)[\\/]} && $1 || abs_path) . '/../lib';

my  $prog_dir = abs_path($0 =~ m{^(.*)[\\/]} && $1 || abs_path);
do "$prog_dir/include/fastareader_txt.inc.pl";

###############################################################################

use MCE::Flow chunk_size => '1024k', max_workers => 'auto';
use Time::HiRes qw(time);

## Iterator for preserving output order.

sub output_iterator {
   my %tmp; my $order_id = 1;

   return sub {
      $tmp{ (shift) } = \@_;

      while (1) {
         last unless exists $tmp{$order_id};
         print @{ delete $tmp{$order_id++} };
      }

      return;
   };
}

## Process file in parallel.

my $file  = shift || \*DATA;
my $start = time;

mce_flow_f {
   gather => output_iterator,
   RS => "\n>", use_slurpio => 1,
},
sub {
   my ($mce, $slurp_ref, $chunk_id) = @_;
   my ($hdr, $seq); my $output = '';

   ## prepend leading '>' for chunks 2 and higher
   ${ $slurp_ref } = '>' . ${ $slurp_ref } if $chunk_id > 1;

   ## read from scalar reference
   my $next_seq = BioUtil::Test::FastaReader_txt($slurp_ref, 0);

   ## loop through sequences in $slurp_ref
   while (my $fa = &$next_seq()) {
      ($hdr, $seq) = @{ $fa };
      $output .= ">$hdr\n".$seq."\n";
   }

   ## gather output for this chunk
   MCE->gather($chunk_id, $output);

}, $file;

printf {*STDERR} "\n## Compute time: %0.03f\n\n", time - $start;

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

