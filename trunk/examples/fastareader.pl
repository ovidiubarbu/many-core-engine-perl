#!/usr/bin/env perl

use strict;
use warnings;

## usage: fastareader.pl [ /path/to/fastafile.fa ]

###############################################################################

package BioUtil::Seq;

## The original plan was to run CPAN BioUtil::Seq::FastaReader in parallel.
## I thought faster was possible if logic processed records versus lines.
## https://gist.github.com/marioroy/1294672e8e3ba42cb684 (for most current)
##
## Two million clusters taken from uniref100.fasta.gz (2013_12).
## gunzip -c uniref100.fasta.gz | head -15687827 > uniref.fasta
##
## Running serially (one core)
##   Perl, line driven    15.649s      FastaReader, BioUtil-2014.1226
##   Perl, record driven   8.057s      By input record separator "\n>"
##
## Many-Core Engine
##   Perl, line driven     4.872s      FastaReader, BioUtil-2014.1226
##   Perl, record driven   2.507s      By input record separator "\n>"

sub FastaReader {
   my ($file, $not_trim) = @_;

   my ($is_stdin, $rec, $finished) = (0, 0, 0);
   my ($fh, $pos, $hdr, $seq);

   if ($file =~ /^STDIN$/i) {
      ($is_stdin, $fh) = (1, *STDIN);
   } else {
      open $fh, '<', $file or die "fail to open file: $file!\n";
   }

   return sub {
      return if $finished;
      local $/ = "\n>";                     ## set input record separator

      while (<$fh>) {
         unless ($rec++) {                  ## 1st record must have leading ">"
            s/^>// || next;                 ## trim ">", otherwise skip record
         }
         chop if substr($_, -1, 1) eq '>';  ## trim trailing ">"

         $pos = index($_, "\n");            ## extract header and sequence data
         $hdr = substr($_, 0, $pos + 1);
         $seq = substr($_, $pos + 1);

         unless ($not_trim) {
            chop $hdr;                                 ## trim trailing "\n"
            chop $hdr if substr($hdr, -1, 1) eq "\r";  ## trim trailing "\r"
            $seq =~ tr/ \t\r\n//d;                     ## trim white space
         }

         return [ $hdr, $seq ];
      }

      close $fh unless $is_stdin;
      $finished = 1;

      return;
   };
}

###############################################################################

package main;

use Cwd 'abs_path'; ## Insert lib-path at the head of @INC.
use lib abs_path($0 =~ m{^(.*)[\\/]} && $1 || abs_path) . '/../lib';

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

## Run in parallel.

my $fasta_file = shift || \*DATA;
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
   my $next_seq = BioUtil::Seq::FastaReader($slurp_ref, 0);

   ## loop through sequences in $slurp_ref
   while (my $fa = &$next_seq()) {
      ($hdr, $seq) = @{ $fa };
      $output .= ">$hdr\n".$seq."\n";
   }

   ## gather output for this chunk
   MCE->gather($chunk_id, $output);

}, $fasta_file;

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

