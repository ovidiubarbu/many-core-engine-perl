#!/usr/bin/env perl

use strict;
use warnings;

## usage: fastaindexer.pl [ /path/to/fastafile.fa ]
## creates index file; e.g. /path/to/fastafile.fa.fai

###############################################################################

package BioUtil::Seq;

## The original plan was to run CPAN BioUtil::Seq::FastaReader in parallel.
## I thought faster was possible if logic processed records versus lines.
## https://gist.github.com/marioroy/85d08fc82845f11d12b5 (for most current)
##
## If running using the CPAN module, change 1 to 2 in the mce_flow_f block.
##   from:  $c3 += (1 + length $hdr);  ## ">", header line
##     to:  $c3 += (2 + length $hdr);  ## ">", header line, "\n"
##
## Two million clusters taken from uniref100.fasta.gz (2013_12).
## gunzip -c uniref100.fasta.gz | head -15687827 > uniref.fasta
##
## Running serially (one core)
##   C++ , fastahack -i   28.216s      https://github.com/ekg/fastahack
##   Perl, line driven    21.934s      FastaReader, BioUtil-2014.1226
##   Perl, record driven  15.258s      By input record separator "\n>"
##
## Many-Core Engine
##   Perl, line driven     6.781s      FastaReader, BioUtil-2014.1226.
##   Perl, record driven   4.835s      By input record separator "\n>"

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
   my ($output_fh) = @_;

   my (%tmp, $size); my ($order_id, $offset) = (1, 0);

   return sub {
      $tmp{ (shift) } = \@_;

      while (1) {
         last unless exists $tmp{$order_id};

         $size = shift @{ $tmp{$order_id} };
         my $buffer = '';

         foreach my $row ( @{ delete $tmp{$order_id++} } ) {
            $buffer .= $row->[0].( $row->[1] + $offset ).$row->[2];
         }

         print {$output_fh} $buffer;
         $offset += $size;
      }

      return;
   };
}

## Display error message.

my $exit_status = 0;

sub print_error {
   my ($error_msg) = @_;
   print {*STDERR} $error_msg."\n";
   $exit_status = 1;
}

## Obtain handle to index file *.fai.

my $fasta_file = shift || \*DATA;
my $start = time;
my $output_fh;

if (ref $fasta_file) {
   $output_fh = \*STDOUT;
}
else {
   die "$fasta_file: $!\n" unless -f $fasta_file;
   open $output_fh, '>', "$fasta_file.fai.tmp"
      or die "$fasta_file.fai.tmp: $!\n";
}

## Run in parallel.

mce_flow_f {
   gather => output_iterator($output_fh),
   RS => "\n>", use_slurpio => 1,
},
sub {
   my ($mce, $slurp_ref, $chunk_id) = @_;

   ## prepend leading '>' for chunks 2 and higher
   ${ $slurp_ref } = '>' . ${ $slurp_ref } if $chunk_id > 1;

   ## read from scalar reference
   my $next_seq = BioUtil::Seq::FastaReader($slurp_ref, 1);

   my ($p1, $err, $hdr, $seq, $sz, @output);  $sz = 0;
   my ($p2, $len, $c1, $c2, $c3, $c4, $c5);   $c3 = 0;

   ## loop through sequences in $slurp_ref
   while (my $fa = &$next_seq()) {
      ($hdr, $seq) = @{ $fa };

      ($c1) = ($hdr) =~ /^(\S+)/;
       $c3 += (1 + length $hdr);  ## ">", header line
     # $c3 += (2 + length $hdr);  ## ">", header line, "\n" (CPAN FastaReader)

       $len = length $seq;
       $sz  = $c3 + $len;
       $c5  = index $seq, "\n";

      if ($c5 < 0) {
         ($c2, $c3, $c4, $c5) = (0, -1, 0, 0);     ## without sequence data
      }
      else {
         my @a;  $p1 = $c5 + 1;

         while ($p1 < $len) {                      ## collect line lengths
            $p2 = index $seq, "\n", $p1;           ##   performs faster than
            push @a, $p2 - $p1;                    ##   @a = split(/\n/, $seq)
            $p1 = $p2 + 1;
         }
         if (scalar @a) {
            pop @a while ($a[-1] == 0);            ## pop trailing newlines
            pop @a;

            foreach (0 .. scalar(@a) - 1) {        ## any length mismatch
               if ($a[$_] != $c5) {
                  $len = -1; last;
               }
            }
         }
         $c4  =  (substr($seq, ++$c5 - 2, 1) eq "\r") ? $c5 - 2 : $c5 - 1;
         $seq =~ tr/ \t\r\n//d;     ## tr performs faster than s/\s//g
         $c2  =  length $seq;
      }

      if ($len < 0) {
         chomp $hdr;
         my $err = 'SKIPPED: mismatched line lengths within sequence '.$hdr;
         MCE->do('print_error', $err);
      }
      else {
         ## concatenate left and right sides to reduce time needed
         ## by the manager process. $c3 is the offset column.
         push @output, [ "$c1\t$c2\t", $c3, "\t$c4\t$c5\n" ];
      }

      $c3 = $sz;
   }

   ## gather output for this chunk
   MCE->gather($chunk_id, $sz, @output);

}, $fasta_file;

close $output_fh unless ref $fasta_file;

rename "$fasta_file.fai.tmp", "$fasta_file.fai"
   or die "rename $fasta_file.fai.tmp to $fasta_file.fai: $!\n";

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

