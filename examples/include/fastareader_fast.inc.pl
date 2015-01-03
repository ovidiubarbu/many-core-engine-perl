
use strict;
use warnings;

package BioUtil::Seq;

no strict 'refs';
no warnings 'redefine';

## Below, a faster alternative to CPAN BioUtil::Seq::FastaReader.
## Using "\n>" for the input record separator, thus record driven.

sub FastaReader {
   my ($file, $not_trim) = @_;

   my ($is_stdin, $rec, $finished) = (0, 0, 0);
   my ($fh, $pos, $hdr, $seq);

   if ($file =~ /^STDIN$/i) {
      ($is_stdin, $fh) = (1, *STDIN);
   } else {
      if (ref $file eq '' || ref $file eq 'SCALAR') {
         open $fh, '<', $file or die "fail to open file: $file!\n";
      } else {
         $fh = $file;
      }
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

1;

