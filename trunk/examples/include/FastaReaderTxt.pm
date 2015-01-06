
use strict;
use warnings;

package FastaReaderTxt;

## Using "\n>" for the input record separator, thus record driven.
## Generates text output containing the raw data, excluding '>'.
## Also see BioUtil::Seq::FastaReader.

sub Reader {
   my ($file, $not_trim) = @_;

   my ($open_flg, $finished, $first_flg) = (0, 0, 1);
   my ($fh, $pos, $hdr, $seq);

   if (ref $file eq '' || ref $file eq 'SCALAR') {
      open($fh, '<', $file) or die "$file: open: !\n";
      $open_flg = 1;
   } else {
      $fh = $file;
   }

   return sub {
      return if $finished;
      local $/ = "\n>";                     ## set input record separator

      while (<$fh>) {
         if ($first_flg) {                  ## 1st record must have leading ">"
            $first_flg--;                   ## trim ">", otherwise skip record
            s/^>// || next;
         }
         chop if substr($_, -1, 1) eq '>';  ## trim trailing ">", part of $/

         $pos = index($_, "\n") + 1;        ## extract header and bases
         $hdr = substr($_, 0, $pos);
         $seq = substr($_, $pos);

         undef $_;                          ## reduces memory consumption

         unless ($not_trim) {
            chop $hdr;                                 ## trim trailing "\n"
            chop $hdr if substr($hdr, -1, 1) eq "\r";  ## trim trailing "\r"
            $seq =~ tr/ \t\r\n//d;                     ## trim white space
         }

         return [ $hdr, $seq ];
      }

      close $fh if $open_flg;
      $finished = 1;

      return;
   };
}

1;

